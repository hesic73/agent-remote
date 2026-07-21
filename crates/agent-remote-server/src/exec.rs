use std::time::Duration;

use agent_remote_protocol::{ErrorCode, ExecEventKind, OperationId, ProtocolError};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::config::ServerConfig;
use crate::workspace::Workspace;

pub struct ExecOutcome {
    pub exit_code: i32,
    pub operation_id: OperationId,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub duration_ms: u64,
    /// True if the process was killed by timeout.
    pub timed_out: bool,
    /// True if either captured stream hit CAPTURE_LIMIT.
    pub output_truncated: bool,
}

enum StreamEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
}

/// Run a command, streaming stdout/stderr chunks via `emit`. `emit` returns a
/// future so the caller can perform async writes. Captures the full stdout and
/// stderr for logging. Returns the final exit code and timing.
#[allow(clippy::too_many_arguments)]
pub async fn exec<F, Fut>(
    ws: &Workspace,
    config: &ServerConfig,
    cwd: Option<&str>,
    profile: Option<&str>,
    argv: &[String],
    timeout_ms: Option<u64>,
    operation_id: OperationId,
    mut emit: F,
) -> Result<ExecOutcome, ProtocolError>
where
    F: FnMut(ExecEventKind) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    if argv.is_empty() {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "argv must not be empty",
        ));
    }
    let setup = config.setup_for(profile)?;
    let working_dir = match cwd {
        Some(c) => ws.resolve(c)?,
        None => ws.root.clone(),
    };
    if !working_dir.starts_with(&ws.root) {
        return Err(ProtocolError::new(
            ErrorCode::PathOutsideRoot,
            "cwd escapes workspace root",
        ));
    }
    if !working_dir.is_dir() {
        return Err(ProtocolError::new(
            ErrorCode::NotFound,
            format!("cwd not found: {}", working_dir.display()),
        ));
    }

    // Always run through bash so profile setup (conda/ROS/etc) takes effect.
    // After sourcing the setup, `exec` replaces the shell with the target
    // command so signals propagate naturally to the child.
    let quoted: Vec<String> = argv.iter().map(|a| shell_quote(a)).collect();
    let script = if setup.is_empty() {
        format!("exec {}", quoted.join(" "))
    } else {
        format!("{setup}\nexec {}", quoted.join(" "))
    };

    let mut cmd = Command::new("bash");
    cmd.arg("-c")
        .arg(&script)
        .current_dir(&working_dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    // Put the child in its own process group so a timeout can kill the whole
    // tree (grandchildren included), not just the direct bash process. Without
    // this, a grandchild holding the stdout pipe can keep a timed-out exec alive.
    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
    let start = std::time::Instant::now();
    let mut child = cmd
        .spawn()
        .map_err(|e| ProtocolError::new(ErrorCode::ExecFailed, format!("spawn failed: {e}")))?;
    let pid = child.id();
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");

    let (tx, mut rx) = tokio::sync::mpsc::channel::<StreamEvent>(64);
    let tx1 = tx.clone();
    tokio::spawn(async move {
        let mut reader = stdout;
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx1
                        .send(StreamEvent::Stdout(buf[..n].to_vec()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });
    let tx2 = tx.clone();
    tokio::spawn(async move {
        let mut reader = stderr;
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx2
                        .send(StreamEvent::Stderr(buf[..n].to_vec()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });
    drop(tx);

    // ABSOLUTE deadline computed ONCE, before the loop. Recreating the timer
    // each iteration (the old bug) let a continuously-outputting command reset
    // the timer forever and bypass the timeout entirely.
    let deadline = timeout_ms.map(|ms| tokio::time::Instant::now() + Duration::from_millis(ms));
    let mut timed_out = false;
    let mut captured_stdout: Vec<u8> = Vec::new();
    let mut captured_stderr: Vec<u8> = Vec::new();
    let mut output_truncated = false;
    // Incremental UTF-8 decoders per stream so a multi-byte codepoint split
    // across two pipe reads is emitted as one correct character, not two
    // replacement chars.
    let mut stdout_dec = Utf8Decoder::new();
    let mut stderr_dec = Utf8Decoder::new();

    loop {
        // NO `biased`: a fair select means the deadline arm has a real chance
        // to fire even when output is constantly available. We also poll the
        // deadline FIRST on every iteration so a command producing output every
        // few ms is still killed at the deadline.
        if let Some(dl) = deadline {
            if tokio::time::Instant::now() >= dl {
                timed_out = true;
                kill_process_group(pid);
                let _ = child.start_kill();
                break;
            }
        }
        tokio::select! {
            // Deadline first (after the explicit check above) so it cannot be
            // starved by constant output.
            () = async {
                match deadline {
                    Some(dl) => tokio::time::sleep_until(dl).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                timed_out = true;
                kill_process_group(pid);
                let _ = child.start_kill();
            }
            Some(ev) = rx.recv() => {
                handle_stream_event(
                    &ev,
                    &mut captured_stdout,
                    &mut captured_stderr,
                    &mut output_truncated,
                    &mut stdout_dec,
                    &mut stderr_dec,
                    &mut emit,
                )
                .await;
            }
            res = child.wait() => {
                // Drain remaining buffered output.
                while let Some(ev) = rx.recv().await {
                    handle_stream_event(
                        &ev,
                        &mut captured_stdout,
                        &mut captured_stderr,
                        &mut output_truncated,
                        &mut stdout_dec,
                        &mut stderr_dec,
                        &mut emit,
                    )
                    .await;
                }
                // Flush any trailing incomplete UTF-8 sequence (same as the
                // timeout path's drain_after_kill does).
                if let Some(text) = stdout_dec.flush() {
                    if !text.is_empty() {
                        emit(ExecEventKind::Stdout { data: text }).await;
                    }
                }
                if let Some(text) = stderr_dec.flush() {
                    if !text.is_empty() {
                        emit(ExecEventKind::Stderr { data: text }).await;
                    }
                }
                let _ = res; // status is meaningful only if we did not time out
                if timed_out {
                    break;
                }
                let status = res.map_err(|e| {
                    ProtocolError::new(ErrorCode::ExecFailed, format!("wait failed: {e}"))
                })?;
                let exit_code = status.code().unwrap_or(-1);
                return Ok(ExecOutcome {
                    exit_code,
                    operation_id,
                    stdout: captured_stdout,
                    stderr: captured_stderr,
                    duration_ms: start.elapsed().as_millis() as u64,
                    timed_out: false,
                    output_truncated,
                });
            }
        }
    }

    // Reaching here means we broke out due to timeout. Reap the child and flush
    // any trailing decoder state.
    drain_after_kill(
        &mut rx,
        &mut captured_stdout,
        &mut captured_stderr,
        &mut output_truncated,
        &mut stdout_dec,
        &mut stderr_dec,
        &mut emit,
    )
    .await;
    let _ = child.wait().await;
    Ok(ExecOutcome {
        exit_code: -1,
        operation_id,
        stdout: captured_stdout,
        stderr: captured_stderr,
        duration_ms: start.elapsed().as_millis() as u64,
        timed_out,
        output_truncated,
    })
}

#[allow(clippy::too_many_arguments)]
async fn handle_stream_event<F, Fut>(
    ev: &StreamEvent,
    captured_stdout: &mut Vec<u8>,
    captured_stderr: &mut Vec<u8>,
    output_truncated: &mut bool,
    stdout_dec: &mut Utf8Decoder,
    stderr_dec: &mut Utf8Decoder,
    emit: &mut F,
) where
    F: FnMut(ExecEventKind) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    match ev {
        StreamEvent::Stdout(b) => {
            if extend_capped(captured_stdout, b, CAPTURE_LIMIT) {
                *output_truncated = true;
            }
            if let Some(text) = stdout_dec.feed(b) {
                emit(ExecEventKind::Stdout { data: text }).await;
            }
        }
        StreamEvent::Stderr(b) => {
            if extend_capped(captured_stderr, b, CAPTURE_LIMIT) {
                *output_truncated = true;
            }
            if let Some(text) = stderr_dec.feed(b) {
                emit(ExecEventKind::Stderr { data: text }).await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drain_after_kill<F, Fut>(
    rx: &mut tokio::sync::mpsc::Receiver<StreamEvent>,
    captured_stdout: &mut Vec<u8>,
    captured_stderr: &mut Vec<u8>,
    output_truncated: &mut bool,
    stdout_dec: &mut Utf8Decoder,
    stderr_dec: &mut Utf8Decoder,
    emit: &mut F,
) where
    F: FnMut(ExecEventKind) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    // Best-effort drain of output already buffered by the reader tasks, then
    // flush any trailing incomplete UTF-8 sequence as a replacement char so the
    // client sees a clean end-of-stream.
    while let Ok(ev) = rx.try_recv() {
        handle_stream_event(
            &ev,
            captured_stdout,
            captured_stderr,
            output_truncated,
            stdout_dec,
            stderr_dec,
            emit,
        )
        .await;
    }
    if let Some(text) = stdout_dec.flush() {
        if !text.is_empty() {
            emit(ExecEventKind::Stdout { data: text }).await;
        }
    }
    if let Some(text) = stderr_dec.flush() {
        if !text.is_empty() {
            emit(ExecEventKind::Stderr { data: text }).await;
        }
    }
}

fn kill_process_group(pid: Option<u32>) {
    if let Some(pid) = pid {
        let pg = pid as i32;
        unsafe {
            libc::killpg(pg, libc::SIGKILL);
        }
    }
}

/// Upper bound on in-memory captured output per stream, to keep a runaway
/// command from OOMing the server. Output beyond this is still streamed to the
/// client live; only the *captured* copy (for logging) is truncated.
const CAPTURE_LIMIT: usize = 64 * 1024 * 1024;

/// Returns true if this chunk hit the cap (truncation occurred).
fn extend_capped(buf: &mut Vec<u8>, chunk: &[u8], cap: usize) -> bool {
    if buf.len() >= cap {
        return true;
    }
    let remaining = cap - buf.len();
    if chunk.len() <= remaining {
        buf.extend_from_slice(chunk);
        false
    } else {
        buf.extend_from_slice(&chunk[..remaining]);
        true
    }
}

/// Incremental UTF-8 decoder. Accumulates bytes that do not yet form a complete
/// sequence and emits the decoded prefix; a trailing partial codepoint is held
/// until the next chunk completes it (or flush() replaces it on stream end).
struct Utf8Decoder {
    pending: Vec<u8>,
}

impl Utf8Decoder {
    fn new() -> Self {
        Self {
            pending: Vec::new(),
        }
    }

    /// Feed bytes; returns Some(text) to emit (possibly empty if everything is
    /// still partial) or None if there is nothing complete yet.
    fn feed(&mut self, bytes: &[u8]) -> Option<String> {
        self.pending.extend_from_slice(bytes);
        let mut out = String::new();
        // Repeatedly extract the longest valid UTF-8 prefix. When the prefix
        // ends on an incomplete sequence, hold those bytes for the next chunk.
        // When it ends on a genuinely invalid byte, emit U+FFFD in its place
        // (the client still sees the lossy replacement rather than nothing).
        loop {
            if self.pending.is_empty() {
                return if out.is_empty() { None } else { Some(out) };
            }
            match std::str::from_utf8(&self.pending) {
                Ok(s) => {
                    out.push_str(s);
                    self.pending.clear();
                    return Some(out);
                }
                Err(e) => {
                    let valid = e.valid_up_to();
                    if valid > 0 {
                        out.push_str(std::str::from_utf8(&self.pending[..valid]).unwrap());
                        self.pending.drain(..valid);
                        continue;
                    }
                    // valid == 0: first byte is problematic.
                    if let Some(err_len) = e.error_len() {
                        // Genuinely invalid byte(s): emit a replacement char
                        // for them so the client sees something, then advance.
                        out.push('\u{FFFD}');
                        self.pending.drain(..err_len);
                    } else {
                        // Incomplete multi-byte sequence: hold everything for
                        // the next chunk.
                        return if out.is_empty() { None } else { Some(out) };
                    }
                }
            }
        }
    }

    /// Flush any remaining bytes as lossy UTF-8 (replacement chars), called when
    /// the stream ends. Returns None if nothing was pending.
    fn flush(&mut self) -> Option<String> {
        if self.pending.is_empty() {
            return None;
        }
        let s = String::from_utf8_lossy(&self.pending).into_owned();
        self.pending.clear();
        Some(s)
    }
}

fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".into();
    }
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || "-_./=:".contains(c))
    {
        return s.into();
    }
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_decoder_holds_split_codepoint() {
        // "é" is 0xC3 0xA9 (2 bytes). Feed the first byte, then the second.
        let mut dec = Utf8Decoder::new();
        assert_eq!(dec.feed(&[0xC3]), None, "leader alone is incomplete");
        let got = dec.feed(&[0xA9]).unwrap();
        assert_eq!(got, "é");
    }

    #[test]
    fn utf8_decoder_emits_complete_prefix() {
        // "ab" + incomplete leader 0xC3 -> emit "ab", hold 0xC3.
        let mut dec = Utf8Decoder::new();
        let got = dec.feed(b"ab\xc3").unwrap();
        assert_eq!(got, "ab");
        assert_eq!(dec.flush(), Some("\u{FFFD}".into()));
    }

    #[test]
    fn utf8_decoder_flushes_trailing_as_replacement() {
        let mut dec = Utf8Decoder::new();
        assert_eq!(dec.feed(&[0xE2, 0x82]), None); // 3-byte leader, 2 bytes
        let got = dec.flush().unwrap();
        assert_eq!(got, "\u{FFFD}");
    }

    #[test]
    fn utf8_decoder_emits_replacement_for_invalid_byte() {
        // 0xFF is never a valid UTF-8 byte; it should be emitted as U+FFFD.
        let mut dec = Utf8Decoder::new();
        let got = dec.feed(b"\xFFok");
        assert_eq!(got, Some("\u{FFFD}ok".into()));
    }

    #[test]
    fn extend_capped_stops_at_cap_and_reports_truncation() {
        let mut buf = Vec::new();
        assert!(!extend_capped(&mut buf, b"abc", 5));
        assert!(
            extend_capped(&mut buf, b"defg", 5),
            "crossing cap truncates"
        );
        assert_eq!(buf, b"abcde");
        assert!(extend_capped(&mut buf, b"x", 5), "already-full drops input");
        assert_eq!(buf, b"abcde");
    }
}
