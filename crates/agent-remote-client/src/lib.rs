use std::process::Stdio;
use std::sync::Arc;

use agent_remote_protocol::{
    ErrorCode, ExecEvent, ExecEventKind, FileEntry, ListEntry, OperationDetails, OperationId,
    ProtocolError, ReadResult, Request, RequestBody, RequestId, RequestStatusResult, ServerMessage,
    UndoResult, WriteOrPatchResult,
};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, warn};

mod log_writer;

pub use log_writer::ClientLog;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("protocol error from server: {0}")]
    Server(ProtocolError),
    #[error("transport io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("server closed connection")]
    Closed,
    #[error("request timed out")]
    Timeout,
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

type DispMap = Arc<Mutex<std::collections::HashMap<RequestId, oneshot::Sender<ServerMessage>>>>;

type StreamMap = Arc<
    Mutex<std::collections::HashMap<RequestId, tokio::sync::mpsc::UnboundedSender<ServerMessage>>>,
>;

/// Hard cap on how long a non-streaming request waits for a reply. Guards
/// against a server that stays connected but never responds. Long-running work
/// goes through `exec`, which has its own server-side `timeout_ms`.
const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Spawns the remote process (ssh or local). Implementations return the child
/// and its stdin/stdout pipes.
pub trait Transport: Send {
    fn spawn(&mut self) -> std::io::Result<(Child, ChildStdin, ChildStdout)>;
}

/// Default transport: spawns the given argv as a subprocess. For SSH use
/// argv like `["ssh", host, "agent-remote-server", "--root", path]`; for tests
/// use the local server binary directly.
pub struct ArgvTransport {
    pub argv: Vec<String>,
}

impl Transport for ArgvTransport {
    fn spawn(&mut self) -> std::io::Result<(Child, ChildStdin, ChildStdout)> {
        let mut cmd = Command::new(&self.argv[0]);
        cmd.args(&self.argv[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = cmd.spawn()?;
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        Ok((child, stdin, stdout))
    }
}

pub struct ExecStream {
    pub events: tokio::sync::mpsc::UnboundedReceiver<ServerMessage>,
}

impl ExecStream {
    /// Collect stdout/stderr/exit into a simple aggregate. Returns (stdout,
    /// stderr, exit_code, operation_id).
    pub async fn collect(self) -> (String, String, Option<i32>, Option<OperationId>) {
        let mut stdout = String::new();
        let mut stderr = String::new();
        let mut exit = None;
        let mut op = None;
        let mut rx = self.events;
        while let Some(m) = rx.recv().await {
            match m {
                ServerMessage::ExecEvent(ExecEvent { event, .. }) => match event {
                    ExecEventKind::Stdout { data } => stdout.push_str(&data),
                    ExecEventKind::Stderr { data } => stderr.push_str(&data),
                    ExecEventKind::Exit {
                        exit_code,
                        operation_id,
                    } => {
                        exit = Some(exit_code);
                        op = Some(operation_id);
                    }
                },
                ServerMessage::Result { .. } | ServerMessage::Error { .. } => break,
            }
        }
        (stdout, stderr, exit, op)
    }
}

pub struct Client {
    stdin: Arc<Mutex<ChildStdin>>,
    reply_map: DispMap,
    stream_map: StreamMap,
    /// Persistent close flag: once the transport EOFs, this stays true so any
    /// later request on this Client fails immediately instead of hanging.
    closed: Arc<std::sync::atomic::AtomicBool>,
    closed_notify: Arc<tokio::sync::Notify>,
    log: Option<Arc<ClientLog>>,
}

impl Client {
    pub async fn connect<T: Transport + 'static>(
        mut transport: T,
        log: Option<ClientLog>,
    ) -> Result<Self, ClientError> {
        let (mut _child, stdin, stdout) = transport.spawn()?;
        // Keep the child alive for the connection lifetime by leaking its
        // handle into a detached task that also reaps it on drop. We hold it
        // via the reader task.
        let stdin = Arc::new(Mutex::new(stdin));
        let reply_map: DispMap = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let stream_map: StreamMap = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let closed_notify = Arc::new(tokio::sync::Notify::new());
        let log = log.map(Arc::new);

        let reader_reply = reply_map.clone();
        let reader_stream = stream_map.clone();
        let drain_reply = reply_map.clone();
        let drain_stream = stream_map.clone();
        let reader_closed = closed.clone();
        let reader_notify = closed_notify.clone();
        let reader_log = log.clone();
        tokio::spawn(async move {
            reader_loop(stdout, reader_reply, reader_stream, reader_log).await;
            // Mark the connection persistently closed so future requests on this
            // Client fail fast, then wake any current waiters.
            reader_closed.store(true, std::sync::atomic::Ordering::SeqCst);
            drain_waiters(&drain_reply, &drain_stream).await;
            reader_notify.notify_waiters();
            // When stdout ends, the child has exited.
            let _ = _child.wait().await;
        });

        Ok(Self {
            stdin,
            reply_map,
            stream_map,
            closed,
            closed_notify,
            log,
        })
    }

    fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Resolves once the connection is closed. Checks the persistent flag first
    /// (so a Client reused after EOF returns immediately) and otherwise waits
    /// for the reader task's notify.
    async fn wait_closed(&self) {
        if self.is_closed() {
            return;
        }
        loop {
            // Either the flag is already set, or we register for the notify
            // before re-checking, avoiding the lost-wakeup window of a bare
            // Notify (which does not remember past notifications).
            let notified = self.closed_notify.notified();
            if self.is_closed() {
                return;
            }
            notified.await;
            if self.is_closed() {
                return;
            }
        }
    }

    fn next_request_id(&self) -> RequestId {
        format!("req-{}", unique_id())
    }

    async fn send_request(
        &self,
        body: RequestBody,
    ) -> Result<(RequestId, ServerMessage), ClientError> {
        if self.is_closed() {
            return Err(ClientError::Closed);
        }
        let request_id = self.next_request_id();
        let req = Request {
            request_id: request_id.clone(),
            body,
        };
        let (tx, rx) = oneshot::channel::<ServerMessage>();
        self.reply_map.lock().await.insert(request_id.clone(), tx);
        let line = serde_json::to_string(&req)?;
        if let Some(l) = &self.log {
            l.log_request(&request_id, &line).await;
        }
        {
            let mut w = self.stdin.lock().await;
            w.write_all(line.as_bytes()).await?;
            w.write_all(b"\n").await?;
            w.flush().await?;
        }
        // Race the reply against connection close and a hard request timeout:
        // if the server/SSH disappears, the reader drains reply_map (closing
        // `tx`) and notifies here; if the server stalls, the timeout fires.
        // Either way we never block indefinitely.
        let msg = tokio::select! {
            biased;
            () = self.wait_closed() => return Err(ClientError::Closed),
            () = tokio::time::sleep(DEFAULT_REQUEST_TIMEOUT) => {
                self.reply_map.lock().await.remove(&request_id);
                return Err(ClientError::Timeout);
            }
            m = rx => m.map_err(|_| ClientError::Closed)?,
        };
        if let Some(l) = &self.log {
            l.log_response(&request_id, &msg).await;
        }
        Ok((request_id, msg))
    }

    fn unpack(msg: ServerMessage) -> Result<agent_remote_protocol::ResultBody, ClientError> {
        match msg {
            ServerMessage::Result { result, .. } => Ok(result),
            ServerMessage::Error { error, .. } => Err(ClientError::Server(error)),
            ServerMessage::ExecEvent(_) => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected exec event on reply channel",
            ))),
        }
    }

    pub async fn list(&self, path: &str) -> Result<Vec<ListEntry>, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::List { path: path.into() })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::List { entries } => Ok(entries),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for list",
            ))),
        }
    }

    pub async fn stat(&self, path: &str) -> Result<FileEntry, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Stat { path: path.into() })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Stat { stat } => Ok(stat),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for stat",
            ))),
        }
    }

    pub async fn read(
        &self,
        path: &str,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> Result<ReadResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Read {
                path: path.into(),
                offset,
                limit,
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Read(r) => Ok(r),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for read",
            ))),
        }
    }

    pub async fn write(
        &self,
        path: &str,
        content: &str,
        base_hash: Option<&str>,
    ) -> Result<WriteOrPatchResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Write {
                path: path.into(),
                content: content.into(),
                base_hash: base_hash.map(|s| s.into()),
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::WriteOrPatch(w) => Ok(w),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for write",
            ))),
        }
    }

    pub async fn patch(
        &self,
        path: &str,
        base_hash: &str,
        patch: &str,
    ) -> Result<WriteOrPatchResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Patch {
                path: path.into(),
                base_hash: base_hash.into(),
                patch: patch.into(),
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::WriteOrPatch(w) => Ok(w),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for patch",
            ))),
        }
    }

    pub async fn delete(&self, path: &str) -> Result<WriteOrPatchResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Delete { path: path.into() })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::WriteOrPatch(w) => Ok(w),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for delete",
            ))),
        }
    }

    /// Run an exec, streaming events. The callback is invoked for each
    /// stdout/stderr/exit event. Returns the final exit code and operation id.
    pub async fn exec<F>(
        &self,
        argv: Vec<String>,
        cwd: Option<String>,
        profile: Option<String>,
        timeout_ms: Option<u64>,
        mut on_event: F,
    ) -> Result<(i32, OperationId), ClientError>
    where
        F: FnMut(ExecEventKind),
    {
        if self.is_closed() {
            return Err(ClientError::Closed);
        }
        let request_id = self.next_request_id();
        let req = Request {
            request_id: request_id.clone(),
            body: RequestBody::Exec {
                argv,
                cwd,
                profile,
                timeout_ms,
            },
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ServerMessage>();
        self.stream_map.lock().await.insert(request_id.clone(), tx);
        let line = serde_json::to_string(&req)?;
        if let Some(l) = &self.log {
            l.log_request(&request_id, &line).await;
        }
        {
            let mut w = self.stdin.lock().await;
            w.write_all(line.as_bytes()).await?;
            w.write_all(b"\n").await?;
            w.flush().await?;
        }

        let mut exit_code = None;
        let mut op_id = None;
        loop {
            // Abort promptly if the connection drops mid-exec.
            let m = tokio::select! {
                biased;
                () = self.wait_closed() => {
                    self.stream_map.lock().await.remove(&request_id);
                    return Err(ClientError::Closed);
                }
                m = rx.recv() => match m {
                    Some(m) => m,
                    None => {
                        self.stream_map.lock().await.remove(&request_id);
                        return Err(ClientError::Closed);
                    }
                },
            };
            if let Some(l) = &self.log {
                l.log_response(&request_id, &m).await;
            }
            match m {
                ServerMessage::ExecEvent(ExecEvent { event, .. }) => {
                    on_event(event);
                }
                ServerMessage::Result { result, .. } => {
                    if let agent_remote_protocol::ResultBody::Exit {
                        exit_code: c,
                        operation_id,
                    } = result
                    {
                        exit_code = Some(c);
                        op_id = Some(operation_id);
                    }
                    break;
                }
                ServerMessage::Error { error, .. } => {
                    self.stream_map.lock().await.remove(&request_id);
                    return Err(ClientError::Server(error));
                }
            }
        }
        self.stream_map.lock().await.remove(&request_id);
        match (exit_code, op_id) {
            (Some(c), Some(o)) => Ok((c, o)),
            _ => Err(ClientError::Closed),
        }
    }

    pub async fn undo(&self, operation_id: &str) -> Result<UndoResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::Undo {
                operation_id: operation_id.into(),
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Undo(u) => Ok(u),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for undo",
            ))),
        }
    }

    pub async fn history(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<agent_remote_protocol::AnyOperationRecord>, ClientError> {
        let (_, msg) = self.send_request(RequestBody::History { limit }).await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::History { operations } => Ok(operations),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for history",
            ))),
        }
    }

    pub async fn operation_get(&self, operation_id: &str) -> Result<OperationDetails, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::OperationGet {
                operation_id: operation_id.into(),
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Operation(o) => Ok(o),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for operation_get",
            ))),
        }
    }

    pub async fn gc(
        &self,
        keep: Option<usize>,
    ) -> Result<agent_remote_protocol::GcResult, ClientError> {
        let (_, msg) = self.send_request(RequestBody::Gc { keep }).await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Gc(g) => Ok(g),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for gc",
            ))),
        }
    }

    pub async fn request_status(&self, target: &str) -> Result<RequestStatusResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::RequestStatus {
                target: target.into(),
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::RequestStatus(r) => Ok(r),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for request_status",
            ))),
        }
    }
}

/// On connection close, fail every waiter by removing and dropping their
/// senders. oneshot senders signal an error on drop, and stream senders cause
/// `recv()` to return None, so both ordinary and streaming requests wake up
/// and surface ClientError::Closed instead of hanging forever.
async fn drain_waiters(reply_map: &DispMap, stream_map: &StreamMap) {
    reply_map.lock().await.clear();
    stream_map.lock().await.clear();
}

async fn reader_loop(
    stdout: ChildStdout,
    reply_map: DispMap,
    stream_map: StreamMap,
    log: Option<Arc<ClientLog>>,
) {
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                warn!(error = ?e, "client reader error");
                break;
            }
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let msg: ServerMessage = match serde_json::from_str(trimmed) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, line = trimmed, "could not parse server message");
                continue;
            }
        };
        let rid = match &msg {
            ServerMessage::Result { request_id, .. } | ServerMessage::Error { request_id, .. } => {
                request_id.clone()
            }
            ServerMessage::ExecEvent(e) => e.request_id.clone(),
        };
        if let Some(l) = &log {
            l.log_raw(&rid, trimmed).await;
        }
        debug!(request_id = %rid, "recv");
        let is_terminal = matches!(
            &msg,
            ServerMessage::Result { .. } | ServerMessage::Error { .. }
        );
        // Exec streams are routed by request_id across multiple messages, so
        // keep the entry until the terminal message arrives.
        let stream_tx_present = { stream_map.lock().await.contains_key(&rid) };
        if stream_tx_present {
            if let Some(tx) = { stream_map.lock().await.get(&rid).cloned() } {
                let _ = tx.send(msg);
            }
            if is_terminal {
                stream_map.lock().await.remove(&rid);
            }
            continue;
        }
        let reply_tx = { reply_map.lock().await.remove(&rid) };
        if let Some(tx) = reply_tx {
            let _ = tx.send(msg);
        } else {
            warn!(request_id = %rid, "no handler for message");
        }
    }
}

/// Shell-quote a string for safe use inside a remote command line: wrapped in
/// single quotes, embedded single quotes escaped.
pub fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".into();
    }
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Build the argv for spawning the server over ssh. `ssh` joins its trailing
/// arguments with spaces and hands the result to the remote shell, so every
/// remote-side argument is quoted into one command string.
pub fn ssh_server_argv(
    host: &str,
    remote_bin: &str,
    root: &str,
    config: Option<&str>,
    log_dir: Option<&str>,
    state_base: Option<&str>,
) -> Vec<String> {
    let mut cmd = shell_quote(remote_bin);
    cmd.push_str(" --root ");
    cmd.push_str(&shell_quote(root));
    if let Some(c) = config {
        cmd.push_str(" --config ");
        cmd.push_str(&shell_quote(c));
    }
    if let Some(d) = log_dir {
        cmd.push_str(" --log-dir ");
        cmd.push_str(&shell_quote(d));
    }
    if let Some(b) = state_base {
        cmd.push_str(" --state-base ");
        cmd.push_str(&shell_quote(b));
    }
    vec!["ssh".into(), host.into(), cmd]
}

/// Request IDs must be globally unique because the server dedupes on them for
/// idempotent replay. Timestamp separates processes over time, pid separates
/// concurrent processes, and the counter separates requests within a process.
fn unique_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{ts:016x}-{:08x}-{n:08x}", std::process::id())
}

#[cfg(test)]
mod quote_tests {
    use super::{shell_quote, ssh_server_argv};

    #[test]
    fn quotes_empty_spaces_and_metacharacters() {
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("plain"), "'plain'");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("$(rm -rf /);`x`|&"), "'$(rm -rf /);`x`|&'");
        assert_eq!(shell_quote("it's"), r#"'it'\''s'"#);
    }

    #[test]
    fn ssh_argv_is_one_quoted_remote_command() {
        let argv = ssh_server_argv(
            "host",
            "agent-remote-server",
            "/data/my project",
            None,
            Some("/tmp/st ate"),
            None,
        );
        assert_eq!(argv[0], "ssh");
        assert_eq!(argv[1], "host");
        assert_eq!(
            argv[2],
            "'agent-remote-server' --root '/data/my project' --log-dir '/tmp/st ate'"
        );
        assert_eq!(argv.len(), 3);
    }

    #[test]
    fn ssh_argv_forwards_state_base() {
        let argv = ssh_server_argv(
            "host",
            "srv",
            "/r",
            None,
            None,
            Some("/data/sicheng/agent state"),
        );
        assert_eq!(
            argv[2],
            "'srv' --root '/r' --state-base '/data/sicheng/agent state'"
        );
    }
}
