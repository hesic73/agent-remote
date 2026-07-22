use std::process::Stdio;
use std::sync::Arc;

use agent_remote_protocol::{
    ErrorCode, ExecResult, FileEntry, ListResult, OperationDetails, ProtocolError, ReadResult,
    Request, RequestBody, RequestId, RequestStatusResult, ServerMessage, UndoResult,
    WriteOrPatchResult,
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

/// Hard cap on how long a non-streaming request waits for a reply. Guards
/// against a server that stays connected but never responds. Long-running work
/// goes through `exec`, which has its own server-side `timeout_ms`.
const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
const DEFAULT_EXEC_TIMEOUT_MS: u64 = 5 * 60 * 1000;

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
        // Die with the parent: if the consumer (CLI/MCP) is killed -- even
        // with SIGKILL, where no destructor runs -- the transport child must
        // not outlive it as an orphan holding the remote session (and the
        // server-side state lock) open.
        unsafe {
            cmd.pre_exec(|| {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                Ok(())
            });
        }
        let mut child = cmd.spawn()?;
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        Ok((child, stdin, stdout))
    }
}

pub struct Client {
    stdin: Arc<Mutex<ChildStdin>>,
    reply_map: DispMap,
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
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let closed_notify = Arc::new(tokio::sync::Notify::new());
        let log = log.map(Arc::new);

        let reader_reply = reply_map.clone();
        let drain_reply = reply_map.clone();
        let reader_closed = closed.clone();
        let reader_notify = closed_notify.clone();
        let reader_log = log.clone();
        tokio::spawn(async move {
            reader_loop(stdout, reader_reply, reader_log).await;
            // Mark the connection persistently closed so future requests on this
            // Client fail fast, then wake any current waiters.
            reader_closed.store(true, std::sync::atomic::Ordering::SeqCst);
            drain_waiters(&drain_reply).await;
            reader_notify.notify_waiters();
            // When stdout ends, the child has exited.
            let _ = _child.wait().await;
        });

        Ok(Self {
            stdin,
            reply_map,
            closed,
            closed_notify,
            log,
        })
    }

    /// True once the transport has EOF'd. A closed Client never recovers;
    /// callers that need resilience must build a fresh Client.
    pub fn is_closed(&self) -> bool {
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
        self.send_request_with_timeout(body, DEFAULT_REQUEST_TIMEOUT)
            .await
    }

    async fn send_request_with_timeout(
        &self,
        body: RequestBody,
        timeout: std::time::Duration,
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
            () = tokio::time::sleep(timeout) => {
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
        }
    }

    pub async fn list(
        &self,
        path: &str,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<ListResult, ClientError> {
        let (_, msg) = self
            .send_request(RequestBody::List {
                path: path.into(),
                offset,
                limit,
            })
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::List(result) => Ok(result),
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

    /// Run a command synchronously and return its bounded output preview.
    pub async fn exec(
        &self,
        argv: Vec<String>,
        cwd: Option<String>,
        profile: Option<String>,
        timeout_ms: Option<u64>,
    ) -> Result<ExecResult, ClientError> {
        let wait = std::time::Duration::from_millis(
            timeout_ms
                .unwrap_or(DEFAULT_EXEC_TIMEOUT_MS)
                .saturating_add(30_000),
        );
        let (_, msg) = self
            .send_request_with_timeout(
                RequestBody::Exec {
                    argv,
                    cwd,
                    profile,
                    timeout_ms,
                },
                wait,
            )
            .await?;
        match Self::unpack(msg)? {
            agent_remote_protocol::ResultBody::Exec(result) => Ok(result),
            _ => Err(ClientError::Server(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "unexpected result body for exec",
            ))),
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
/// senders. Dropping a oneshot sender wakes the request with Closed.
async fn drain_waiters(reply_map: &DispMap) {
    reply_map.lock().await.clear();
}

async fn reader_loop(stdout: ChildStdout, reply_map: DispMap, log: Option<Arc<ClientLog>>) {
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
        };
        if let Some(l) = &log {
            l.log_raw(&rid, trimmed).await;
        }
        debug!(request_id = %rid, "recv");
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

/// Argv for spawning the server as a local subprocess: plain argv elements,
/// no shell involved.
pub fn local_server_argv(
    server_bin: &str,
    root: &str,
    config: Option<&str>,
    state_base: Option<&str>,
) -> Vec<String> {
    let mut argv = vec![server_bin.into(), "--root".into(), root.into()];
    if let Some(c) = config {
        argv.push("--config".into());
        argv.push(c.into());
    }
    if let Some(b) = state_base {
        argv.push("--state-base".into());
        argv.push(b.into());
    }
    argv
}

/// Build the argv for spawning the server over ssh. `ssh` joins its trailing
/// arguments with spaces and hands the result to the remote shell, so every
/// remote-side argument is quoted into one command string.
pub fn ssh_server_argv(
    host: &str,
    remote_bin: &str,
    root: &str,
    config: Option<&str>,
    state_base: Option<&str>,
) -> Vec<String> {
    let mut cmd = shell_quote(remote_bin);
    cmd.push_str(" --root ");
    cmd.push_str(&shell_quote(root));
    if let Some(c) = config {
        cmd.push_str(" --config ");
        cmd.push_str(&shell_quote(c));
    }
    if let Some(b) = state_base {
        cmd.push_str(" --state-base ");
        cmd.push_str(&shell_quote(b));
    }
    // BatchMode: fail fast instead of hanging on an auth prompt (there is no
    // tty when spawned by an agent). ServerAlive: keep NAT'd / idle-pruning
    // connections open across long sessions.
    vec![
        "ssh".into(),
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ServerAliveInterval=30".into(),
        "-o".into(),
        "ServerAliveCountMax=4".into(),
        host.into(),
        cmd,
    ]
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
            Some("/data/sicheng/agent state"),
        );
        assert_eq!(argv[0], "ssh");
        // Keepalive/batch options come before the host.
        assert!(argv.contains(&"BatchMode=yes".to_string()));
        let host_pos = argv.iter().position(|a| a == "host").unwrap();
        assert_eq!(host_pos, argv.len() - 2, "host is second to last");
        assert_eq!(
            argv[argv.len() - 1],
            "'agent-remote-server' --root '/data/my project' --state-base '/data/sicheng/agent state'"
        );
    }
}
