use std::path::PathBuf;
use std::sync::Arc;

use agent_remote_protocol::{
    ErrorCode, OperationDetails, Request, RequestBody, ResultBody, ServerMessage,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::config::ServerConfig;
use crate::exec;
use crate::fs_ops;
use crate::store::{OperationStore, StoredResult};
use crate::undo;
use crate::workspace::Workspace;

pub struct Server {
    pub workspace: Arc<Workspace>,
    pub store: OperationStore,
    pub config: Arc<ServerConfig>,
    history_limit: Option<usize>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server").finish_non_exhaustive()
    }
}

pub struct ServerOptions {
    pub root: PathBuf,
    pub log_dir: PathBuf,
    pub config_path: Option<PathBuf>,
    /// Keep only this many recent operations; pruned automatically at startup
    /// and on `gc`. `None` disables automatic pruning.
    pub history_limit: Option<usize>,
}

impl Server {
    pub fn new(opts: ServerOptions) -> anyhow::Result<Self> {
        let workspace = Arc::new(Workspace::new(opts.root)?);
        let store = OperationStore::new(opts.log_dir).map_err(|e| anyhow::anyhow!(e))?;
        // Run WAL recovery before serving: reconcile any prepared markers left
        // by a crash, and clear requests stuck InProgress so they become retryable.
        let actions = store
            .recover(&workspace)
            .map_err(|e| anyhow::anyhow!("startup recovery failed: {e}"))?;
        for a in &actions {
            tracing::info!(action = ?a, "recovery");
        }
        if actions
            .iter()
            .any(|a| matches!(a, crate::store::RecoveryAction::Conflict { .. }))
        {
            tracing::warn!("startup recovery encountered one or more conflicts; affected requests are marked Done with an error");
        }
        if let Some(keep) = opts.history_limit {
            let stats = store
                .prune(keep)
                .map_err(|e| anyhow::anyhow!("startup prune failed: {e}"))?;
            if stats.removed_operations > 0 || stats.removed_requests > 0 {
                tracing::info!(
                    removed_operations = stats.removed_operations,
                    removed_requests = stats.removed_requests,
                    "pruned history at startup"
                );
            }
        }
        let config = match opts.config_path {
            Some(p) => {
                let text = std::fs::read_to_string(&p)
                    .map_err(|e| anyhow::anyhow!("read config {p:?}: {e}"))?;
                Arc::new(ServerConfig::load_from_str(&text)?)
            }
            None => Arc::new(ServerConfig::default()),
        };
        Ok(Self {
            workspace,
            store,
            config,
            history_limit: opts.history_limit,
        })
    }

    pub async fn run_stdio(self) -> std::io::Result<()> {
        let stdin = tokio::io::stdin();
        let stdout = tokio::io::stdout();
        self.run(stdin, stdout).await
    }

    pub async fn run<R, W>(self, read: R, write: W) -> std::io::Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + Send,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let server = Arc::new(self);
        let mut reader = BufReader::new(read);
        let stdout: Arc<tokio::sync::Mutex<W>> = Arc::new(tokio::sync::Mutex::new(write));
        let mut line = String::new();

        loop {
            line.clear();
            let n = reader.read_line(&mut line).await?;
            if n == 0 {
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let req: Request = match serde_json::from_str(trimmed) {
                Ok(r) => r,
                Err(e) => {
                    let msg = ServerMessage::Error {
                        request_id: "(parse-error)".into(),
                        error: agent_remote_protocol::ProtocolError::new(
                            ErrorCode::InvalidRequest,
                            format!("invalid request line: {e}"),
                        ),
                    };
                    write_line(&stdout, &msg).await;
                    continue;
                }
            };
            let server = server.clone();
            let stdout = stdout.clone();
            tokio::spawn(async move {
                server.handle(req, stdout).await;
            });
        }
        Ok(())
    }

    async fn handle<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        req: Request,
        stdout: Arc<tokio::sync::Mutex<W>>,
    ) {
        let request_id = req.request_id.clone();

        // Idempotency via atomic claim: if we have seen this request_id, replay
        // its stored result without re-executing. Otherwise this call wins
        // ownership and proceeds. claim_request is a single locked
        // check-and-insert, so concurrent duplicate requests cannot both run.
        let op_kind = op_kind_str(&req.body);
        match self.store.claim_request(&request_id, op_kind) {
            Ok(None) => {} // won ownership; proceed to dispatch below.
            Ok(Some(entry)) => match entry.result {
                Some(StoredResult::Done(m)) => {
                    write_line(&stdout, &m).await;
                    return;
                }
                Some(StoredResult::Error(e)) => {
                    write_line(
                        &stdout,
                        &ServerMessage::Error {
                            request_id,
                            error: e,
                        },
                    )
                    .await;
                    return;
                }
                // A genuinely in-flight request should not happen in a
                // single-connection server, but if it does, refuse rather than
                // re-execute.
                None => {
                    write_line(
                        &stdout,
                        &ServerMessage::Error {
                            request_id,
                            error: agent_remote_protocol::ProtocolError::new(
                                ErrorCode::InvalidRequest,
                                "request already in progress",
                            ),
                        },
                    )
                    .await;
                    return;
                }
            },
            // Claiming the request failed (e.g. request log is not writable).
            // Surface the error; do NOT execute, since we cannot record state.
            Err(e) => {
                write_line(
                    &stdout,
                    &ServerMessage::Error {
                        request_id,
                        error: e,
                    },
                )
                .await;
                return;
            }
        }

        match req.body {
            RequestBody::List { path } => {
                self.finish(&request_id, fs_ops::list(&self.workspace, &path))
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::Stat { path } => {
                self.finish(&request_id, fs_ops::stat(&self.workspace, &path))
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::Read {
                path,
                offset,
                limit,
            } => {
                self.finish(
                    &request_id,
                    fs_ops::read(&self.workspace, &path, offset, limit),
                )
                .await
                .with_stdout(&stdout)
                .await;
            }
            RequestBody::Write {
                path,
                content,
                base_hash,
            } => {
                let guard = self.store.write_guard().await;
                let result = fs_ops::write(
                    &self.workspace,
                    &self.store,
                    &guard,
                    &request_id,
                    &path,
                    &content,
                    &base_hash,
                );
                drop(guard);
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::Patch {
                path,
                base_hash,
                patch,
            } => {
                let guard = self.store.write_guard().await;
                let result = fs_ops::patch(
                    &self.workspace,
                    &self.store,
                    &guard,
                    &request_id,
                    &path,
                    &base_hash,
                    &patch,
                );
                drop(guard);
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::Exec {
                argv,
                cwd,
                profile,
                timeout_ms,
            } => {
                self.handle_exec(&request_id, argv, cwd, profile, timeout_ms, stdout)
                    .await;
            }
            RequestBody::Delete { path } => {
                let guard = self.store.write_guard().await;
                let result =
                    fs_ops::delete(&self.workspace, &self.store, &guard, &request_id, &path);
                drop(guard);
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::Undo { operation_id } => {
                let guard = self.store.write_guard().await;
                let result = match self.store.find_record(&operation_id) {
                    Some(agent_remote_protocol::AnyOperationRecord::Fs(fs)) => {
                        undo::undo(&self.workspace, &self.store, &guard, &request_id, &fs)
                    }
                    Some(agent_remote_protocol::AnyOperationRecord::Exec(_)) => {
                        Err(agent_remote_protocol::ProtocolError::new(
                            ErrorCode::InvalidRequest,
                            "cannot undo an exec operation",
                        ))
                    }
                    // A Prepared/Aborted marker should have been reconciled at
                    // startup. If one is still here, treat the op as not found.
                    Some(
                        agent_remote_protocol::AnyOperationRecord::Prepared(_)
                        | agent_remote_protocol::AnyOperationRecord::Aborted(_),
                    )
                    | None => Err(agent_remote_protocol::ProtocolError::new(
                        ErrorCode::OperationNotFound,
                        format!("operation not found: {operation_id}"),
                    )),
                };
                drop(guard);
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::History { limit } => {
                let operations = self.store.history(limit);
                self.finish(&request_id, Ok(ResultBody::History { operations }))
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::OperationGet { operation_id } => {
                match self.store.find_record(&operation_id) {
                    Some(
                        record @ (agent_remote_protocol::AnyOperationRecord::Fs(_)
                        | agent_remote_protocol::AnyOperationRecord::Exec(_)),
                    ) => {
                        self.finish(
                            &request_id,
                            Ok(ResultBody::Operation(OperationDetails { record })),
                        )
                        .await
                        .with_stdout(&stdout)
                        .await;
                    }
                    None | Some(_) => {
                        // Prepared/Aborted already filtered by find_record
                        let err = agent_remote_protocol::ProtocolError::new(
                            ErrorCode::OperationNotFound,
                            format!("operation not found: {operation_id}"),
                        );
                        self.finish_err(&request_id, err)
                            .await
                            .with_stdout(&stdout)
                            .await;
                    }
                }
            }
            RequestBody::RequestStatus { target: rid } => {
                let result = self.store.status_for_request(&rid);
                self.finish(&request_id, Ok(ResultBody::RequestStatus(result)))
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
            RequestBody::Gc { keep } => {
                let guard = self.store.write_guard().await;
                let result = match keep.or(self.history_limit) {
                    Some(k) => self.store.prune(k).map(|s| {
                        ResultBody::Gc(agent_remote_protocol::GcResult {
                            removed_operations: s.removed_operations,
                            removed_requests: s.removed_requests,
                            retained_operations: s.retained_operations,
                        })
                    }),
                    None => Err(agent_remote_protocol::ProtocolError::new(
                        ErrorCode::InvalidRequest,
                        "server has no history limit configured; pass keep explicitly",
                    )),
                };
                drop(guard);
                self.finish(&request_id, result)
                    .await
                    .with_stdout(&stdout)
                    .await;
            }
        }
    }

    async fn handle_exec<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        request_id: &str,
        argv: Vec<String>,
        cwd: Option<String>,
        profile: Option<String>,
        timeout_ms: Option<u64>,
        stdout: Arc<tokio::sync::Mutex<W>>,
    ) {
        // Allocate the operation id up front so that even a rejected exec
        // (bad profile, empty argv, missing cwd) consumes an id and is
        // recorded. This keeps ids monotonic and lets operation.get/history
        // report the attempted command.
        let operation_id = self.store.next_operation_id();
        let ws = self.workspace.clone();
        let config = self.config.clone();

        let stdout_for_emit = stdout.clone();
        let rid_emit = request_id.to_string();
        let emit = move |kind: agent_remote_protocol::ExecEventKind| {
            let stdout = stdout_for_emit.clone();
            let rid = rid_emit.clone();
            async move {
                let msg = ServerMessage::ExecEvent(agent_remote_protocol::ExecEvent {
                    request_id: rid,
                    event: kind,
                });
                write_line(&stdout, &msg).await;
            }
        };

        let outcome = exec::exec(
            &ws,
            &config,
            cwd.as_deref(),
            profile.as_deref(),
            &argv,
            timeout_ms,
            operation_id.clone(),
            emit,
        )
        .await;

        match outcome {
            Ok(o) => {
                // disposition reflects what actually happened: Completed if it
                // ran to an exit code, TimedOut if killed by timeout (but it
                // DID run, so duration and captured output are meaningful).
                let disposition = if o.timed_out {
                    agent_remote_protocol::ExecDisposition::TimedOut
                } else {
                    agent_remote_protocol::ExecDisposition::Completed
                };
                let record = agent_remote_protocol::ExecOperationRecord {
                    operation_id: o.operation_id.clone(),
                    request_id: request_id.to_string(),
                    argv,
                    cwd,
                    profile,
                    timeout_ms,
                    disposition,
                    exit_code: Some(o.exit_code),
                    duration_ms: o.duration_ms,
                    timestamp_ms: now_ms(),
                    error: if o.timed_out {
                        Some(format!("killed after {timeout_ms:?} ms timeout"))
                    } else {
                        None
                    },
                    error_code: if o.timed_out {
                        Some(agent_remote_protocol::ErrorCode::ExecFailed)
                    } else {
                        None
                    },
                    output_truncated: if o.output_truncated { Some(true) } else { None },
                };
                if let Err(e) =
                    self.store
                        .append_exec_record(record, Some(&o.stdout), Some(&o.stderr))
                {
                    let _ = self.store.remember_error(request_id, e.clone());
                    write_line(
                        &stdout,
                        &ServerMessage::Error {
                            request_id: request_id.to_string(),
                            error: e,
                        },
                    )
                    .await;
                    return;
                }
                let body = ServerMessage::Result {
                    request_id: request_id.to_string(),
                    result: ResultBody::Exit {
                        exit_code: o.exit_code,
                        operation_id: o.operation_id,
                    },
                };
                if let Err(log_err) = self.store.remember_result(request_id, body.clone()) {
                    write_line(
                        &stdout,
                        &ServerMessage::Error {
                            request_id: request_id.to_string(),
                            error: log_err,
                        },
                    )
                    .await;
                    return;
                }
                write_line(&stdout, &body).await;
            }
            Err(e) => {
                // Rejected: the command never started (bad profile/cwd/argv, or
                // spawn failure). It consumed an id, so record it with the
                // Rejected disposition. Logging failures are surfaced, not
                // swallowed.
                let record = agent_remote_protocol::ExecOperationRecord {
                    operation_id,
                    request_id: request_id.to_string(),
                    argv,
                    cwd,
                    profile,
                    timeout_ms,
                    disposition: agent_remote_protocol::ExecDisposition::Rejected,
                    exit_code: None,
                    duration_ms: 0,
                    timestamp_ms: now_ms(),
                    error: Some(e.message.clone()),
                    error_code: Some(e.code),
                    output_truncated: None,
                };
                let record_err = self.store.append_exec_record(record, None, None).err();
                let remember_err = self.store.remember_error(request_id, e.clone()).err();
                let report = remember_err.or(record_err).unwrap_or(e);
                write_line(
                    &stdout,
                    &ServerMessage::Error {
                        request_id: request_id.to_string(),
                        error: report,
                    },
                )
                .await;
            }
        }
    }

    /// Wrap a sync result into a ServerMessage, remember it, and return it for
    /// writing. If persisting the result to the request log fails, the client
    /// is told the operation failed (with an IO error), so the server never
    /// reports success for state it could not durably record. This honors the
    /// repo's no-silent-failure rule.
    async fn finish(
        &self,
        request_id: &str,
        result: Result<ResultBody, agent_remote_protocol::ProtocolError>,
    ) -> FinishResult {
        match result {
            Ok(body) => {
                let msg = ServerMessage::Result {
                    request_id: request_id.to_string(),
                    result: body,
                };
                match self.store.remember_result(request_id, msg.clone()) {
                    Ok(()) => FinishResult::Msg(msg),
                    Err(log_err) => FinishResult::Msg(ServerMessage::Error {
                        request_id: request_id.to_string(),
                        error: log_err,
                    }),
                }
            }
            Err(e) => match self.store.remember_error(request_id, e.clone()) {
                Ok(()) => FinishResult::Msg(ServerMessage::Error {
                    request_id: request_id.to_string(),
                    error: e,
                }),
                Err(log_err) => FinishResult::Msg(ServerMessage::Error {
                    request_id: request_id.to_string(),
                    error: log_err,
                }),
            },
        }
    }

    async fn finish_err(
        &self,
        request_id: &str,
        err: agent_remote_protocol::ProtocolError,
    ) -> FinishResult {
        match self.store.remember_error(request_id, err.clone()) {
            Ok(()) => FinishResult::Msg(ServerMessage::Error {
                request_id: request_id.to_string(),
                error: err,
            }),
            Err(log_err) => FinishResult::Msg(ServerMessage::Error {
                request_id: request_id.to_string(),
                error: log_err,
            }),
        }
    }
}

enum FinishResult {
    Msg(ServerMessage),
}

impl FinishResult {
    async fn with_stdout<W: tokio::io::AsyncWrite + Unpin + Send>(
        self,
        stdout: &Arc<tokio::sync::Mutex<W>>,
    ) {
        match self {
            FinishResult::Msg(m) => write_line(stdout, &m).await,
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn op_kind_str(body: &RequestBody) -> &str {
    match body {
        RequestBody::List { .. } => "list",
        RequestBody::Stat { .. } => "stat",
        RequestBody::Read { .. } => "read",
        RequestBody::Write { .. } => "write",
        RequestBody::Patch { .. } => "patch",
        RequestBody::Exec { .. } => "exec",
        RequestBody::Delete { .. } => "delete",
        RequestBody::Undo { .. } => "undo",
        RequestBody::History { .. } => "history",
        RequestBody::OperationGet { .. } => "operation_get",
        RequestBody::RequestStatus { .. } => "request_status",
        RequestBody::Gc { .. } => "gc",
    }
}

async fn write_line<W: tokio::io::AsyncWrite + Unpin + Send>(
    stdout: &Arc<tokio::sync::Mutex<W>>,
    msg: &ServerMessage,
) {
    let mut line = serde_json::to_string(msg).expect("server message must serialize");
    line.push('\n');
    let mut g = stdout.lock().await;
    let _ = g.write_all(line.as_bytes()).await;
    let _ = g.flush().await;
}
