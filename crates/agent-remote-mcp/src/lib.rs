use std::sync::Arc;

use agent_remote_client::{Client, Endpoint};
use agent_remote_protocol::ListKind;
use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ServerHandler,
};
use serde::Deserialize;

const SERVER_NAME: &str = "agent-remote-mcp";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const AGENT_GUIDANCE: &str = include_str!("../../../AGENT_GUIDANCE.md");

// ---- Helpers ----

fn ok(text: impl Into<String>) -> CallToolResult {
    CallToolResult::success(vec![Content::text(text)])
}

fn err(text: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![Content::text(text)])
}

// ---- Input structs ----

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListDirInput {
    #[schemars(
        description = "Directory path relative to workspace, or @scratch/... for server-managed scratch"
    )]
    pub path: String,
    #[schemars(description = "Entry offset to start at (default: 0)")]
    pub offset: Option<usize>,
    #[schemars(description = "Maximum entries to return (default and maximum: 1000)")]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReadFileInput {
    #[schemars(
        description = "File path relative to workspace, or @scratch/... for server-managed scratch"
    )]
    pub path: String,
    #[schemars(description = "Byte offset to start reading from (default 0)")]
    pub offset: Option<u64>,
    #[schemars(description = "Maximum bytes to read (default and hard maximum: 65536)")]
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StatInput {
    #[schemars(
        description = "File or directory path relative to workspace, or @scratch/... for server-managed scratch"
    )]
    pub path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WriteFileInput {
    #[schemars(
        description = "File path relative to workspace, or @scratch/... for server-managed scratch"
    )]
    pub path: String,
    #[schemars(description = "Full file content to write")]
    pub content: String,
    #[schemars(
        description = "Expected current hash for optimistic concurrency; omit to skip check"
    )]
    pub base_hash: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PatchFileInput {
    #[schemars(
        description = "File path relative to workspace, or @scratch/... for server-managed scratch"
    )]
    pub path: String,
    #[schemars(description = "Expected current hash (required for optimistic concurrency)")]
    pub base_hash: String,
    #[schemars(description = "Patch script: one edit per line, e.g. \"2c NEW\" to change line 2")]
    pub patch: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DeleteFileInput {
    #[schemars(
        description = "File path relative to workspace, or @scratch/... for server-managed scratch"
    )]
    pub path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunCommandInput {
    #[schemars(description = "Command and arguments, e.g. [\"pytest\", \"-q\"]")]
    pub argv: Vec<String>,
    #[schemars(
        description = "Working directory relative to workspace, or @scratch/... (default: workspace root)"
    )]
    pub cwd: Option<String>,
    #[schemars(description = "Environment profile name (configured server-side)")]
    pub profile: Option<String>,
    #[schemars(description = "Timeout in milliseconds (default: 300000; maximum: 3600000)")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UndoInput {
    #[schemars(description = "Operation ID to undo (from write/patch/delete result)")]
    pub operation_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct HistoryInput {
    #[schemars(description = "Maximum operations to return (default: 50; maximum: 100)")]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct OperationGetInput {
    #[schemars(description = "Operation ID to look up")]
    pub operation_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RequestStatusInput {
    #[schemars(description = "Request ID whose status to query")]
    pub request_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UploadFileInput {
    #[schemars(description = "Absolute or relative path of the local source file")]
    pub local_path: String,
    #[schemars(
        description = "Destination path relative to workspace, or @scratch/... for server-managed scratch"
    )]
    pub remote_path: String,
    #[schemars(description = "Replace an existing destination file (default: false)")]
    pub overwrite: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DownloadFileInput {
    #[schemars(
        description = "Source path relative to workspace, or @scratch/... for server-managed scratch"
    )]
    pub remote_path: String,
    #[schemars(description = "Absolute or relative path of the local destination file")]
    pub local_path: String,
    #[schemars(description = "Replace an existing destination file (default: false)")]
    pub overwrite: Option<bool>,
}

// ---- MCP server ----

pub struct RemoteWorkspaceServer {
    /// Where the server runs; source of the control-plane argv used to
    /// (re)spawn the transport and of the raw transfer argvs.
    endpoint: Endpoint,
    /// Current connection. A Client never recovers once its transport dies
    /// (e.g. sshd resetting the connection), so tool calls fetch it through
    /// `client()`, which reconnects on demand.
    client_slot: tokio::sync::Mutex<Option<Arc<Client>>>,
}

const CONNECT_ATTEMPTS: u32 = 4;
const CONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

impl RemoteWorkspaceServer {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            client_slot: tokio::sync::Mutex::new(None),
        }
    }

    /// Returns a live client, (re)connecting with retries if there is none or
    /// the previous connection died. A fresh connection is probed with a real
    /// round-trip, because a transport can spawn fine and die immediately
    /// (e.g. sshd resetting rapid successive connections).
    async fn client(&self) -> Result<Arc<Client>, String> {
        let mut slot = self.client_slot.lock().await;
        if let Some(c) = slot.as_ref() {
            if !c.is_closed() {
                return Ok(c.clone());
            }
        }
        let mut last = String::new();
        for attempt in 1..=CONNECT_ATTEMPTS {
            if attempt > 1 {
                tokio::time::sleep(CONNECT_BACKOFF).await;
            }
            let transport = agent_remote_client::ArgvTransport {
                argv: self.endpoint.control_argv(),
            };
            match Client::connect(transport, None).await {
                Ok(c) => match c.stat(".").await {
                    Ok(_) => {
                        let c = Arc::new(c);
                        *slot = Some(c.clone());
                        return Ok(c);
                    }
                    Err(e) => last = format!("attempt {attempt}: connection probe failed: {e}"),
                },
                Err(e) => last = format!("attempt {attempt}: connect failed: {e}"),
            }
        }
        Err(format!(
            "cannot reach the remote workspace after {CONNECT_ATTEMPTS} attempts ({last})"
        ))
    }
}

#[tool_router]
impl RemoteWorkspaceServer {
    #[tool(description = "List the contents of a directory in the remote workspace.")]
    async fn list_dir(
        &self,
        Parameters(ListDirInput {
            path,
            offset,
            limit,
        }): Parameters<ListDirInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.list(&path, offset, limit).await {
            Ok(result) => {
                if result.entries.is_empty() {
                    return ok("(empty directory)");
                }
                let mut out = result
                    .entries
                    .iter()
                    .map(|e| match e.kind {
                        ListKind::Dir => format!("  {}/", e.name),
                        ListKind::File => match e.size {
                            Some(s) => format!("  {} ({} bytes)", e.name, s),
                            None => format!("  {}", e.name),
                        },
                        ListKind::Symlink => format!("  {} ->", e.name),
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                if let Some(next) = result.next_offset {
                    out.push_str(&format!("\n[more entries: use offset={next}]"));
                }
                ok(out)
            }
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Read the content of a file. Returns text, a hash for concurrency, and truncation status."
    )]
    async fn read_file(
        &self,
        Parameters(ReadFileInput {
            path,
            offset,
            limit,
        }): Parameters<ReadFileInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.read(&path, offset, limit).await {
            Ok(r) => {
                let mut out = r.content;
                if let Some(next) = r.next_offset {
                    out.push_str(&format!(
                        "\n\n[output truncated; use offset={next} to read more]"
                    ));
                }
                if let Some(hash) = &r.hash {
                    out.push_str(&format!("\n\n[hash: {hash}]"));
                }
                ok(out)
            }
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(description = "Get metadata for a file or directory: type, size, hash, permissions.")]
    async fn stat(&self, Parameters(StatInput { path }): Parameters<StatInput>) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.stat(&path).await {
            Ok(s) => ok(serde_json::to_string_pretty(&s)
                .unwrap_or_else(|e| format!("stat ok, serialize error: {e}"))),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Write content to a file (full overwrite). Returns operation_id and new hash."
    )]
    async fn write_file(
        &self,
        Parameters(WriteFileInput {
            path,
            content,
            base_hash,
        }): Parameters<WriteFileInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.write(&path, &content, base_hash.as_deref()).await {
            Ok(w) => ok(format!(
                "Wrote {path}. operation_id={}, new_hash={}",
                w.operation_id, w.new_hash
            )),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Apply a line-based patch. Requires base_hash. Format: \"<n>c <text>\", \"<n>d\", \"<n>a <text>\"."
    )]
    async fn patch_file(
        &self,
        Parameters(PatchFileInput {
            path,
            base_hash,
            patch,
        }): Parameters<PatchFileInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.patch(&path, &base_hash, &patch).await {
            Ok(w) => ok(format!(
                "Patched {path}. operation_id={}, new_hash={}",
                w.operation_id, w.new_hash
            )),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(description = "Delete a file. Recorded in the operation log and can be undone.")]
    async fn delete_file(
        &self,
        Parameters(DeleteFileInput { path }): Parameters<DeleteFileInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.delete(&path).await {
            Ok(w) => ok(format!("Deleted {path}. operation_id={}", w.operation_id)),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Run a command synchronously. Returns termination, duration, and a fixed-size preview of each output stream (first 4 KiB and last 12 KiB). Redirect full output to $AGENT_REMOTE_SCRATCH and read it through @scratch/... when needed."
    )]
    async fn run_command(
        &self,
        Parameters(RunCommandInput {
            argv,
            cwd,
            profile,
            timeout_ms,
        }): Parameters<RunCommandInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        let result = client.exec(argv, cwd, profile, timeout_ms).await;
        match result {
            Ok(result) => match serde_json::to_string_pretty(&result) {
                Ok(text) => ok(text),
                Err(e) => err(format!("could not serialize command result: {e}")),
            },
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Upload one local regular file to the remote workspace as raw streamed bytes; the file content never enters the model context, so use this (not write_file or shell tricks) for binary or large files. remote_path is workspace-relative or @scratch/...; its parent directory must already exist. Synchronous: the call returns only when the file is fully installed remotely, so a long-running call is normal for big files. Existing destinations are never replaced unless overwrite=true."
    )]
    async fn upload_file(
        &self,
        Parameters(UploadFileInput {
            local_path,
            remote_path,
            overwrite,
        }): Parameters<UploadFileInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match agent_remote_client::upload_file(
            &client,
            &self.endpoint,
            std::path::Path::new(&local_path),
            &remote_path,
            overwrite.unwrap_or(false),
            None,
        )
        .await
        {
            Ok(r) => ok(serde_json::to_string_pretty(&r)
                .unwrap_or_else(|e| format!("upload ok, serialize error: {e}"))),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Download one remote regular file to the local machine as raw streamed bytes; the file content never enters the model context, so use this (not read_file) for binary or large files. remote_path is workspace-relative or @scratch/...; the local parent directory must already exist. Synchronous: the call returns only when the file is fully installed locally, so a long-running call is normal for big files. Existing destinations are never replaced unless overwrite=true."
    )]
    async fn download_file(
        &self,
        Parameters(DownloadFileInput {
            remote_path,
            local_path,
            overwrite,
        }): Parameters<DownloadFileInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match agent_remote_client::download_file(
            &client,
            &self.endpoint,
            &remote_path,
            std::path::Path::new(&local_path),
            overwrite.unwrap_or(false),
            None,
        )
        .await
        {
            Ok(mut r) => {
                // For a download the useful path is the local destination; the
                // server-side record keeps the remote logical path.
                r.path = local_path;
                ok(serde_json::to_string_pretty(&r)
                    .unwrap_or_else(|e| format!("download ok, serialize error: {e}")))
            }
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Undo a recorded file operation. Only works if the file has not been modified since."
    )]
    async fn undo(
        &self,
        Parameters(UndoInput { operation_id }): Parameters<UndoInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.undo(&operation_id).await {
            Ok(u) => ok(format!(
                "Undid target {operation_id}; undo_operation_id={}, new_hash={}",
                u.operation_id, u.new_hash
            )),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Show the history of recorded operations (file mutations and exec invocations)."
    )]
    async fn history(
        &self,
        Parameters(HistoryInput { limit }): Parameters<HistoryInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.history(limit).await {
            Ok(ops) => {
                if ops.is_empty() {
                    return ok("(no operations recorded)");
                }
                let out = ops
                    .iter()
                    .map(|r| serde_json::to_string(r).unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join("\n");
                ok(out)
            }
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(description = "Get details of a specific operation by ID.")]
    async fn operation_get(
        &self,
        Parameters(OperationGetInput { operation_id }): Parameters<OperationGetInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.operation_get(&operation_id).await {
            Ok(d) => ok(serde_json::to_string_pretty(&d)
                .unwrap_or_else(|e| format!("serialize error: {e}"))),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(description = "Query the status of a previously-issued request by request ID.")]
    async fn request_status(
        &self,
        Parameters(RequestStatusInput { request_id }): Parameters<RequestStatusInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.request_status(&request_id).await {
            Ok(r) => ok(serde_json::to_string_pretty(&r)
                .unwrap_or_else(|e| format!("serialize error: {e}"))),
            Err(e) => err(format!("{e}")),
        }
    }
}

#[tool_handler]
impl ServerHandler for RemoteWorkspaceServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.server_info.name = SERVER_NAME.into();
        info.server_info.version = SERVER_VERSION.into();
        info.instructions = Some(AGENT_GUIDANCE.into());
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info
    }
}
