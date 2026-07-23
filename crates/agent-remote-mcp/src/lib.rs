use std::collections::BTreeMap;
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

// ---- Fleet configuration ----

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FleetFile {
    workspaces: BTreeMap<String, WorkspaceEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceEntry {
    /// SSH host (resolvable via ~/.ssh/config); omit to run the server on the
    /// local machine.
    host: Option<String>,
    root: String,
    /// Server binary path on that machine. Defaults to `agent-remote-server`
    /// on PATH.
    bin: Option<String>,
    config: Option<String>,
    state_base: Option<String>,
    /// Human-readable description shown by list_workspaces, for telling apart
    /// entries whose names alone are ambiguous.
    label: Option<String>,
}

/// A configured workspace: where its server runs, plus display metadata.
pub struct Workspace {
    pub endpoint: Endpoint,
    pub label: Option<String>,
}

/// Parse and validate a fleet config. Rejects an empty fleet and two
/// workspaces addressing the same (host, root): they would contend for the
/// same server-side state lock and one of them would always fail.
pub fn parse_fleet(text: &str) -> anyhow::Result<BTreeMap<String, Workspace>> {
    let file: FleetFile = toml::from_str(text)?;
    if file.workspaces.is_empty() {
        anyhow::bail!("fleet config declares no workspaces");
    }
    let mut seen: BTreeMap<(Option<String>, String), String> = BTreeMap::new();
    let mut out = BTreeMap::new();
    for (name, entry) in file.workspaces {
        if let Some(prev) = seen.insert((entry.host.clone(), entry.root.clone()), name.clone()) {
            anyhow::bail!(
                "workspaces '{prev}' and '{name}' address the same host and root; \
                 they would contend for the same server state lock"
            );
        }
        let bin = entry.bin.unwrap_or_else(|| "agent-remote-server".into());
        let endpoint = match entry.host {
            Some(host) => Endpoint::Ssh {
                host,
                remote_bin: bin,
                root: entry.root,
                state_base: entry.state_base,
                config: entry.config,
            },
            None => Endpoint::Local {
                server_bin: bin,
                root: entry.root,
                state_base: entry.state_base,
                config: entry.config,
            },
        };
        out.insert(
            name,
            Workspace {
                endpoint,
                label: entry.label,
            },
        );
    }
    Ok(out)
}

/// One-shot health probe of a workspace: spawn its server and do a real
/// round-trip. Single attempt, no retries -- this is a diagnostic, not the
/// resilient tool-call path. The error text starts with a stable code:
/// `connect_failed` (transport/spawn) or `probe_failed` (server reached but
/// the round-trip failed, e.g. bad root or a locked state directory -- see
/// the server's stderr above for its own explanation).
pub async fn check_workspace(endpoint: &Endpoint) -> Result<(), String> {
    let transport = agent_remote_client::ArgvTransport {
        argv: endpoint.control_argv(),
    };
    match Client::connect(transport, None).await {
        Err(e) => Err(format!("connect_failed: {e}")),
        Ok(c) => match c.stat(".").await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("probe_failed: {e}")),
        },
    }
}

// ---- Helpers ----

fn ok(text: impl Into<String>) -> CallToolResult {
    CallToolResult::success(vec![Content::text(text)])
}

fn err(text: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![Content::text(text)])
}

/// Serialize a result object with the workspace name injected, so the agent
/// never has to guess which workspace an operation_id or path belongs to.
fn ok_json_in_workspace<T: serde::Serialize>(workspace: &str, value: &T) -> CallToolResult {
    let mut v = match serde_json::to_value(value) {
        Ok(v) => v,
        Err(e) => return err(format!("result serialize error: {e}")),
    };
    if let Some(obj) = v.as_object_mut() {
        obj.insert("workspace".into(), workspace.into());
    }
    match serde_json::to_string_pretty(&v) {
        Ok(text) => ok(text),
        Err(e) => err(format!("result serialize error: {e}")),
    }
}

// ---- Input structs ----

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListDirInput {
    #[schemars(description = "Workspace name (see list_workspaces)")]
    pub workspace: String,
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
    #[schemars(description = "Workspace name (see list_workspaces)")]
    pub workspace: String,
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
    #[schemars(description = "Workspace name (see list_workspaces)")]
    pub workspace: String,
    #[schemars(
        description = "File or directory path relative to workspace, or @scratch/... for server-managed scratch"
    )]
    pub path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WriteFileInput {
    #[schemars(description = "Workspace name (see list_workspaces)")]
    pub workspace: String,
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
    #[schemars(description = "Workspace name (see list_workspaces)")]
    pub workspace: String,
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
    #[schemars(description = "Workspace name (see list_workspaces)")]
    pub workspace: String,
    #[schemars(
        description = "File path relative to workspace, or @scratch/... for server-managed scratch"
    )]
    pub path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunCommandInput {
    #[schemars(description = "Workspace name (see list_workspaces)")]
    pub workspace: String,
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
    #[schemars(description = "Workspace name the operation was recorded in")]
    pub workspace: String,
    #[schemars(description = "Operation ID to undo (from write/patch/delete result)")]
    pub operation_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct HistoryInput {
    #[schemars(description = "Workspace name (see list_workspaces)")]
    pub workspace: String,
    #[schemars(description = "Maximum operations to return (default: 50; maximum: 100)")]
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct OperationGetInput {
    #[schemars(description = "Workspace name the operation was recorded in")]
    pub workspace: String,
    #[schemars(description = "Operation ID to look up")]
    pub operation_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RequestStatusInput {
    #[schemars(description = "Workspace name the request was issued against")]
    pub workspace: String,
    #[schemars(description = "Request ID whose status to query")]
    pub request_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UploadFileInput {
    #[schemars(description = "Destination workspace name (see list_workspaces)")]
    pub workspace: String,
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
    #[schemars(description = "Source workspace name (see list_workspaces)")]
    pub workspace: String,
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

/// One configured workspace: its endpoint plus an independent connection
/// slot, so an unreachable machine's connect retries never block calls to the
/// other workspaces.
struct WorkspaceHandle {
    endpoint: Endpoint,
    label: Option<String>,
    /// Current connection. A Client never recovers once its transport dies
    /// (e.g. sshd resetting the connection), so tool calls fetch it through
    /// `client()`, which reconnects on demand.
    slot: tokio::sync::Mutex<Option<Arc<Client>>>,
}

pub struct RemoteWorkspaceServer {
    /// Immutable after startup: the fleet is fixed for the process lifetime.
    workspaces: BTreeMap<String, WorkspaceHandle>,
}

const CONNECT_ATTEMPTS: u32 = 4;
const CONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

impl RemoteWorkspaceServer {
    pub fn new(fleet: BTreeMap<String, Workspace>) -> Self {
        let workspaces = fleet
            .into_iter()
            .map(|(name, ws)| {
                (
                    name,
                    WorkspaceHandle {
                        endpoint: ws.endpoint,
                        label: ws.label,
                        slot: tokio::sync::Mutex::new(None),
                    },
                )
            })
            .collect();
        Self { workspaces }
    }

    fn handle(&self, workspace: &str) -> Result<&WorkspaceHandle, String> {
        self.workspaces.get(workspace).ok_or_else(|| {
            let names = self
                .workspaces
                .keys()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("unknown_workspace: '{workspace}' is not a configured workspace; available workspaces: {names}")
        })
    }

    /// Returns a live client for the workspace, (re)connecting with retries if
    /// there is none or the previous connection died. A fresh connection is
    /// probed with a real round-trip, because a transport can spawn fine and
    /// die immediately (e.g. sshd resetting rapid successive connections).
    async fn client(&self, workspace: &str) -> Result<(Arc<Client>, &WorkspaceHandle), String> {
        let handle = self.handle(workspace)?;
        let mut slot = handle.slot.lock().await;
        if let Some(c) = slot.as_ref() {
            if !c.is_closed() {
                return Ok((c.clone(), handle));
            }
        }
        // `code` is a stable keyword so agents can tell transport failures
        // (connect_failed) apart from a reachable-but-unhealthy workspace
        // (probe_failed: server spawned, the round-trip did not survive).
        let mut code = "connect_failed";
        let mut last = String::new();
        for attempt in 1..=CONNECT_ATTEMPTS {
            if attempt > 1 {
                tokio::time::sleep(CONNECT_BACKOFF).await;
            }
            let transport = agent_remote_client::ArgvTransport {
                argv: handle.endpoint.control_argv(),
            };
            match Client::connect(transport, None).await {
                Ok(c) => match c.stat(".").await {
                    Ok(_) => {
                        let c = Arc::new(c);
                        *slot = Some(c.clone());
                        return Ok((c, handle));
                    }
                    Err(e) => {
                        code = "probe_failed";
                        last = format!("attempt {attempt}: {e}");
                    }
                },
                Err(e) => {
                    code = "connect_failed";
                    last = format!("attempt {attempt}: {e}");
                }
            }
        }
        Err(format!(
            "{code}: cannot reach workspace '{workspace}' after {CONNECT_ATTEMPTS} attempts ({last})"
        ))
    }
}

#[tool_router]
impl RemoteWorkspaceServer {
    #[tool(
        description = "List the configured workspaces: name, host, and root directory. Every other tool requires one of these names as its workspace argument."
    )]
    async fn list_workspaces(&self) -> CallToolResult {
        let rows: Vec<serde_json::Value> = self
            .workspaces
            .iter()
            .map(|(name, h)| {
                let (host, root) = match &h.endpoint {
                    Endpoint::Ssh { host, root, .. } => (host.as_str(), root.as_str()),
                    Endpoint::Local { root, .. } => ("(local)", root.as_str()),
                };
                let mut row = serde_json::json!({"name": name, "host": host, "root": root});
                if let Some(label) = &h.label {
                    row["label"] = serde_json::Value::String(label.clone());
                }
                row
            })
            .collect();
        match serde_json::to_string_pretty(&rows) {
            Ok(text) => ok(text),
            Err(e) => err(format!("serialize error: {e}")),
        }
    }

    #[tool(description = "List the contents of a directory in a workspace.")]
    async fn list_dir(
        &self,
        Parameters(ListDirInput {
            workspace,
            path,
            offset,
            limit,
        }): Parameters<ListDirInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
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
            workspace,
            path,
            offset,
            limit,
        }): Parameters<ReadFileInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
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
    async fn stat(
        &self,
        Parameters(StatInput { workspace, path }): Parameters<StatInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.stat(&path).await {
            Ok(s) => ok_json_in_workspace(&workspace, &s),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Write content to a file (full overwrite). Returns operation_id and new hash."
    )]
    async fn write_file(
        &self,
        Parameters(WriteFileInput {
            workspace,
            path,
            content,
            base_hash,
        }): Parameters<WriteFileInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.write(&path, &content, base_hash.as_deref()).await {
            Ok(w) => ok(format!(
                "Wrote {path} in workspace '{workspace}'. operation_id={}, new_hash={}",
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
            workspace,
            path,
            base_hash,
            patch,
        }): Parameters<PatchFileInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.patch(&path, &base_hash, &patch).await {
            Ok(w) => ok(format!(
                "Patched {path} in workspace '{workspace}'. operation_id={}, new_hash={}",
                w.operation_id, w.new_hash
            )),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(description = "Delete a file. Recorded in the operation log and can be undone.")]
    async fn delete_file(
        &self,
        Parameters(DeleteFileInput { workspace, path }): Parameters<DeleteFileInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.delete(&path).await {
            Ok(w) => ok(format!(
                "Deleted {path} in workspace '{workspace}'. operation_id={}",
                w.operation_id
            )),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Run a command synchronously in a workspace. Returns termination, duration, and a fixed-size preview of each output stream (first 4 KiB and last 12 KiB). Redirect full output to $AGENT_REMOTE_SCRATCH and read it through @scratch/... when needed."
    )]
    async fn run_command(
        &self,
        Parameters(RunCommandInput {
            workspace,
            argv,
            cwd,
            profile,
            timeout_ms,
        }): Parameters<RunCommandInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.exec(argv, cwd, profile, timeout_ms).await {
            Ok(result) => ok_json_in_workspace(&workspace, &result),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Upload one local regular file to a workspace as raw streamed bytes; the file content never enters the model context, so use this (not write_file or shell tricks) for binary or large files. remote_path is workspace-relative or @scratch/...; its parent directory must already exist. Synchronous: the call returns only when the file is fully installed remotely, so a long-running call is normal for big files. Existing destinations are never replaced unless overwrite=true."
    )]
    async fn upload_file(
        &self,
        Parameters(UploadFileInput {
            workspace,
            local_path,
            remote_path,
            overwrite,
        }): Parameters<UploadFileInput>,
    ) -> CallToolResult {
        let (client, handle) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match agent_remote_client::upload_file(
            &client,
            &handle.endpoint,
            std::path::Path::new(&local_path),
            &remote_path,
            overwrite.unwrap_or(false),
            None,
        )
        .await
        {
            Ok(r) => ok_json_in_workspace(&workspace, &r),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Download one remote regular file from a workspace to the local machine as raw streamed bytes; the file content never enters the model context, so use this (not read_file) for binary or large files. remote_path is workspace-relative or @scratch/...; the local parent directory must already exist. Synchronous: the call returns only when the file is fully installed locally, so a long-running call is normal for big files. Existing destinations are never replaced unless overwrite=true."
    )]
    async fn download_file(
        &self,
        Parameters(DownloadFileInput {
            workspace,
            remote_path,
            local_path,
            overwrite,
        }): Parameters<DownloadFileInput>,
    ) -> CallToolResult {
        let (client, handle) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match agent_remote_client::download_file(
            &client,
            &handle.endpoint,
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
                ok_json_in_workspace(&workspace, &r)
            }
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Undo a recorded file operation. Only works if the file has not been modified since. Operation IDs are scoped to one workspace; pass the workspace the operation was recorded in."
    )]
    async fn undo(
        &self,
        Parameters(UndoInput {
            workspace,
            operation_id,
        }): Parameters<UndoInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.undo(&operation_id).await {
            Ok(u) => ok(format!(
                "Undid target {operation_id} in workspace '{workspace}'; undo_operation_id={}, new_hash={}",
                u.operation_id, u.new_hash
            )),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Show the history of operations recorded in one workspace (file mutations, exec invocations, transfers)."
    )]
    async fn history(
        &self,
        Parameters(HistoryInput { workspace, limit }): Parameters<HistoryInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.history(limit).await {
            Ok(ops) => {
                if ops.is_empty() {
                    return ok(format!(
                        "(no operations recorded in workspace '{workspace}')"
                    ));
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

    #[tool(description = "Get details of a specific operation by ID, within one workspace.")]
    async fn operation_get(
        &self,
        Parameters(OperationGetInput {
            workspace,
            operation_id,
        }): Parameters<OperationGetInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.operation_get(&operation_id).await {
            Ok(d) => ok_json_in_workspace(&workspace, &d),
            Err(e) => err(format!("{e}")),
        }
    }

    #[tool(
        description = "Query the status of a previously-issued request by request ID, within one workspace."
    )]
    async fn request_status(
        &self,
        Parameters(RequestStatusInput {
            workspace,
            request_id,
        }): Parameters<RequestStatusInput>,
    ) -> CallToolResult {
        let (client, _) = match self.client(&workspace).await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.request_status(&request_id).await {
            Ok(r) => ok_json_in_workspace(&workspace, &r),
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
