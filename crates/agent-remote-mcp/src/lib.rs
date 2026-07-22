use std::sync::Arc;

use agent_remote_client::Client;
use agent_remote_protocol::{ExecEventKind, ListKind};
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ServerHandler,
};
use serde::Deserialize;

const SERVER_NAME: &str = "agent-remote-mcp";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Per-stream accumulation cap for run_command output (16 MiB). The server
/// already caps its own captured blobs at 64 MiB; this lower limit keeps the
/// MCP response from growing unbounded.
const OUTPUT_LIMIT: usize = 16 * 1024 * 1024;

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
    #[schemars(description = "Directory path relative to workspace root (e.g. \"src\" or \".\")")]
    pub path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReadFileInput {
    #[schemars(description = "File path relative to workspace root")]
    pub path: String,
    #[schemars(description = "Byte offset to start reading from (default 0)")]
    pub offset: Option<u64>,
    #[schemars(description = "Maximum bytes to read (default 65536)")]
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StatInput {
    #[schemars(description = "File or directory path relative to workspace root")]
    pub path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WriteFileInput {
    #[schemars(description = "File path relative to workspace root")]
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
    #[schemars(description = "File path relative to workspace root")]
    pub path: String,
    #[schemars(description = "Expected current hash (required for optimistic concurrency)")]
    pub base_hash: String,
    #[schemars(description = "Patch script: one edit per line, e.g. \"2c NEW\" to change line 2")]
    pub patch: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DeleteFileInput {
    #[schemars(description = "File path relative to workspace root")]
    pub path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunCommandInput {
    #[schemars(description = "Command and arguments, e.g. [\"pytest\", \"-q\"]")]
    pub argv: Vec<String>,
    #[schemars(description = "Working directory relative to root (default: root)")]
    pub cwd: Option<String>,
    #[schemars(description = "Environment profile name (configured server-side)")]
    pub profile: Option<String>,
    #[schemars(description = "Timeout in milliseconds")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UndoInput {
    #[schemars(description = "Operation ID to undo (from write/patch/delete result)")]
    pub operation_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct HistoryInput {
    #[schemars(description = "Maximum number of operations to return")]
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

// ---- MCP server ----

pub struct RemoteWorkspaceServer {
    /// Argv used to (re)spawn the transport to the server.
    server_argv: Vec<String>,
    /// Current connection. A Client never recovers once its transport dies
    /// (e.g. sshd resetting the connection), so tool calls fetch it through
    /// `client()`, which reconnects on demand.
    client_slot: tokio::sync::Mutex<Option<Arc<Client>>>,
    #[allow(dead_code)]
    tool_router: ToolRouter<RemoteWorkspaceServer>,
}

const CONNECT_ATTEMPTS: u32 = 4;
const CONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

impl RemoteWorkspaceServer {
    pub fn new(server_argv: Vec<String>) -> Self {
        Self {
            server_argv,
            client_slot: tokio::sync::Mutex::new(None),
            tool_router: Self::tool_router(),
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
                argv: self.server_argv.clone(),
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
        Parameters(ListDirInput { path }): Parameters<ListDirInput>,
    ) -> CallToolResult {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match client.list(&path).await {
            Ok(entries) => {
                if entries.is_empty() {
                    return ok("(empty directory)");
                }
                let out = entries
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
                if r.truncated {
                    out.push_str("\n\n[output truncated; use offset/limit to read more]");
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
        description = "Run a command. Returns combined stdout/stderr and exit code. Output is capped; truncated=true if exceeded."
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
        let mut stdout = CappedString::new(OUTPUT_LIMIT);
        let mut stderr = CappedString::new(OUTPUT_LIMIT);
        let result = client
            .exec(
                argv,
                cwd,
                profile,
                timeout_ms,
                |ev: ExecEventKind| match ev {
                    ExecEventKind::Stdout { data } => stdout.push_str(&data),
                    ExecEventKind::Stderr { data } => stderr.push_str(&data),
                    ExecEventKind::Exit { .. } => {}
                },
            )
            .await;
        match result {
            Ok((exit_code, op)) => {
                let mut out = String::new();
                if !stdout.text.is_empty() {
                    out.push_str("[stdout]\n");
                    out.push_str(&stdout.text);
                }
                if !stderr.text.is_empty() {
                    if !out.is_empty() {
                        out.push('\n');
                    }
                    out.push_str("[stderr]\n");
                    out.push_str(&stderr.text);
                }
                if stdout.truncated() || stderr.truncated() {
                    out.push_str(&format!(
                        "\n[output truncated: dropped {} stdout bytes, {} stderr bytes]",
                        stdout.dropped, stderr.dropped
                    ));
                }
                out.push_str(&format!("\n[exit code: {exit_code}] (operation_id: {op})"));
                ok(out)
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

/// A String that stops growing after `cap` bytes, tracking how many bytes were
/// dropped. This prevents unbounded memory growth from chatty commands.
struct CappedString {
    text: String,
    cap: usize,
    dropped: usize,
}

impl CappedString {
    fn new(cap: usize) -> Self {
        Self {
            text: String::new(),
            cap,
            dropped: 0,
        }
    }

    fn push_str(&mut self, s: &str) {
        if self.text.len() >= self.cap {
            self.dropped += s.len();
            return;
        }
        let remaining = self.cap - self.text.len();
        if s.len() <= remaining {
            self.text.push_str(s);
        } else {
            // Back off to a char boundary so we never split a UTF-8 sequence.
            let mut take = remaining;
            while take > 0 && !s.is_char_boundary(take) {
                take -= 1;
            }
            self.text.push_str(&s[..take]);
            self.dropped += s.len() - take;
        }
    }

    fn truncated(&self) -> bool {
        self.dropped > 0
    }
}

#[tool_handler]
impl ServerHandler for RemoteWorkspaceServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.server_info.name = SERVER_NAME.into();
        info.server_info.version = SERVER_VERSION.into();
        info.instructions = Some(
            "Remote workspace tools for coding agents. All paths are relative to the workspace root."
                .into(),
        );
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info
    }
}

#[cfg(test)]
mod tests {
    use super::CappedString;

    #[test]
    fn cap_inside_multibyte_char_drops_whole_char() {
        let mut c = CappedString::new(1);
        c.push_str("é");
        assert_eq!(c.text, "");
        assert_eq!(c.dropped, 2);
        assert!(c.truncated());
    }

    #[test]
    fn cap_at_multibyte_char_end_keeps_it() {
        let mut c = CappedString::new(2);
        c.push_str("é");
        assert_eq!(c.text, "é");
        assert_eq!(c.dropped, 0);
        assert!(!c.truncated());
    }

    #[test]
    fn cap_splitting_mixed_input_backs_off_to_boundary() {
        let mut c = CappedString::new(2);
        c.push_str("aéx");
        assert_eq!(c.text, "a");
        assert_eq!(c.dropped, 3);
        assert!(c.truncated());
    }

    #[test]
    fn cap_after_multibyte_char_keeps_prefix() {
        let mut c = CappedString::new(3);
        c.push_str("aéx");
        assert_eq!(c.text, "aé");
        assert_eq!(c.dropped, 1);
    }

    #[test]
    fn pushes_after_full_only_count_dropped() {
        let mut c = CappedString::new(4);
        c.push_str("aaaa");
        c.push_str("éé");
        assert_eq!(c.text, "aaaa");
        assert_eq!(c.dropped, 4);
    }

    #[test]
    fn under_cap_appends_verbatim() {
        let mut c = CappedString::new(16);
        c.push_str("aé");
        c.push_str("x");
        assert_eq!(c.text, "aéx");
        assert_eq!(c.dropped, 0);
        assert!(!c.truncated());
    }
}
