use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

fn mcp_bin() -> String {
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../target/debug/agent-remote-mcp");
    p.to_string_lossy().into_owned()
}

fn server_bin() -> String {
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../target/debug/agent-remote-server");
    p.to_string_lossy().into_owned()
}

struct McpSession {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    id: u64,
}

impl McpSession {
    fn spawn(root: &str) -> Self {
        let srv = server_bin();
        assert!(
            std::path::Path::new(&srv).exists(),
            "server binary not found at {srv}"
        );
        let mcp = mcp_bin();
        assert!(
            std::path::Path::new(&mcp).exists(),
            "mcp binary not found at {mcp}"
        );
        // Keep server state inside the test tempdir instead of the real HOME.
        let state = format!("{root}/.agent-remote-test");
        let mut child = Command::new(&mcp)
            .args(["--local", "--remote-bin", &srv, "--root", root])
            .args(["--state-base", &state])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn mcp");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        Self {
            child,
            stdin,
            stdout,
            id: 0,
        }
    }

    fn call(&mut self, method: &str, params: serde_json::Value) -> serde_json::Value {
        self.id += 1;
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": self.id,
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&req).unwrap();
        self.stdin.write_all(line.as_bytes()).unwrap();
        self.stdin.write_all(b"\n").unwrap();
        self.stdin.flush().unwrap();
        self.read_response()
    }

    fn notify(&mut self, method: &str, params: serde_json::Value) {
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&req).unwrap();
        self.stdin.write_all(line.as_bytes()).unwrap();
        self.stdin.write_all(b"\n").unwrap();
        self.stdin.flush().unwrap();
    }

    fn read_response(&mut self) -> serde_json::Value {
        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("read response");
        // Skip notifications (no "id" field).
        let v: serde_json::Value = serde_json::from_str(line.trim()).expect("parse response");
        if v.get("id").is_none() {
            return self.read_response();
        }
        v
    }
}

impl Drop for McpSession {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn mcp_initialize_and_server_info() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());

    // initialize
    let resp = s.call(
        "initialize",
        serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1"},
        }),
    );
    assert_eq!(resp["result"]["serverInfo"]["name"], "agent-remote-mcp");
    // Send initialized notification.
    s.notify("notifications/initialized", serde_json::json!({}));
}

#[test]
fn mcp_tools_list_has_expected_tools() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());

    s.call(
        "initialize",
        serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1"},
        }),
    );
    s.notify("notifications/initialized", serde_json::json!({}));

    let resp = s.call("tools/list", serde_json::json!({}));
    let tools = resp["result"]["tools"].as_array().expect("tools array");
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    for expected in [
        "list_dir",
        "read_file",
        "stat",
        "write_file",
        "patch_file",
        "delete_file",
        "run_command",
        "undo",
        "history",
        "operation_get",
        "request_status",
    ] {
        assert!(
            names.contains(&expected),
            "missing tool {expected}; have {names:?}"
        );
    }
}

#[test]
fn mcp_tool_call_success_is_not_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());

    s.call(
        "initialize",
        serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1"},
        }),
    );
    s.notify("notifications/initialized", serde_json::json!({}));

    // write_file should succeed.
    let resp = s.call(
        "tools/call",
        serde_json::json!({
            "name": "write_file",
            "arguments": {"path": "test.txt", "content": "hello\n"},
        }),
    );
    assert_eq!(
        resp["result"]["isError"], false,
        "write should not be an error"
    );
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("Wrote test.txt"), "unexpected text: {text}");
}

#[test]
fn mcp_tool_call_failure_is_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());

    s.call(
        "initialize",
        serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1"},
        }),
    );
    s.notify("notifications/initialized", serde_json::json!({}));

    // read_file on a non-existent file must return isError=true.
    let resp = s.call(
        "tools/call",
        serde_json::json!({
            "name": "read_file",
            "arguments": {"path": "missing.txt"},
        }),
    );
    assert_eq!(
        resp["result"]["isError"], true,
        "reading a missing file must be isError=true"
    );
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("NotFound"),
        "error text should mention the error: {text}"
    );
}

#[test]
fn mcp_run_command_returns_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());

    s.call(
        "initialize",
        serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1"},
        }),
    );
    s.notify("notifications/initialized", serde_json::json!({}));

    let resp = s.call(
        "tools/call",
        serde_json::json!({
            "name": "run_command",
            "arguments": {"argv": ["echo", "hello-from-mcp"]},
        }),
    );
    assert_eq!(resp["result"]["isError"], false);
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("hello-from-mcp"), "stdout missing: {text}");
    assert!(text.contains("exit code: 0"), "exit code missing: {text}");
}

// Drive every remaining tool over real MCP stdio: stat, patch_file,
// delete_file, undo, history, operation_get, request_status.
#[test]
fn mcp_full_tool_surface() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = McpSession::spawn(dir.path().to_str().unwrap());
    s.call(
        "initialize",
        serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1"},
        }),
    );
    s.notify("notifications/initialized", serde_json::json!({}));

    let tool = |s: &mut McpSession, name: &str, args: serde_json::Value| -> (bool, String) {
        let resp = s.call(
            "tools/call",
            serde_json::json!({"name": name, "arguments": args}),
        );
        let is_err = resp["result"]["isError"].as_bool().unwrap();
        let text = resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_string();
        (is_err, text)
    };

    let (e, _) = tool(
        &mut s,
        "write_file",
        serde_json::json!({"path": "f.txt", "content": "l1\nl2\n"}),
    );
    assert!(!e);

    // stat returns the hash we need for patching.
    let (e, text) = tool(&mut s, "stat", serde_json::json!({"path": "f.txt"}));
    assert!(!e, "stat failed: {text}");
    let stat: serde_json::Value = serde_json::from_str(&text).unwrap();
    let hash = stat["hash"].as_str().unwrap().to_string();

    let (e, text) = tool(
        &mut s,
        "patch_file",
        serde_json::json!({"path": "f.txt", "base_hash": hash, "patch": "2c L2"}),
    );
    assert!(!e, "patch failed: {text}");
    let patch_op = text
        .split("operation_id=")
        .nth(1)
        .unwrap()
        .split(',')
        .next()
        .unwrap()
        .to_string();

    let (e, text) = tool(&mut s, "read_file", serde_json::json!({"path": "f.txt"}));
    assert!(!e);
    assert!(text.starts_with("l1\nL2\n"), "patch not applied: {text}");

    // Undo the patch.
    let (e, text) = tool(
        &mut s,
        "undo",
        serde_json::json!({"operation_id": patch_op}),
    );
    assert!(!e, "undo failed: {text}");
    let (_, text) = tool(&mut s, "read_file", serde_json::json!({"path": "f.txt"}));
    assert!(text.starts_with("l1\nl2\n"), "undo not applied: {text}");

    // Delete, then verify it is gone.
    let (e, text) = tool(&mut s, "delete_file", serde_json::json!({"path": "f.txt"}));
    assert!(!e, "delete failed: {text}");
    assert!(!dir.path().join("f.txt").exists());
    let (e, _) = tool(&mut s, "read_file", serde_json::json!({"path": "f.txt"}));
    assert!(e, "reading a deleted file must be an error");

    // History shows all four operations; operation_get resolves the patch op.
    let (e, text) = tool(&mut s, "history", serde_json::json!({}));
    assert!(!e);
    assert!(
        text.contains("\"delete\""),
        "history missing delete: {text}"
    );
    let (e, text) = tool(
        &mut s,
        "operation_get",
        serde_json::json!({"operation_id": patch_op}),
    );
    assert!(!e, "operation_get failed: {text}");
    assert!(text.contains(&patch_op));

    // request_status on an unknown id reports unknown, not an error.
    let (e, text) = tool(
        &mut s,
        "request_status",
        serde_json::json!({"request_id": "never-existed"}),
    );
    assert!(!e, "request_status failed: {text}");
    assert!(text.contains("unknown"), "unexpected status: {text}");
}
