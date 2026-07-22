# agent-remote

A lightweight remote-workspace protocol for coding agents. Code and the
execution environment stay on a remote server; the agent's machine runs only a
client, which reaches the remote side over an existing SSH channel and invokes
a small set of atomic operations.

> One-line definition: a persistent SSH connection plus a small set of
> reliable remote filesystem and command primitives for coding agents.

## Motivation

Coding agents are practical to run on only a few machines, while code and
execution environments are spread across servers, workstations, and
containers. Installing a full agent everywhere does not scale, especially for
short-lived containers.

So: decouple the agent's intelligence from the execution environment. The
agent runs on the client side and plans changes; the remote side runs a
lightweight endpoint exposing file reads, file mutations, command execution,
and status queries. The client never clones or syncs the workspace, and one
agent can reach many heterogeneous environments through one interface.

## Architecture

```text
coding agent
    |
    v
agent-remote (CLI)  or  agent-remote-mcp (MCP server)
    |
    v
agent-remote client library
    |
    | persistent SSH stdio connection
    v
agent-remote-server  --  fs ops, exec, operation log, workspace root
```

The client starts the remote process itself:

```bash
ssh <host> agent-remote-server --root /path/to/project
```

SSH stdin/stdout is the transport: no public IP, no extra port, no daemon. If
`ssh <host>` works, the connection works, inheriting `~/.ssh/config`,
ProxyJump, ControlMaster, Tailscale, and the rest.

The protocol is JSON Lines: one message per line, with a `request_id` tying
together a request, its streamed events, and its final result.

## Session semantics

Persistent connection, stateless execution. The SSH connection, server
process, and workspace root persist; every `exec` spawns a fresh child
process, so `conda activate` in one command does not leak into the next.
Environment setup (conda, ROS, ...) is re-applied per command via server-side
profiles:

```toml
[profiles.robot]
setup = """
source /mnt/data/miniconda3/etc/profile.d/conda.sh
conda activate robot
source /opt/ros/humble/setup.bash
"""
```

There is no server-side `cd`; every request carries explicit paths or `cwd`.
Interactive sessions (PTY, REPL, persistent shell) are out of scope.

## Operations

```text
list  stat  read  write  patch  delete  exec
undo  history  operation.get  request.status  gc
```

### Reads and hashes

`read` returns content plus a hash over the file's raw bytes:

```json
{"request_id":"r1","op":"read","path":"src/main.py","offset":0,"limit":65536}
{"request_id":"r1","type":"read","content":"...","hash":"sha256:abc","truncated":false}
```

### Mutations: optimistic concurrency, all-or-nothing

`write`/`patch` accept a `base_hash` (`patch` requires it). The server checks
the current hash first and rejects with `STALE_FILE` (carrying
`expected_hash`/`actual_hash`) if the file changed under you. Mutations build
the complete new content, then atomically rename into place -- a failed patch
leaves the file byte-for-byte unchanged. Success returns an `operation_id`
plus `old_hash`/`new_hash`.

The patch format is a small line-based edit script (change/delete/insert by
1-based line number), not unified diff; conflicting or out-of-range edits are
rejected as a whole.

### Exec

```json
{"request_id":"r3","op":"exec","argv":["pytest","-q"],"cwd":".","profile":"robot","timeout_ms":300000}
{"request_id":"r3","type":"stdout","data":"collecting...\n"}
{"request_id":"r3","type":"exit","exit_code":0,"operation_id":"op-43"}
```

Output streams as events; the exit event carries the exit code. `exec`
promises no transactionality and no undo -- it can do anything the remote
user can.

## Undo

Applies only to recorded file mutations (`write`, `patch`, `delete`). Each
mutation stores `before_hash`, `after_hash`, and a `before` blob. Undo runs
only if `current_hash == after_hash`, otherwise returns `UNDO_CONFLICT`
instead of clobbering later changes. Undoing a file creation removes the
file. Single-file only; no multi-file transactions.

## Server state and logging

All server state lives **outside the workspace**, on the remote host, keyed by
the canonical root path:

```text
~/.agent-remote/state/<rootname>-<hash>/
|-- operations.jsonl   one record per operation (fs + exec)
|-- requests.jsonl     request idempotency table
|-- blobs/             before-content and exec stdout/stderr
|-- lock               single-writer flock
`-- op-counter         id high-water mark (prevents reuse after pruning)
```

The workspace stays untouched -- nothing for `git status`, nothing a
destructive command inside the workspace can destroy along with itself.
`--state-base` swaps the base directory while keeping per-root keying (for
hosts where home is nearly full). State is per-workspace, not per-session:
sessions are just connections, and cross-session features (undo, history,
replay after reconnect) are exactly the reason the state must outlive them.

* **Server log = execution truth.** Every operation is recorded with hashes,
  argv, exit codes, and blob references. Appends are fsync'd. Mutations are
  write-ahead: `prepared` before the rename, `committed` after, so a crash in
  between is reconciled on restart instead of leaving a phantom operation.
* **Client log = interaction truth.** Optional JSONL log of every request
  sent and every response/event received (including truncation flags), i.e.
  what the agent actually saw.
* **Bounded growth.** At startup the server prunes to the newest
  `--history-limit` operations (default 1000; 0 disables), dropping older
  records, their blobs, and request entries no longer referenced. The `gc`
  operation does the same on demand. Undo of a pruned operation returns
  `OPERATION_NOT_FOUND`; pruned ids are never reallocated.
* **Single writer.** The state directory is protected by an exclusive flock
  held for the server's lifetime (auto-released by the kernel on death). A
  second server on the same root fails fast with a clear error; reconnects
  get a short grace period while the predecessor shuts down.

## Idempotency and reconnect

Every request has a globally unique `request_id`. The server persists results
in `requests.jsonl` and reloads them on restart. After a dropped connection
the client can either query `request.status` or resend the same
`request_id` -- the server returns the stored result without re-executing.
`exec` is never auto-retried, since re-running a command may not be safe.

The replay window equals the retention window: request entries older than the
newest `--history-limit` operations are pruned along with them. Reconnect
recovery happens minutes after a drop, far inside any reasonable window.

## Workspace boundary

All file paths resolve inside `--root`; `..`, absolute paths, and symlinks
escaping the root are rejected (including a non-existent leaf under a
symlinked parent). This guards against accidents, not adversaries -- `exec`
can still reach anything the remote user can. Real isolation belongs to
containers or user permissions.

## MCP integration

`agent-remote-mcp` wraps the client library in an MCP stdio server, so any
MCP-capable agent gets the workspace as tools: `list_dir`, `stat`,
`read_file`, `write_file`, `patch_file`, `delete_file`, `run_command`,
`undo`, `history`, `operation_get`, `request_status`.

* Protocol errors map to MCP `isError` results, so failures are visible to
  the agent.
* `run_command` output is capped at 16 MiB per stream (UTF-8-safe
  truncation, dropped bytes reported) so chatty commands cannot grow the
  response unbounded.
* In SSH mode the remote command line is shell-quoted per argument, because
  `ssh` re-parses its trailing arguments through the remote shell.

## Technology

Rust workspace; both ends ship as single near-static binaries (no runtime to
install remotely). Tokio for stdio/process/timeout concurrency; serde for the
protocol; the system `ssh` binary as transport (no SSH library). The protocol
crate has no I/O dependencies, so other transports can be added without
touching operation semantics.

Deliberately absent: databases (JSONL + blobs suffice), custom daemons,
embedded shells (commands run from explicit `argv`; only profile setup goes
through a shell), and RPC frameworks.

```text
crates/
  agent-remote-protocol/  # pure serde types: messages, errors, records
  agent-remote-server/    # workspace boundary, fs ops, exec, operation store
  agent-remote-client/    # transport, typed API, client log, CLI
  agent-remote-mcp/       # MCP stdio server on top of the client
```

Tests live inside each crate: protocol round-trips, in-process server tests,
end-to-end tests that spawn the real server binary over stdio, and MCP tests
that drive the real `agent-remote-mcp` binary.

## MVP status

All criteria are implemented and tested:

* [x] Persistent SSH stdio session from client to server
* [x] `list`, `stat`, `read`, `write`, `patch`, `delete`, `exec`
* [x] All-or-nothing single-file write/patch
* [x] `read` returns hash; `write`/`patch` accept `base_hash`
* [x] `exec` streams stdout/stderr and returns the exit code
* [x] Operation IDs and fsync'd JSONL log with crash recovery
* [x] Client interaction log
* [x] `history` and `operation.get`
* [x] Safe single-file undo, including undo of creation
* [x] Request status queryable and replayable by `request_id` after reconnect
* [x] State outside the workspace, single-writer locked, with bounded growth
  and on-demand `gc`

Non-goals: workspace sync/clone, resident daemons, undo of `exec`,
multi-file transactions, multi-agent merging, job scheduling, interactive
PTYs.
