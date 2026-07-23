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

The protocol is JSON Lines: one request and one terminal response per line,
correlated by `request_id`.

## Session semantics

Persistent connection, stateless execution. The SSH connection, server
process, and workspace root persist; every `exec` spawns a fresh child
process, so `conda activate` in one command does not leak into the next.
Environment setup (conda, ROS, ...) is re-applied per command via server-side
profiles:

```toml
default_profile = "user-zsh"

[profiles.user-zsh]
shell = ["zsh", "-lic"]
setup = ""

[profiles.robot]
setup = """
source /mnt/data/miniconda3/etc/profile.d/conda.sh
conda activate robot
source /opt/ros/humble/setup.bash
"""
```

A profile owns two things and nothing more: which shell to start (`shell`,
default `["bash", "-c"]`; the server appends `setup` + `exec <argv>` as the
final argument) and what to run before the command (`setup`). Choosing
`["zsh", "-lic"]` reuses the user's own login/interactive configuration
instead of teaching the server about conda or ROS -- the server never
understands toolchains, it only picks a shell and execs through it. Without
any profile (explicit or `default_profile`), the argv is spawned directly
with no shell at all. Config parsing is strict (`deny_unknown_fields`, empty
shells and undeclared defaults rejected at startup): an older server reading
a newer config must fail loudly, never silently run commands in the wrong
environment.

There is no server-side `cd`; every request carries explicit paths or `cwd`.
Interactive sessions (PTY, REPL, persistent shell) are out of scope.

## Operations

```text
list  stat  read  write  patch  delete  exec
undo  history  operation.get  request.status  gc
upload_prepare  upload_commit  upload_abort  download_record   (transfer control plane)
```

### Reads and hashes

`read` returns content plus a hash over the file's raw bytes:

```json
{"request_id":"r1","op":"read","path":"src/main.py","offset":0,"limit":65536}
{"request_id":"r1","type":"read","content":"...","hash":"sha256:abc","truncated":true,"next_offset":65536}
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
{"request_id":"r3","type":"exec","operation_id":"op-43","termination":{"kind":"exited","code":0},"duration_ms":842,"stdout":{"prefix":"...","suffix":"","total_bytes":3,"omitted_bytes":0},"stderr":{"prefix":"","suffix":"","total_bytes":0,"omitted_bytes":0}}
```

The result is synchronous and bounded: each stream retains its first 4 KiB and
last 12 KiB. `exec` promises no transactionality and no undo -- it can do
anything the remote user can.

### File transfer

`upload_file`/`download_file` (exposed as MCP tools and client-library
functions) move single regular files without pushing content through the JSONL
protocol. The control plane stays on the resident connection; the data plane is
a separate short-lived process per transfer, spawned over the same SSH
configuration:

```text
agent-remote-server --transfer-receive <staging> --expect-size N   # stdin -> staging file
agent-remote-server --transfer-send <path> --root R [--state-base B]  # header JSON, raw bytes, trailer JSON -> stdout
```

These raw modes never open the operation store, so they cannot conflict with
the resident server's state lock. `--transfer-send` re-validates the path with
the same workspace/`@scratch` boundary rules as every other operation.

Uploads are three-phase on the control plane: `upload_prepare` validates the
target (parent must exist; existing targets refused unless `overwrite`) and
creates a random `.part` staging file in the target's directory;
`upload_commit` verifies the staged size, installs atomically (rename for
overwrite, hard-link-then-unlink for race-free no-replace), fsyncs, and appends
the operation record; `upload_abort` deletes the staging file after a failure.
The staging path travels only between the resident server and the client; it is
never persisted or shown to the agent. Downloads verify size and SHA-256
against the sender's framing, install locally via temp file + (no-clobber)
rename, then append a `download_record`.

Both directions stream through fixed 64 KiB buffers with SHA-256 computed on
each side; memory does not grow with file size. Operation records are
metadata-only (direction, remote logical path, size, hash, duration) -- no
local paths, no content. Transfers are synchronous, cannot be undone, and have
no resume/job machinery: a dropped connection fails the call, the destination
is never left half-written, and the caller just retries.

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
|-- blobs/             before-content for undo
|-- scratch/           agent-visible runtime artifacts (`@scratch/...`)
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

Operational conventions for agents live only in
[`AGENT_GUIDANCE.md`](AGENT_GUIDANCE.md); the MCP server embeds that file in
`ServerInfo.instructions`. This section documents protocol behavior rather
than duplicating those instructions.

`agent-remote-mcp` wraps the client library in an MCP stdio server that
multiplexes a fleet of named workspaces, declared in a single TOML file
(`~/.agent-remote/workspaces.toml` by convention). A workspace is a `(machine,
root)` pair; "two roots on one machine" and "one root each on two machines"
are the same concept, because all server-side state is already keyed per
root. The agent sees tools: `list_workspaces`, `list_dir`, `stat`,
`read_file`, `write_file`, `patch_file`, `delete_file`, `run_command`,
`upload_file`, `download_file`, `undo`, `history`, `operation_get`,
`request_status` -- each (except `list_workspaces`) with a required
`workspace` argument. Making it required, with no default, is deliberate: a
call can never land on the wrong machine because a default silently filled
in. Results echo the workspace name, since operation and request IDs are
only unique within one workspace.

The MCP process keeps one independent, lazily-opened connection per
workspace, so a dead machine costs only its own calls, and the fleet needs
no server-side coordination at all -- there is no cross-workspace operation
(file movement between workspaces goes through a local file via
`download_file` + `upload_file`).

* Protocol errors map to MCP `isError` results, so failures are visible to
  the agent.
* `run_command` returns one synchronous terminal result. The server drains both
  pipes but retains only the first 4 KiB and last 12 KiB of each stream, with
  total and omitted byte counts. No streaming output path exists.
* `read_file` returns at most 64 KiB per call; directory listings return at
  most 1,000 entries with `next_offset`. History defaults to 50 records,
  rejects limits above 100, and omits exec preview text.
* In SSH mode the remote command line is shell-quoted per argument, because
  `ssh` re-parses its trailing arguments through the remote shell.
* Connections are rebuilt on demand: a dead link is replaced on the next
  tool call (retries with backoff, probed with a real round-trip), while a
  call that dies mid-flight surfaces as an error and is never auto-retried.
  `initialize` never blocks on connecting, and the transport child carries
  PDEATHSIG so a killed MCP cannot orphan its ssh (which would keep the
  remote server -- and the state lock -- alive).

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
* [x] `exec` returns bounded stdout/stderr previews and termination details
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
