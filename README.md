# agent-remote

A lightweight remote-workspace protocol for coding agents. The agent runs
locally; code and the execution environment live on a remote host running
`agent-remote-server`. The client talks to it over plain SSH stdio -- no
daemon, port, or public IP.

```
coding agent  ->  agent-remote (CLI) or agent-remote-mcp (MCP)  ->  ssh stdio  ->  agent-remote-server  ->  workspace
```

The transport is JSON Lines over the SSH process's stdin/stdout, so
`~/.ssh/config`, ProxyJump, Tailscale, SSH agent, and ControlMaster all work
unchanged. See `DESIGN.md` for the rationale and protocol details.

## Build

```bash
cargo build --release
# produces:
#   target/release/agent-remote        (client + CLI)
#   target/release/agent-remote-server (server)
#   target/release/agent-remote-mcp    (MCP server for coding agents)
```

Copy `agent-remote-server` onto the remote host (anywhere on `PATH`, or pass
its full path with `--remote-bin`).

If the remote's glibc is older than your build machine's, build the server as
a fully static musl binary instead — it runs anywhere on the same
architecture:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl -p agent-remote-server
# -> target/x86_64-unknown-linux-musl/release/agent-remote-server
```

## Quick start

Local mode (`--local`) runs the server as a subprocess -- handy for a single
host or for trying things out:

```bash
WS=$(mktemp -d)
SRV=$(pwd)/target/release/agent-remote-server

agent-remote --local --remote-bin "$SRV" --root "$WS" write hello.txt <<< "hi there"
agent-remote --local --remote-bin "$SRV" --root "$WS" cat hello.txt
agent-remote --local --remote-bin "$SRV" --root "$WS" exec -- make test
```

Over SSH:

```bash
scp target/release/agent-remote-server robot@workstation:~/.local/bin/

agent-remote --host robot@workstation --root /home/robot/project ls .
agent-remote --host robot@workstation --root /home/robot/project exec -- pytest -q
```

Environment profiles (conda, ROS, ...) live in a TOML file on the remote side
and are re-applied for every `exec`, so commands stay stateless:

```toml
# on the remote host; point the server at it with --config
[profiles.robot]
setup = """
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate robot
source /opt/ros/humble/setup.bash
"""
```

## CLI subcommands

| Command | Purpose |
|---------|---------|
| `ls <path> [--offset N] [--limit N]` | List a directory |
| `stat <path>` | Stat a file or directory |
| `cat <path> [--offset N] [--limit N]` | Read a file |
| `write <path> [--file F] [--base-hash H]` | Write content (file or stdin) |
| `patch <path> --base-hash H [--file F]` | Apply a patch (see below) |
| `rm <path>` | Delete a file |
| `exec [--cwd DIR] [--profile P] [--timeout-ms N] -- argv...` | Run a command |
| `undo <operation_id>` | Undo a recorded file change |
| `history [--limit N]` | List recorded operations |
| `op <operation_id>` | Details of one operation |
| `status <request_id>` | Status of a previously-issued request |
| `gc [--keep N]` | Prune stored history down to the newest N operations |

Shared flags: `--host`, `--remote-bin` (default `agent-remote-server`),
`--root`, `--config`, `--local`, `--log <file>` (client interaction log),
`--state-base` (relocate server state, see below).

## Server state

The server keeps its history, undo blobs, and idempotency table **outside the
workspace**, on the remote host:

```
~/.agent-remote/state/<rootname>-<hash>/   # keyed by canonical root path
  operations.jsonl  requests.jsonl  blobs/  lock  op-counter
```

The workspace itself is never touched (no dotdir, nothing for `git status`),
and a destructive command inside the workspace cannot take the undo data with
it. To relocate state -- e.g. when home is nearly full -- pass
`--state-base /data/$USER`: the base changes, the automatic per-workspace
keying stays.

Growth is bounded: at startup the server keeps only the newest
`--history-limit` operations (default 1000; 0 disables) and drops older
records, their blobs, and stale request entries. `gc --keep N` does the same
on demand; deleting the state directory itself is always safe. Undoing a
pruned operation returns `OPERATION_NOT_FOUND`, and pruned operation ids are
never reused.

One server per workspace root: the state directory holds an exclusive lock,
so a second concurrent session fails fast instead of corrupting the logs
(reconnects get a short grace period while the predecessor exits).

### Patch format

A small line-based edit script (not unified diff), one edit per line, with
1-based line numbers referring to the original content:

| Syntax | Meaning |
|--------|---------|
| `<n>c <text>` | Change line `n` to `<text>` |
| `<n>d` | Delete line `n` |
| `<n>a <text>` | Insert `<text>` after line `n` (`0a` inserts at the top) |
| `# ...` / blank | Ignored |

Conflicting or out-of-range edits are rejected and the file is left untouched.

## MCP server (use from a coding agent)

Agent-facing conventions have a single canonical source in
[`AGENT_GUIDANCE.md`](AGENT_GUIDANCE.md). The MCP server embeds that file
verbatim in its initialization instructions.

`agent-remote-mcp` exposes the same operations as MCP tools over stdio. It
serves a **fleet** of named workspaces declared in
`~/.agent-remote/workspaces.toml` (override with `--fleet <path>`); each
workspace is a `(machine, root)` pair, on any mix of SSH hosts and the local
machine:

```toml
# ~/.agent-remote/workspaces.toml
[workspaces.robot]
host = "robot@workstation"        # omit host to run on the local machine
root = "/home/robot/project"
bin  = "/home/robot/.local/bin/agent-remote-server"  # optional, default: on PATH
# config / state_base optional, same meaning as the server flags

[workspaces.lab-gpu]
host = "lab-gpu-1"
root = "/data/experiments"
```

```bash
# Claude Code example: one MCP entry serves every workspace
claude mcp add agent-remote -- agent-remote-mcp
```

Tools: `list_workspaces`, `list_dir`, `stat`, `read_file`, `write_file`,
`patch_file`, `delete_file`, `run_command`, `upload_file`, `download_file`,
`undo`, `history`, `operation_get`, `request_status`. Every tool except
`list_workspaces` takes a required `workspace` argument naming which
workspace to act on. Workspaces are fully isolated: state, operation IDs,
history, and undo are scoped per workspace (server-side, keyed by root), and
connections are opened per workspace on demand -- an unreachable machine
fails only its own calls. Two entries must not address the same host and
root (they would contend for the same server state lock; the config is
rejected at startup).

`upload_file` / `download_file` move single regular files between the local
machine and the remote workspace (or `@scratch/...`). They are synchronous and
stream raw bytes over a dedicated SSH process with a fixed-size buffer -- file
content never passes through the JSONL protocol or the model context, and
memory use does not grow with file size. Both ends compute SHA-256 and the
transfer only commits (atomically, on the remote via rename/link, locally via
temp-file rename) when size and hash match. Existing destinations are refused
unless `overwrite=true`; parent directories are never created implicitly. The
result and the operation log carry metadata only: direction, remote path,
size, `sha256:...`, duration. Transfers cannot be undone, and there is no
resume -- a failed or disconnected transfer leaves the destination absent (or
untouched), and you simply call the tool again.

Tool failures come back as MCP `isError` results. `run_command` is synchronous
and returns a server-bounded preview for each stream: the first 4 KiB and last
12 KiB, together with byte counts and termination details. Full logs belong in
the server-managed `@scratch/...` namespace and can be paged with `read_file`.
Reads are limited to 64 KiB per call. Directory listings return at most 1,000
entries with `next_offset`. History defaults to 50 records, has a hard maximum
of 100, and omits stored exec preview text.

The connection is resilient: if the SSH link dies (network blip, sshd
resetting the connection), the next tool call reconnects automatically with
retries and a liveness probe. A call that fails mid-flight is reported as an
error, never silently re-executed. The spawned `ssh` uses BatchMode (no tty
prompts) and keepalives, and dies with the MCP process, so a killed session
cannot leave an orphaned connection holding the remote state lock.

## Guarantees

- **File boundary.** Normal paths resolve inside `--root`; `@scratch/...`
  resolves inside a separate server-managed scratch root. `..`, absolute
  paths, and symlinks escaping either root are rejected. Guards against
  accidents, not adversaries -- `exec` can still touch anything the user can.
- **Atomic writes.** `write`/`patch` build the full result, then atomically
  rename into place, preserving file mode. A failed patch changes nothing.
- **Optimistic concurrency.** Mutations accept `base_hash` and are rejected
  with `STALE_FILE` if the file changed; `read` returns a hash usable
  directly as the next `base_hash`.
- **Durable, recoverable log.** Every operation is recorded in fsync'd JSONL
  with before/after hashes or bounded exec output previews. Mutations are
  write-ahead (`prepared`/`committed`), and startup recovery reconciles a
  crash between rename and commit.
- **Safe undo.** Only applies while the file is still in the recorded
  `after` state; otherwise `UNDO_CONFLICT`. Undoing a creation removes the
  file.
- **Idempotency.** Request results persist across restarts; replaying a
  `request_id` returns the stored result without re-executing, and
  `status <request_id>` answers "did that ever run?" after a reconnect.
  The replay window equals the retention window: entries older than the
  newest `--history-limit` operations are pruned.
- **Non-invasive state.** All server state lives outside the workspace,
  keyed by root path, with a single-writer lock and bounded growth (see
  "Server state" above).

## Layout and testing

```
crates/
  agent-remote-protocol/  # pure serde types: messages, errors, records
  agent-remote-server/    # workspace, fs ops, exec, operation store (binary)
  agent-remote-client/    # transport, typed API, CLI (binary `agent-remote`)
  agent-remote-mcp/       # MCP stdio server on top of the client (binary)
```

```bash
cargo test --workspace --all-targets   # includes end-to-end tests against
                                       # the real server and MCP binaries
cargo clippy --workspace --all-targets -- -D warnings
```
