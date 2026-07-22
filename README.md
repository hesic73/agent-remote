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
| `ls <path>` | List a directory |
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
`--state-base` / `--log-dir` (relocate server state, see below).

## Server state

The server keeps its history, undo blobs, and idempotency table **outside the
workspace**, on the remote host:

```
~/.agent-remote/state/<rootname>-<hash>/   # keyed by canonical root path
  operations.jsonl  requests.jsonl  blobs/  lock  op-counter
```

The workspace itself is never touched (no dotdir, nothing for `git status`),
and a destructive command inside the workspace cannot take the undo data with
it. Two ways to relocate state (mutually exclusive):

- `--state-base /data/$USER` -- different base, same automatic per-workspace
  keying (`<base>/state/<name>-<hash>`). Use when home is nearly full.
- `--log-dir <dir>` -- exact directory, no keying. Full manual control.

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

`agent-remote-mcp` exposes the same operations as MCP tools over stdio:

```bash
# Claude Code example
claude mcp add remote-ws -- \
  agent-remote-mcp --host robot@workstation --root /home/robot/project

# or against a local workspace (no SSH)
claude mcp add local-ws -- \
  agent-remote-mcp --local --remote-bin ./target/release/agent-remote-server \
  --root /path/to/project
```

Tools: `list_dir`, `stat`, `read_file`, `write_file`, `patch_file`,
`delete_file`, `run_command`, `undo`, `history`, `operation_get`,
`request_status`. Flags mirror the CLI.

Tool failures come back as MCP `isError` results. `run_command` output is
capped at 16 MiB per stream (UTF-8-safe truncation, dropped bytes reported),
so chatty commands cannot blow up the agent's context.

## Guarantees

- **Workspace boundary.** Paths resolve inside `--root`; `..`, absolute
  paths, and symlinks escaping the root are rejected. Guards against
  accidents, not adversaries -- `exec` can still touch anything the user can.
- **Atomic writes.** `write`/`patch` build the full result, then atomically
  rename into place, preserving file mode. A failed patch changes nothing.
- **Optimistic concurrency.** Mutations accept `base_hash` and are rejected
  with `STALE_FILE` if the file changed; `read` returns a hash usable
  directly as the next `base_hash`.
- **Durable, recoverable log.** Every operation is recorded in fsync'd JSONL
  with before/after hashes and stdout/stderr blobs. Mutations are
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
cargo test --workspace --all-targets   # 113 tests, incl. end-to-end against
                                       # the real server and MCP binaries
cargo clippy --workspace --all-targets -- -D warnings
```
