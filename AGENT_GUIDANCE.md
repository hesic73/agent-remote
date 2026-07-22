# Agent guidance

Use the file tools for workspace reads and mutations. Use `run_command` for builds, tests, searches, and process management.

File reads and directory listings are bounded. Follow `next_offset` or the returned offset notice to retrieve another page instead of requesting unbounded output.

Normal relative paths address the workspace. Paths beginning with `@scratch/` address the workspace's server-managed scratch area. Commands receive its physical path in `AGENT_REMOTE_SCRATCH`. Use scratch for logs and other runtime artifacts; server operation logs, request state, and undo blobs are private and are never exposed there.

`run_command` is synchronous. Its result contains the termination reason, duration, and a bounded preview of each output stream: the first 4 KiB and last 12 KiB. Prefer targeted commands and filters. When full output is needed, redirect it to `$AGENT_REMOTE_SCRATCH`, then inspect the corresponding `@scratch/...` file incrementally with `read_file` using `offset` and `limit`.

For work that must outlive one call, use a remote-native supervisor such as tmux and write its logs to scratch. A successful launcher command confirms only that the launcher exited successfully; it does not confirm that the detached workload completed successfully.

Do not automatically retry `run_command` after an uncertain transport failure because the command may already have produced side effects.

File tools are confined to the workspace and scratch roots. Command execution is not a sandbox and can access anything permitted to the remote server user.
