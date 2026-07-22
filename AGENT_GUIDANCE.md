# Agent guidance

Use the file tools for workspace reads and mutations. Use `run_command` for builds, tests, searches, and process management.

File reads and directory listings are bounded. Follow `next_offset` or the returned offset notice to retrieve another page instead of requesting unbounded output.

Normal relative paths address the workspace. Paths beginning with `@scratch/` address the workspace's server-managed scratch area. Commands receive its physical path in `AGENT_REMOTE_SCRATCH`. Use scratch for logs and other runtime artifacts; server operation logs, request state, and undo blobs are private and are never exposed there.

`run_command` is synchronous. Its result contains the termination reason, duration, and a bounded preview of each output stream: the first 4 KiB and last 12 KiB. Prefer targeted commands and filters. When full output is needed, redirect it to `$AGENT_REMOTE_SCRATCH`, then inspect the corresponding `@scratch/...` file incrementally with `read_file` using `offset` and `limit`.

Use `upload_file` and `download_file` to move large or binary files between the local machine and the remote workspace. They stream raw bytes; the file content never enters the model context. Keep using `read_file` for inspecting text. Never move binary data through `write_file`, base64, shell quoting, or paginated `read_file`. Each transfer handles exactly one regular file, the destination's parent directory must already exist, and an existing destination is only replaced with `overwrite=true`. Transfers are synchronous: a long-running call means bytes are still flowing, not that the tool is stuck. After a disconnected transfer, `stat` the destination before deciding whether to retry; the destination only exists once a transfer fully succeeded. To import a file from a URL, `run_command` a downloader (e.g. `curl -o`) on the remote side instead of transferring through the local machine.

For work that must outlive one call, use a remote-native supervisor such as tmux and write its logs to scratch. A successful launcher command confirms only that the launcher exited successfully; it does not confirm that the detached workload completed successfully.

Do not automatically retry `run_command` after an uncertain transport failure because the command may already have produced side effects.

File tools are confined to the workspace and scratch roots. Command execution is not a sandbox and can access anything permitted to the remote server user.
