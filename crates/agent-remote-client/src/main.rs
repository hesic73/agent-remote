use std::path::PathBuf;

use agent_remote_client::{ArgvTransport, Client, ClientLog};
use agent_remote_protocol::{ExecOutput, ExecTermination, ListKind};
use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "agent-remote",
    version,
    about = "Client for agent-remote remote workspaces"
)]
struct Cli {
    /// SSH host (as resolvable via ~/.ssh/config). Required unless --local is
    /// given.
    #[arg(long)]
    host: Option<String>,

    /// Path to the remote `agent-remote-server` binary. Defaults to expecting
    /// it on the remote PATH.
    #[arg(long, default_value = "agent-remote-server")]
    remote_bin: String,

    /// Workspace root on the remote host.
    #[arg(long)]
    root: String,

    /// Optional remote config TOML path passed to the server.
    #[arg(long)]
    config: Option<String>,

    /// Optional base directory for server state instead of ~/.agent-remote on
    /// the remote (state still keyed per workspace under <base>/state/).
    #[arg(long)]
    state_base: Option<String>,

    /// Run the server locally as a subprocess instead of over SSH. The
    /// --remote-bin must be an executable path available locally.
    #[arg(long)]
    local: bool,

    /// Path to a client interaction log file (JSONL).
    #[arg(long)]
    log: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List a directory.
    Ls {
        path: String,
        #[arg(long)]
        offset: Option<usize>,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Stat a file or directory.
    Stat { path: String },
    /// Read a file.
    Cat {
        path: String,
        #[arg(long)]
        offset: Option<u64>,
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Write content (from --file or stdin) to a path.
    Write {
        path: String,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long)]
        base_hash: Option<String>,
    },
    /// Apply a patch script. Patch text from --file or stdin.
    Patch {
        path: String,
        #[arg(long)]
        base_hash: String,
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Execute a command remotely.
    Exec {
        /// Working directory relative to root.
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long)]
        profile: Option<String>,
        #[arg(long)]
        timeout_ms: Option<u64>,
        /// Command argv (first element is the program).
        argv: Vec<String>,
    },
    /// Delete a file.
    Rm { path: String },
    /// Undo a recorded file operation.
    Undo { operation_id: String },
    /// Show operation history.
    History {
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Show details of one operation.
    Op { operation_id: String },
    /// Query the status of a previously-issued request.
    Status { request_id: String },
    /// Prune stored history down to the most recent operations.
    Gc {
        /// How many operations to keep. Defaults to the server's
        /// --history-limit.
        #[arg(long)]
        keep: Option<usize>,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async_main_real())
}

async fn async_main_real() -> Result<()> {
    let cli = Cli::parse();
    let log = match &cli.log {
        Some(p) => Some(
            ClientLog::open(p.clone())
                .await
                .context("open client log")?,
        ),
        None => None,
    };

    let endpoint = if cli.local {
        agent_remote_client::Endpoint::Local {
            server_bin: cli.remote_bin.clone(),
            root: cli.root.clone(),
            state_base: cli.state_base.clone(),
            config: cli.config.clone(),
        }
    } else {
        let host = cli.host.clone().ok_or_else(|| {
            anyhow!("--host is required (or use --local to run the server locally)")
        })?;
        agent_remote_client::Endpoint::Ssh {
            host,
            remote_bin: cli.remote_bin.clone(),
            root: cli.root.clone(),
            state_base: cli.state_base.clone(),
            config: cli.config.clone(),
        }
    };

    let transport = ArgvTransport {
        argv: endpoint.control_argv(),
    };
    let client = Client::connect(transport, log)
        .await
        .context("connect to server")?;

    match cli.command {
        Command::Ls {
            path,
            offset,
            limit,
        } => {
            let result = client.list(&path, offset, limit).await?;
            for e in result.entries {
                let kind = match e.kind {
                    ListKind::File => 'f',
                    ListKind::Dir => 'd',
                    ListKind::Symlink => 'l',
                };
                match e.size {
                    Some(s) => println!("{kind} {:>10} {}", s, e.name),
                    None => println!("{kind} {:>10} {}", '-', e.name),
                }
            }
            if let Some(next) = result.next_offset {
                eprintln!("[more entries: use --offset {next}]");
            }
        }
        Command::Stat { path } => {
            let s = client.stat(&path).await?;
            println!("{}", serde_json::to_string_pretty(&s)?);
        }
        Command::Cat {
            path,
            offset,
            limit,
        } => {
            let r = client.read(&path, offset, limit).await?;
            print!("{}", r.content);
            if let Some(next) = r.next_offset {
                eprintln!("\n[truncated: use --offset {next}]");
            }
        }
        Command::Write {
            path,
            file,
            base_hash,
        } => {
            let content = read_input(file)?;
            let res = client.write(&path, &content, base_hash.as_deref()).await?;
            println!("{}", serde_json::to_string_pretty(&res)?);
        }
        Command::Patch {
            path,
            base_hash,
            file,
        } => {
            let patch = read_input(file)?;
            let res = client.patch(&path, &base_hash, &patch).await?;
            println!("{}", serde_json::to_string_pretty(&res)?);
        }
        Command::Exec {
            cwd,
            profile,
            timeout_ms,
            argv,
        } => {
            if argv.is_empty() {
                return Err(anyhow!("exec requires at least one argv element"));
            }
            let result = client.exec(argv, cwd, profile, timeout_ms).await?;
            print_exec_output(&result.stdout, false);
            print_exec_output(&result.stderr, true);
            eprintln!(
                "[{:?}] operation_id={} duration_ms={}",
                result.termination, result.operation_id, result.duration_ms
            );
            let code = match result.termination {
                ExecTermination::Exited { code } => code,
                ExecTermination::TimedOut => 124,
                ExecTermination::Signaled { signal } => 128 + signal,
            };
            std::process::exit(code);
        }
        Command::Rm { path } => {
            let res = client.delete(&path).await?;
            println!("{}", serde_json::to_string_pretty(&res)?);
        }
        Command::Undo { operation_id } => {
            let res = client.undo(&operation_id).await?;
            println!("{}", serde_json::to_string_pretty(&res)?);
        }
        Command::History { limit } => {
            let ops = client.history(limit).await?;
            println!("{}", serde_json::to_string_pretty(&ops)?);
        }
        Command::Op { operation_id } => {
            let d = client.operation_get(&operation_id).await?;
            println!("{}", serde_json::to_string_pretty(&d)?);
        }
        Command::Status { request_id } => {
            let r = client.request_status(&request_id).await?;
            println!("{}", serde_json::to_string_pretty(&r)?);
        }
        Command::Gc { keep } => {
            let r = client.gc(keep).await?;
            println!("{}", serde_json::to_string_pretty(&r)?);
        }
    }
    Ok(())
}

fn print_exec_output(output: &ExecOutput, stderr: bool) {
    use std::io::Write;

    let mut text = output.prefix.clone();
    if output.omitted_bytes > 0 {
        text.push_str(&format!("\n[{} bytes omitted]\n", output.omitted_bytes));
    }
    text.push_str(&output.suffix);
    if stderr {
        let _ = std::io::stderr().write_all(text.as_bytes());
    } else {
        let _ = std::io::stdout().write_all(text.as_bytes());
    }
}

fn read_input(file: Option<PathBuf>) -> Result<String> {
    match file {
        Some(p) => Ok(std::fs::read_to_string(&p).with_context(|| format!("read {p:?}"))?),
        None => {
            use std::io::Read;
            let mut s = String::new();
            std::io::stdin().read_to_string(&mut s)?;
            Ok(s)
        }
    }
}
