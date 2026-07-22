use std::path::PathBuf;

use agent_remote_client::{ArgvTransport, Client, ClientLog};
use agent_remote_protocol::{ExecEventKind, ListKind};
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
    Ls { path: String },
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

    let server_argv: Vec<String> = if cli.local {
        let mut argv = vec![cli.remote_bin.clone(), "--root".into(), cli.root.clone()];
        if let Some(cfg) = &cli.config {
            argv.push("--config".into());
            argv.push(cfg.clone());
        }
        if let Some(b) = &cli.state_base {
            argv.push("--state-base".into());
            argv.push(b.clone());
        }
        argv
    } else {
        let host = cli.host.as_ref().ok_or_else(|| {
            anyhow!("--host is required (or use --local to run the server locally)")
        })?;
        agent_remote_client::ssh_server_argv(
            host,
            &cli.remote_bin,
            &cli.root,
            cli.config.as_deref(),
            cli.state_base.as_deref(),
        )
    };

    let transport = ArgvTransport { argv: server_argv };
    let client = Client::connect(transport, log)
        .await
        .context("connect to server")?;

    match cli.command {
        Command::Ls { path } => {
            let entries = client.list(&path).await?;
            for e in entries {
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
            if r.truncated {
                eprintln!("\n[truncated]");
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
            let (code, op) = client
                .exec(argv, cwd, profile, timeout_ms, |ev| match ev {
                    ExecEventKind::Stdout { data } => {
                        use std::io::Write;
                        let _ = std::io::stdout().write_all(data.as_bytes());
                    }
                    ExecEventKind::Stderr { data } => {
                        use std::io::Write;
                        let _ = std::io::stderr().write_all(data.as_bytes());
                    }
                    ExecEventKind::Exit { .. } => {}
                })
                .await?;
            eprintln!("[exit {code}] operation_id={op}");
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
