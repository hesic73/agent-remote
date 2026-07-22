use std::path::PathBuf;

use agent_remote_server::{Server, ServerOptions};
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "agent-remote-server",
    version,
    about = "Remote workspace endpoint for agent-remote"
)]
struct Args {
    /// Workspace root that all paths are resolved relative to.
    #[arg(long, required = true)]
    root: PathBuf,

    /// Directory for the operation log and blobs. Defaults to
    /// `~/.agent-remote/state/<name>-<hash>` keyed by the canonical root path,
    /// so the workspace itself stays untouched.
    #[arg(long)]
    log_dir: Option<PathBuf>,

    /// Base directory for state instead of `~/.agent-remote` (state still goes
    /// to `<base>/state/<name>-<hash>` per workspace). Useful when home is
    /// nearly full. Mutually exclusive with --log-dir.
    #[arg(long, conflicts_with = "log_dir")]
    state_base: Option<PathBuf>,

    /// Path to a TOML config file with profile setup scripts.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Keep only this many recent operations (older ones and their blobs are
    /// pruned at startup and on gc). 0 disables pruning.
    #[arg(long, default_value_t = 1000)]
    history_limit: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();

    let log_dir = match args.log_dir {
        Some(d) => d,
        None => {
            let base = match args.state_base {
                Some(b) => b,
                None => {
                    let home = std::env::var_os("HOME").ok_or_else(|| {
                        anyhow::anyhow!("HOME is not set; pass --state-base or --log-dir")
                    })?;
                    PathBuf::from(home).join(".agent-remote")
                }
            };
            agent_remote_server::state_dir_under(&base, &args.root)?
        }
    };

    let opts = ServerOptions {
        root: args.root,
        log_dir,
        config_path: args.config,
        history_limit: (args.history_limit > 0).then_some(args.history_limit),
    };

    Server::new(opts)?.run_stdio().await?;
    Ok(())
}
