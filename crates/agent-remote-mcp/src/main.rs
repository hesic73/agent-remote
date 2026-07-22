use agent_remote_mcp::RemoteWorkspaceServer;
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use rmcp::ServiceExt;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "agent-remote-mcp",
    version,
    about = "MCP server exposing remote workspace tools over agent-remote"
)]
struct Cli {
    /// SSH host (resolvable via ~/.ssh/config). Required unless --local.
    #[arg(long)]
    host: Option<String>,

    /// Path to the remote `agent-remote-server` binary.
    #[arg(long, default_value = "agent-remote-server")]
    remote_bin: String,

    /// Workspace root on the remote host.
    #[arg(long)]
    root: String,

    /// Run the server locally (no SSH). --remote-bin must be a local path.
    #[arg(long)]
    local: bool,

    /// Optional remote config TOML path.
    #[arg(long)]
    config: Option<String>,

    /// Optional base directory for server state instead of ~/.agent-remote on
    /// the remote (state still keyed per workspace under <base>/state/).
    #[arg(long)]
    state_base: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    let endpoint = if cli.local {
        agent_remote_client::Endpoint::Local {
            server_bin: cli.remote_bin,
            root: cli.root,
            state_base: cli.state_base,
            config: cli.config,
        }
    } else {
        let host = cli
            .host
            .ok_or_else(|| anyhow!("--host is required (or use --local)"))?;
        agent_remote_client::Endpoint::Ssh {
            host,
            remote_bin: cli.remote_bin,
            root: cli.root,
            state_base: cli.state_base,
            config: cli.config,
        }
    };

    // No eager connect: initialize must answer immediately (a blocking retry
    // loop here makes the MCP host time the server out, e.g. when a session
    // being resumed briefly overlaps its predecessor on the same state lock).
    // The first tool call connects on demand.
    let server = RemoteWorkspaceServer::new(endpoint);
    let service = server
        .serve(rmcp::transport::stdio())
        .await
        .context("failed to start MCP server")?;
    service.waiting().await?;
    Ok(())
}
