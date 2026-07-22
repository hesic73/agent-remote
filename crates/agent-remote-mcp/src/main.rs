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

    let server_argv: Vec<String> = if cli.local {
        agent_remote_client::local_server_argv(
            &cli.remote_bin,
            &cli.root,
            cli.config.as_deref(),
            cli.state_base.as_deref(),
        )
    } else {
        let host = cli
            .host
            .as_ref()
            .ok_or_else(|| anyhow!("--host is required (or use --local)"))?;
        agent_remote_client::ssh_server_argv(
            host,
            &cli.remote_bin,
            &cli.root,
            cli.config.as_deref(),
            cli.state_base.as_deref(),
        )
    };

    // No eager connect: initialize must answer immediately (a blocking retry
    // loop here makes the MCP host time the server out, e.g. when a session
    // being resumed briefly overlaps its predecessor on the same state lock).
    // The first tool call connects on demand.
    let server = RemoteWorkspaceServer::new(server_argv);
    let service = server
        .serve(rmcp::transport::stdio())
        .await
        .context("failed to start MCP server")?;
    service.waiting().await?;
    Ok(())
}
