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

    // In local mode each arg is a separate argv element (no shell). In SSH
    // mode ssh_server_argv shell-quotes everything into one remote command.
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

    let server = RemoteWorkspaceServer::new(server_argv);
    // Surface connection problems (auth, wrong paths) in the log immediately,
    // but keep serving either way: tool calls reconnect on demand.
    if let Err(e) = server.warm_up().await {
        tracing::warn!("initial connection failed: {e}");
    }
    let service = server
        .serve(rmcp::transport::stdio())
        .await
        .context("failed to start MCP server")?;
    service.waiting().await?;
    Ok(())
}
