use std::path::PathBuf;

use agent_remote_mcp::RemoteWorkspaceServer;
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use rmcp::ServiceExt;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "agent-remote-mcp",
    version,
    about = "MCP server exposing named agent-remote workspaces"
)]
struct Cli {
    /// Path to the fleet config (TOML) declaring the workspaces. Defaults to
    /// ~/.agent-remote/workspaces.toml.
    #[arg(long)]
    fleet: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    let fleet_path = match cli.fleet {
        Some(p) => p,
        None => {
            let home =
                std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set; pass --fleet"))?;
            PathBuf::from(home).join(".agent-remote/workspaces.toml")
        }
    };
    let text = std::fs::read_to_string(&fleet_path)
        .with_context(|| format!("read fleet config {fleet_path:?}; create it or pass --fleet"))?;
    let fleet = agent_remote_mcp::parse_fleet(&text)
        .with_context(|| format!("invalid fleet config {fleet_path:?}"))?;

    // No eager connect: initialize must answer immediately (a blocking retry
    // loop here makes the MCP host time the server out, e.g. when a session
    // being resumed briefly overlaps its predecessor on the same state lock).
    // The first tool call to each workspace connects on demand.
    let server = RemoteWorkspaceServer::new(fleet);
    let service = server
        .serve(rmcp::transport::stdio())
        .await
        .context("failed to start MCP server")?;
    service.waiting().await?;
    Ok(())
}
