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

    /// Diagnostic mode: validate the fleet config, probe every workspace once
    /// (spawn its server, one real round-trip), report per-workspace status,
    /// and exit nonzero if any workspace is unhealthy.
    #[arg(long)]
    check: bool,
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

    if cli.check {
        println!(
            "fleet config {} ok: {} workspace(s)",
            fleet_path.display(),
            fleet.len()
        );
        let mut unhealthy = 0;
        for (name, ws) in &fleet {
            let location = match &ws.endpoint {
                agent_remote_client::Endpoint::Ssh { host, root, .. } => format!("{host}:{root}"),
                agent_remote_client::Endpoint::Local { root, .. } => format!("(local):{root}"),
            };
            match agent_remote_mcp::check_workspace(&ws.endpoint).await {
                Ok(()) => println!("{name} [{location}]: ok"),
                Err(e) => {
                    unhealthy += 1;
                    println!("{name} [{location}]: {e}");
                }
            }
        }
        if unhealthy > 0 {
            anyhow::bail!("{unhealthy} workspace(s) unhealthy");
        }
        return Ok(());
    }

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
