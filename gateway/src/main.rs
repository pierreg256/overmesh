use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use overmesh_gateway::{AppState, GatewayConfig, build_router};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "overmesh-gateway")]
#[command(about = "Microsoft Entra-native Azure Blob federation gateway")]
struct Cli {
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let config = GatewayConfig::load(&cli.config)?;
    let authenticator = config.load_authenticator()?;
    let signed_ring = config.load_ring()?;
    let commit_service = config.load_commit_service(&signed_ring)?;
    commit_service.validate_control_plane().await?;
    let read_service = commit_service.read_service();
    let state = AppState {
        authenticator,
        logical_account: config.logical_account.clone(),
        ring: std::sync::Arc::new(signed_ring.document),
        commit_service: Some(std::sync::Arc::new(commit_service)),
        read_service: Some(std::sync::Arc::new(read_service)),
    };
    let listener = tokio::net::TcpListener::bind(config.listen_address).await?;
    info!(
        address = %config.listen_address,
        version = env!("CARGO_PKG_VERSION"),
        ring_version = state.ring.ring_version,
        "Overmesh gateway started"
    );

    axum::serve(listener, build_router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
