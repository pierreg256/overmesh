use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use overmesh_gateway::resource::LogicalBlobId;
use overmesh_reconciler::{
    config::ReconcilerConfig,
    engine::{ReconcilerEngine, ReconcilerOptions, verify_reconciliation_record},
};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "overmesh-reconciler")]
#[command(about = "Overmesh consistency validation, repair, and quarantine engine")]
struct Cli {
    #[arg(long)]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Once {
        #[arg(long)]
        full_scan: bool,
    },
    Run,
    Recover {
        #[arg(long)]
        blob: String,
        #[arg(long)]
        source_replica: String,
    },
    VerifyRecord {
        path: PathBuf,
    },
    AuditRbac,
    ValidateRuntime,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let runtime = ReconcilerConfig::load(&cli.config)?.build()?;
    let topology_report = runtime.topology_validator.validate().await?;
    info!(
        storage_regions = topology_report.accounts.len(),
        "validated Storage Account Ring topology"
    );
    if let Command::VerifyRecord { path } = &cli.command {
        let payload = verify_reconciliation_record(
            &std::fs::read(path)?,
            runtime.signer.as_ref(),
            runtime.ring.document.ring_version,
        )?;
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }
    let interval = runtime.interval;
    let physical_collection_delay = runtime.physical_collection_delay;
    let history_compaction_max_versions_per_cycle =
        runtime.history_compaction_max_versions_per_cycle;
    let head_discovery_batch_size = runtime.head_discovery_batch_size;
    let staged_block_gc_max_records_per_cycle = runtime.staged_block_gc_max_records_per_cycle;
    let engine = ReconcilerEngine::new(
        runtime.ring.clone(),
        runtime.backends,
        runtime.signer,
        runtime.token_provider,
        runtime.posture_auditor,
        ReconcilerOptions {
            physical_collection_delay,
            history_compaction_max_versions_per_cycle,
            head_discovery_batch_size,
            staged_block_gc_max_records_per_cycle,
        },
    );
    match cli.command {
        Command::Once { full_scan } => {
            let report = if full_scan {
                engine.run_full_audit_cycle().await?
            } else {
                engine.run_cycle().await?
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Run => loop {
            let report = engine.run_cycle().await?;
            info!(
                blobs = report.blobs.len(),
                ring_version = report.ring_version,
                "reconciliation cycle completed"
            );
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = tokio::signal::ctrl_c() => break,
            }
        },
        Command::Recover {
            blob,
            source_replica,
        } => {
            let logical_blob = LogicalBlobId::parse_canonical(&blob)
                .context("recover --blob must be a canonical logical blob path")?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &engine.recover(&logical_blob, &source_replica).await?
                )?
            );
        }
        Command::AuditRbac => {
            println!(
                "{}",
                serde_json::to_string_pretty(&engine.audit_rbac_posture().await?)?
            );
        }
        Command::ValidateRuntime => {
            let posture = engine.audit_rbac_posture().await?;
            let signing_key_id = engine.validate_signing_provider().await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "apiVersion": "reconciler.overmesh.io/runtime-validation/v1",
                    "rbacPosture": posture,
                    "signingKeyId": signing_key_id
                }))?
            );
        }
        Command::VerifyRecord { .. } => unreachable!("handled before engine construction"),
    }
    Ok(())
}
