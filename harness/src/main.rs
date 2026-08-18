use std::{
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use overmesh_gateway::{RingDocument, resource::LogicalBlobId};
use overmesh_harness::{
    RunOptions,
    dataset::generate,
    doc_check,
    environment::local_service_statuses,
    identity::{TestPrincipal, TestTokenKind, issue_test_token},
    manifest_validation::{
        verify_local_commit_manifest, verify_local_garbage_collection_marker,
        verify_local_history_compaction_checkpoint,
    },
    run_scenario,
    runner::ensure_passed,
    scenario::Scenario,
    system_validation::{SystemValidationConfig, validate_system},
    toxiproxy, version,
};
use walkdir::WalkDir;

#[derive(Debug, Parser)]
#[command(name = "overmesh-harness")]
#[command(about = "Executable validation harness for Overmesh V1")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    List {
        #[arg(long, default_value = "harness/scenarios")]
        directory: PathBuf,
    },
    Validate {
        scenario: PathBuf,
    },
    Run {
        scenario: PathBuf,
        #[arg(long)]
        no_report: bool,
    },
    RunAll {
        #[arg(long, default_value = "harness/scenarios")]
        directory: PathBuf,
        #[arg(long)]
        no_report: bool,
    },
    Doctor,
    Fault {
        #[command(subcommand)]
        command: FaultCommand,
    },
    Version,
    VersionCheck,
    DocCheck {
        #[arg(long)]
        json: bool,
    },
    GenerateDataset {
        output: PathBuf,
        #[arg(long)]
        size: u64,
        #[arg(long)]
        seed: u64,
    },
    IssueToken {
        #[arg(value_enum, default_value_t = TokenKindArgument::Valid)]
        kind: TokenKindArgument,
        #[arg(long, value_enum, default_value_t = PrincipalArgument::Caller)]
        principal: PrincipalArgument,
    },
    VerifyCommitManifest {
        path: PathBuf,
        #[arg(long)]
        block_manifest: Option<PathBuf>,
    },
    VerifyGarbageCollectionMarker {
        path: PathBuf,
    },
    VerifyHistoryCompactionCheckpoint {
        path: PathBuf,
    },
    ValidateSystem {
        #[arg(long, default_value = "http://127.0.0.1:18080")]
        gateway_url: String,
        #[arg(long, default_value = "https://127.0.0.1:12100/devstoreaccount1")]
        backend_a_url: String,
        #[arg(long, default_value = "https://127.0.0.1:12101/devstoreaccount1")]
        backend_b_url: String,
        #[arg(long, default_value = "local-overmesh")]
        logical_account: String,
    },
    FindPlacement {
        #[arg(long, default_value = "harness/rings/ring-v1-three-node.yaml")]
        ring: PathBuf,
        #[arg(long, default_value = "local-overmesh")]
        logical_account: String,
        #[arg(long, default_value = "placement")]
        container: String,
        first_node: String,
        second_node: String,
    },
}

#[derive(Debug, Subcommand)]
enum FaultCommand {
    Disable {
        #[arg(value_enum)]
        replica: ReplicaArgument,
    },
    Enable {
        #[arg(value_enum)]
        replica: ReplicaArgument,
    },
    Latency {
        #[arg(value_enum)]
        replica: ReplicaArgument,
        #[arg(long)]
        milliseconds: u64,
        #[arg(long, default_value_t = 0)]
        jitter: u64,
    },
    Reset,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ReplicaArgument {
    A,
    B,
    C,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TokenKindArgument {
    Valid,
    WrongAudience,
    WrongTenant,
    Expired,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PrincipalArgument {
    Caller,
    Gateway,
    Reconciler,
    Denied,
}

impl From<TokenKindArgument> for TestTokenKind {
    fn from(value: TokenKindArgument) -> Self {
        match value {
            TokenKindArgument::Valid => Self::Valid,
            TokenKindArgument::WrongAudience => Self::WrongAudience,
            TokenKindArgument::WrongTenant => Self::WrongTenant,
            TokenKindArgument::Expired => Self::Expired,
        }
    }
}

impl From<PrincipalArgument> for TestPrincipal {
    fn from(value: PrincipalArgument) -> Self {
        match value {
            PrincipalArgument::Caller => Self::Caller,
            PrincipalArgument::Gateway => Self::Gateway,
            PrincipalArgument::Reconciler => Self::Reconciler,
            PrincipalArgument::Denied => Self::Denied,
        }
    }
}

impl From<ReplicaArgument> for toxiproxy::ProxyReplica {
    fn from(value: ReplicaArgument) -> Self {
        match value {
            ReplicaArgument::A => Self::A,
            ReplicaArgument::B => Self::B,
            ReplicaArgument::C => Self::C,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match execute().await {
        Ok(exit_code) => exit_code,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn execute() -> Result<ExitCode> {
    let cli = Cli::parse();
    let repository_root = env::current_dir().context("failed to determine current directory")?;

    match cli.command {
        Command::List { directory } => {
            for path in scenario_paths(&directory)? {
                let scenario = Scenario::load(&path)?;
                println!("{}\t{}\t{}", scenario.id, scenario.suite, path.display());
            }
        }
        Command::Validate { scenario } => {
            let loaded = Scenario::load(&scenario)?;
            println!("{} is valid", loaded.id);
        }
        Command::Run {
            scenario,
            no_report,
        } => {
            run_one(&repository_root, &scenario, no_report)?;
        }
        Command::RunAll {
            directory,
            no_report,
        } => {
            let paths = scenario_paths(&directory)?;
            if paths.is_empty() {
                bail!("no scenario files found in {}", directory.display());
            }
            for path in paths {
                run_one(&repository_root, &path, no_report)?;
            }
        }
        Command::Doctor => {
            let statuses = local_service_statuses()?;
            for status in &statuses {
                println!(
                    "{}\t{}\t{}",
                    status.name,
                    status.address,
                    if status.reachable {
                        "reachable"
                    } else {
                        "unreachable"
                    }
                );
            }
            if statuses.iter().any(|status| !status.reachable) {
                bail!("one or more local harness services are unreachable");
            }
        }
        Command::Fault { command } => match command {
            FaultCommand::Disable { replica } => {
                toxiproxy::set_enabled(replica.into(), false)?;
                println!("replica\t{replica:?}\tdisabled");
            }
            FaultCommand::Enable { replica } => {
                toxiproxy::set_enabled(replica.into(), true)?;
                println!("replica\t{replica:?}\tenabled");
            }
            FaultCommand::Latency {
                replica,
                milliseconds,
                jitter,
            } => {
                toxiproxy::add_latency(replica.into(), milliseconds, jitter)?;
                println!("replica\t{replica:?}\tlatency\t{milliseconds}ms\tjitter\t{jitter}ms");
            }
            FaultCommand::Reset => {
                toxiproxy::reset()?;
                println!("faults\treset");
            }
        },
        Command::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"));
        }
        Command::VersionCheck => {
            let report = version::check(&repository_root)?;
            println!(
                "project\t{}\t{}",
                report.project_version, report.active_generation
            );
            for package in report.workspace_packages {
                println!("module\t{}\t{}", package.name, package.version);
            }
        }
        Command::DocCheck { json } => {
            let report = doc_check::check(&repository_root)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report.violations)?);
            } else if !report.passed() {
                eprint!("{}", report.text());
            }
            if !report.passed() {
                return Ok(ExitCode::FAILURE);
            }
        }
        Command::GenerateDataset { output, size, seed } => {
            if let Some(parent) = output.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create dataset directory {}", parent.display())
                })?;
            }
            let hash = generate(&output, size, seed)?;
            println!("{}\t{}\t{}", output.display(), size, hash);
        }
        Command::IssueToken { kind, principal } => {
            println!("{}", issue_test_token(kind.into(), principal.into())?);
        }
        Command::VerifyCommitManifest {
            path,
            block_manifest,
        } => {
            let manifest = verify_local_commit_manifest(&path, block_manifest.as_deref())?;
            println!(
                "{}\t{}\t{}\t{:?}\t{}\t{}\t{}",
                manifest.blob,
                manifest.write_id,
                manifest.logical_version,
                manifest.state,
                manifest.block_manifest_object,
                manifest.content_container,
                manifest.content_object
            );
        }
        Command::VerifyGarbageCollectionMarker { path } => {
            let marker = verify_local_garbage_collection_marker(&path)?;
            println!(
                "{}\t{}\t{}\t{}",
                marker.blob,
                marker.collected_through_logical_version,
                marker.history_head_logical_version,
                marker.collected_committed_versions.len()
            );
        }
        Command::VerifyHistoryCompactionCheckpoint { path } => {
            let checkpoint = verify_local_history_compaction_checkpoint(&path)?;
            println!(
                "{}\t{}\t{}\t{}",
                checkpoint.blob,
                checkpoint.checkpoint_version,
                checkpoint.compacted_through_logical_version,
                checkpoint.garbage_collection_through_logical_version
            );
        }
        Command::ValidateSystem {
            gateway_url,
            backend_a_url,
            backend_b_url,
            logical_account,
        } => {
            validate_system(&SystemValidationConfig {
                gateway_url,
                backend_a_url,
                backend_b_url,
                logical_account,
            })
            .await?;
        }
        Command::FindPlacement {
            ring,
            logical_account,
            container,
            first_node,
            second_node,
        } => {
            let document: RingDocument = serde_yaml::from_slice(
                &fs::read(&ring)
                    .with_context(|| format!("failed to read Ring {}", ring.display()))?,
            )
            .with_context(|| format!("failed to parse Ring {}", ring.display()))?;
            let mut expected = [first_node, second_node];
            expected.sort();
            if expected[0] == expected[1]
                || expected
                    .iter()
                    .any(|id| !document.nodes.iter().any(|node| node.id == *id))
            {
                bail!("placement nodes must be distinct members of the Ring");
            }
            let mut found = None;
            for index in 0..100_000_u32 {
                let path = format!("/{container}/placement-{index:05}");
                let logical_blob = LogicalBlobId::parse(&logical_account, &path)?;
                let mut selected = document
                    .replicas_for(&logical_blob)?
                    .into_iter()
                    .map(|node| node.id.as_str())
                    .collect::<Vec<_>>();
                selected.sort_unstable();
                if selected == expected {
                    found = Some(path);
                    break;
                }
            }
            println!(
                "{}",
                found.context("failed to find a logical blob for the requested replica pair")?
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn run_one(repository_root: &Path, scenario: &Path, no_report: bool) -> Result<()> {
    let mut options = RunOptions::for_repository(repository_root.to_path_buf());
    options.write_report = !no_report;
    let run = run_scenario(scenario, &options)?;
    println!(
        "{}\t{}\t{:?}\t{:?}",
        run.report.scenario_id,
        if run.report.passed { "PASS" } else { "FAIL" },
        run.report.health_after_operations,
        run.report.health_after_reconciliation
    );
    if let Some(path) = &run.report_path {
        println!("report\t{}", path.display());
    }
    ensure_passed(&run)
}

fn scenario_paths(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = WalkDir::new(directory)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.into_path())
        .filter(|path| {
            matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("yaml" | "yml")
            )
        })
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}
