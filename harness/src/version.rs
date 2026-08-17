use std::{fs, path::Path, process::Command};

use anyhow::{Context, Result, bail};
use semver::Version;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
    workspace_members: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    id: String,
    name: String,
    version: Version,
}

#[derive(Debug, Deserialize)]
struct Roadmap {
    project_version: Version,
    active_generation: String,
    milestones: Vec<Milestone>,
}

#[derive(Debug, Deserialize)]
struct Milestone {
    version: Version,
    generation: String,
    status: MilestoneStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum MilestoneStatus {
    Active,
    Planned,
    Completed,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionReport {
    pub project_version: Version,
    pub active_generation: String,
    pub workspace_packages: Vec<PackageVersion>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackageVersion {
    pub name: String,
    pub version: Version,
}

pub fn check(repository_root: &Path) -> Result<VersionReport> {
    let version_text =
        fs::read_to_string(repository_root.join("VERSION")).context("failed to read VERSION")?;
    let project_version =
        Version::parse(version_text.trim()).context("VERSION is not valid Semantic Versioning")?;

    let roadmap_text = fs::read_to_string(repository_root.join("roadmap.toml"))
        .context("failed to read roadmap.toml")?;
    let roadmap: Roadmap = toml::from_str(&roadmap_text).context("failed to parse roadmap.toml")?;
    if roadmap.project_version != project_version {
        bail!(
            "roadmap project version {} does not match VERSION {}",
            roadmap.project_version,
            project_version
        );
    }

    validate_milestone_state(&roadmap, &project_version)?;

    let output = Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(repository_root)
        .output()
        .context("failed to execute cargo metadata")?;
    if !output.status.success() {
        bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let metadata: CargoMetadata =
        serde_json::from_slice(&output.stdout).context("failed to parse cargo metadata")?;
    let mut workspace_packages = metadata
        .packages
        .into_iter()
        .filter(|package| metadata.workspace_members.contains(&package.id))
        .map(|package| PackageVersion {
            name: package.name,
            version: package.version,
        })
        .collect::<Vec<_>>();
    workspace_packages.sort_by(|left, right| left.name.cmp(&right.name));

    for package in &workspace_packages {
        if package.version != project_version {
            bail!(
                "workspace package {} has version {}, expected {}",
                package.name,
                package.version,
                project_version
            );
        }
    }

    Ok(VersionReport {
        project_version,
        active_generation: roadmap.active_generation,
        workspace_packages,
    })
}

fn validate_milestone_state(roadmap: &Roadmap, project_version: &Version) -> Result<()> {
    let active_milestones = roadmap
        .milestones
        .iter()
        .filter(|milestone| milestone.status == MilestoneStatus::Active)
        .collect::<Vec<_>>();
    match active_milestones.as_slice() {
        [active] => {
            if active.version != *project_version || active.generation != roadmap.active_generation
            {
                bail!(
                    "active roadmap milestone {} {} does not match project {} {}",
                    active.generation,
                    active.version,
                    roadmap.active_generation,
                    project_version
                );
            }
        }
        [] => {
            let current = roadmap
                .milestones
                .iter()
                .filter(|milestone| milestone.version == *project_version)
                .collect::<Vec<_>>();
            if current.len() != 1
                || current[0].generation != roadmap.active_generation
                || current[0].status != MilestoneStatus::Completed
            {
                bail!(
                    "roadmap without an active milestone must mark project {} {} completed",
                    roadmap.active_generation,
                    project_version
                );
            }
        }
        _ => {
            bail!(
                "roadmap must contain at most one active milestone, found {}",
                active_milestones.len()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use semver::Version;

    use super::*;

    #[test]
    fn project_version_is_semantic() {
        Version::parse(env!("CARGO_PKG_VERSION")).expect("Cargo package version is semantic");
    }

    #[test]
    fn accepts_a_completed_project_between_active_milestones() {
        let roadmap = roadmap_with_statuses([
            ("0.8.0", MilestoneStatus::Completed),
            ("0.9.0", MilestoneStatus::Planned),
        ]);
        validate_milestone_state(&roadmap, &Version::new(0, 8, 0))
            .expect("completed transition state");
    }

    #[test]
    fn rejects_multiple_active_milestones() {
        let roadmap = roadmap_with_statuses([
            ("0.8.0", MilestoneStatus::Active),
            ("0.9.0", MilestoneStatus::Active),
        ]);
        assert!(validate_milestone_state(&roadmap, &Version::new(0, 8, 0)).is_err());
    }

    #[test]
    fn rejects_a_planned_project_without_an_active_milestone() {
        let roadmap = roadmap_with_statuses([
            ("0.8.0", MilestoneStatus::Planned),
            ("0.9.0", MilestoneStatus::Planned),
        ]);
        assert!(validate_milestone_state(&roadmap, &Version::new(0, 8, 0)).is_err());
    }

    fn roadmap_with_statuses<const N: usize>(values: [(&str, MilestoneStatus); N]) -> Roadmap {
        Roadmap {
            project_version: Version::new(0, 8, 0),
            active_generation: "V1".to_owned(),
            milestones: values
                .into_iter()
                .map(|(version, status)| Milestone {
                    version: Version::parse(version).expect("version"),
                    generation: "V1".to_owned(),
                    status,
                })
                .collect(),
        }
    }
}
