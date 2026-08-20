use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs,
    net::{Ipv4Addr, Ipv6Addr},
    path::{Component, Path},
    process::{Command, Stdio},
    str::FromStr,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

const TRACEABILITY_PATH: &str = "docs/traceability.toml";
const ADR_DIRECTORY: &str = "docs/adr";
const ADR_INDEX_PATH: &str = "docs/adr/README.md";
const RETAINED_ARTIFACTS_DIRECTORY: &str = "harness/artifacts";

#[derive(Debug, Deserialize)]
struct Traceability {
    schema_version: u32,
    #[serde(default)]
    document: Vec<PublishedDocument>,
    #[serde(default)]
    excluded: Vec<ExcludedDocument>,
    #[serde(default)]
    assertion: Vec<DocumentationAssertion>,
    #[serde(default)]
    evidence_exemption: Vec<EvidenceExemption>,
}

#[derive(Debug, Deserialize)]
struct PublishedDocument {
    path: String,
    section: String,
    title: String,
    weight: i64,
}

#[derive(Debug, Deserialize)]
struct ExcludedDocument {
    path: String,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct DocumentationAssertion {
    document: String,
    metric: String,
    pattern: String,
}

#[derive(Debug, Deserialize)]
struct EvidenceExemption {
    record: String,
    citation: String,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct Roadmap {
    milestones: Vec<RoadmapMilestone>,
}

#[derive(Debug, Deserialize)]
struct RoadmapMilestone {
    version: String,
    status: String,
}

#[derive(Debug)]
struct AdrMetadata {
    id: String,
    path: String,
    status: String,
    supersedes: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DocumentationViolation {
    pub rule: String,
    pub path: String,
    pub line: Option<usize>,
    pub message: String,
    pub hint: String,
}

#[derive(Debug, Default)]
pub struct DocumentationReport {
    pub violations: Vec<DocumentationViolation>,
}

impl DocumentationReport {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }

    #[must_use]
    pub fn text(&self) -> String {
        let mut output = String::new();
        for violation in &self.violations {
            let _ = write!(output, "{}", violation.path);
            if let Some(line) = violation.line {
                let _ = write!(output, ":{line}");
            }
            let _ = writeln!(output, ": {}", violation.message);
            for hint_line in violation.hint.lines() {
                let _ = writeln!(output, "  {hint_line}");
            }
            output.push('\n');
        }
        let count = self.violations.len();
        let suffix = if count == 1 {
            "violation"
        } else {
            "violations"
        };
        let _ = writeln!(output, "{count} documentation {suffix}");
        output
    }
}

pub fn check(repository_root: &Path) -> Result<DocumentationReport> {
    let traceability_text = fs::read_to_string(repository_root.join(TRACEABILITY_PATH))
        .with_context(|| format!("failed to read {TRACEABILITY_PATH}"))?;
    let traceability: Traceability = toml::from_str(&traceability_text)
        .with_context(|| format!("failed to parse {TRACEABILITY_PATH}"))?;

    let mut report = DocumentationReport::default();
    if traceability.schema_version != 1 {
        report.push(
            "R2",
            TRACEABILITY_PATH,
            None,
            format!("unsupported schema_version {}", traceability.schema_version),
            "set schema_version = 1",
        );
    }

    check_registry(repository_root, &traceability, &mut report)?;
    check_links(repository_root, &traceability, &mut report)?;

    let adr_metadata = check_adr_index_and_metadata(repository_root, &mut report)?;
    check_evidence(repository_root, &traceability, &adr_metadata, &mut report)?;
    check_assertions(repository_root, &traceability, &mut report)?;
    check_retained_artifact_redaction(repository_root, &mut report)?;
    check_retained_artifact_provenance(repository_root, &mut report)?;

    report.violations.sort_by(|left, right| {
        (
            left.path.as_str(),
            left.line,
            left.rule.as_str(),
            left.message.as_str(),
        )
            .cmp(&(
                right.path.as_str(),
                right.line,
                right.rule.as_str(),
                right.message.as_str(),
            ))
    });
    Ok(report)
}

impl DocumentationReport {
    fn push(
        &mut self,
        rule: &str,
        path: impl Into<String>,
        line: Option<usize>,
        message: impl Into<String>,
        hint: impl Into<String>,
    ) {
        self.violations.push(DocumentationViolation {
            rule: rule.to_owned(),
            path: path.into(),
            line,
            message: message.into(),
            hint: hint.into(),
        });
    }
}

fn check_registry(
    repository_root: &Path,
    traceability: &Traceability,
    report: &mut DocumentationReport,
) -> Result<()> {
    let mut registered = BTreeMap::<String, &'static str>::new();
    for document in &traceability.document {
        register_path(
            &document.path,
            "document",
            &mut registered,
            report,
            TRACEABILITY_PATH,
        );
        if document.section.trim().is_empty() || document.title.trim().is_empty() {
            report.push(
                "R2",
                TRACEABILITY_PATH,
                None,
                format!("document {:?} has an empty section or title", document.path),
                "set non-empty section and title values",
            );
        }
        let _ = document.weight;
    }
    for excluded in &traceability.excluded {
        register_path(
            &excluded.path,
            "excluded",
            &mut registered,
            report,
            TRACEABILITY_PATH,
        );
        if excluded.reason.trim().is_empty() {
            report.push(
                "R2",
                TRACEABILITY_PATH,
                None,
                format!("excluded document {:?} has no reason", excluded.path),
                "add a non-empty reason",
            );
        }
    }

    for path in authored_markdown_paths(repository_root)? {
        if !registered.contains_key(&path) {
            report.push(
                "R1",
                &path,
                None,
                "not registered for publication",
                "add a [[document]] entry to docs/traceability.toml, or an [[excluded]] entry with a reason",
            );
        }
    }

    for (path, kind) in registered {
        if !is_safe_repository_path(&path) {
            report.push(
                "R2",
                TRACEABILITY_PATH,
                None,
                format!("{kind} path {path:?} is not a repository-relative path"),
                "remove absolute paths and parent-directory components",
            );
            continue;
        }
        if !repository_root.join(&path).is_file() {
            report.push(
                "R2",
                TRACEABILITY_PATH,
                None,
                format!("{kind} {path:?} does not exist"),
                "remove the entry or correct the path",
            );
        }
    }
    Ok(())
}

fn register_path(
    path: &str,
    kind: &'static str,
    registered: &mut BTreeMap<String, &'static str>,
    report: &mut DocumentationReport,
    registry_path: &str,
) {
    if let Some(previous) = registered.insert(path.to_owned(), kind) {
        report.push(
            "R1",
            registry_path,
            None,
            format!("{path:?} is registered as both {previous} and {kind}"),
            "keep exactly one [[document]] or [[excluded]] entry for the path",
        );
    }
}

fn authored_markdown_paths(repository_root: &Path) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args([
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            "*.md",
        ])
        .current_dir(repository_root)
        .output()
        .context("failed to list authored Markdown files with git")?;
    if !output.status.success() {
        anyhow::bail!(
            "git ls-files failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let mut paths = String::from_utf8(output.stdout)
        .context("git returned non-UTF-8 Markdown paths")?
        .lines()
        .filter(|path| !ignored_documentation_path(path))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn ignored_documentation_path(path: &str) -> bool {
    let components = path.split('/').collect::<Vec<_>>();
    components
        .iter()
        .any(|component| matches!(*component, ".git" | ".harness" | "target" | "node_modules"))
        || matches!(
            components.as_slice(),
            ["site", "content", ..]
                | ["site", "public", ..]
                | [".overmesh", "exchange", _, "attachments", ..]
        )
}

fn check_links(
    repository_root: &Path,
    traceability: &Traceability,
    report: &mut DocumentationReport,
) -> Result<()> {
    for document in &traceability.document {
        let source_path = repository_root.join(&document.path);
        if !source_path.is_file() {
            continue;
        }
        let content = fs::read_to_string(&source_path)
            .with_context(|| format!("failed to read {}", document.path))?;
        for (line_index, line) in content.lines().enumerate() {
            for target in markdown_link_targets(line) {
                let Some(relative_target) = relative_link_path(&target) else {
                    continue;
                };
                let resolved = source_path
                    .parent()
                    .unwrap_or(repository_root)
                    .join(relative_target);
                if !resolved.exists() {
                    let expected = display_repository_target(repository_root, &resolved);
                    report.push(
                        "R3",
                        &document.path,
                        Some(line_index + 1),
                        format!("relative link {target:?} does not resolve"),
                        format!("expected a file at {expected}"),
                    );
                }
            }
        }
    }
    Ok(())
}

fn markdown_link_targets(line: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let mut cursor = 0;
    while let Some(relative_start) = line[cursor..].find("](") {
        let start = cursor + relative_start + 2;
        let Some(relative_end) = line[start..].find(')') else {
            break;
        };
        let end = start + relative_end;
        if let Some(target) = markdown_target(&line[start..end]) {
            targets.push(target);
        }
        cursor = end + 1;
    }
    let trimmed = line.trim_start();
    if trimmed.starts_with('[')
        && let Some(definition_end) = trimmed.find("]:")
        && let Some(target) = markdown_target(&trimmed[definition_end + 2..])
    {
        targets.push(target);
    }
    targets
}

fn markdown_target(value: &str) -> Option<String> {
    let value = value.trim();
    let target = if let Some(value) = value.strip_prefix('<') {
        value.split('>').next().unwrap_or_default()
    } else {
        value.split_whitespace().next().unwrap_or_default()
    };
    (!target.is_empty()).then(|| target.to_owned())
}

fn relative_link_path(target: &str) -> Option<&str> {
    let without_anchor = target.split('#').next().unwrap_or_default();
    let without_query = without_anchor.split('?').next().unwrap_or_default();
    if without_query.is_empty()
        || without_query.starts_with('/')
        || without_query.contains("://")
        || without_query.starts_with("mailto:")
    {
        None
    } else {
        Some(without_query)
    }
}

fn check_adr_index_and_metadata(
    repository_root: &Path,
    report: &mut DocumentationReport,
) -> Result<BTreeMap<String, AdrMetadata>> {
    let roadmap_text = fs::read_to_string(repository_root.join("roadmap.toml"))
        .context("failed to read roadmap.toml")?;
    let roadmap: Roadmap = toml::from_str(&roadmap_text).context("failed to parse roadmap.toml")?;
    let roadmap_versions = roadmap
        .milestones
        .iter()
        .map(|milestone| milestone.version.as_str())
        .collect::<BTreeSet<_>>();
    let adr_directory = repository_root.join(ADR_DIRECTORY);
    let mut adr_paths = WalkDir::new(&adr_directory)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter_map(|entry| {
            let name = entry.file_name().to_str()?;
            is_adr_filename(name).then(|| name.to_owned())
        })
        .collect::<BTreeSet<_>>();

    let index_text = fs::read_to_string(repository_root.join(ADR_INDEX_PATH))
        .with_context(|| format!("failed to read {ADR_INDEX_PATH}"))?;
    let indexed = index_text
        .lines()
        .flat_map(markdown_link_targets)
        .filter(|target| is_adr_filename(target))
        .collect::<BTreeSet<_>>();

    for missing in adr_paths.difference(&indexed) {
        report.push(
            "R4",
            ADR_INDEX_PATH,
            None,
            format!("index is missing {missing}"),
            "add a row to the index table",
        );
    }
    for orphaned in indexed.difference(&adr_paths) {
        report.push(
            "R4",
            ADR_INDEX_PATH,
            None,
            format!("index references missing {orphaned}"),
            "remove the row or restore the ADR file",
        );
    }

    let mut metadata = BTreeMap::new();
    for file_name in std::mem::take(&mut adr_paths) {
        let path = format!("{ADR_DIRECTORY}/{file_name}");
        let content = fs::read_to_string(repository_root.join(&path))
            .with_context(|| format!("failed to read {path}"))?;
        let id = file_name[..4].to_owned();
        let status = adr_metadata_value(&content, "Status");
        let milestone = adr_metadata_value(&content, "Milestone");
        let supersedes = adr_metadata_value(&content, "Supersedes");
        match status.as_deref() {
            Some("proposed" | "accepted") => {}
            Some(value) if superseded_target(value).is_some() => {}
            Some(value) => report.push(
                "R5",
                &path,
                metadata_line(&content, "Status"),
                format!("status {value:?} is not valid"),
                "use proposed, accepted, or superseded by ADR-NNNN",
            ),
            None => report.push(
                "R5",
                &path,
                None,
                "Status metadata is missing",
                "add - **Status:** proposed | accepted | superseded by ADR-NNNN",
            ),
        }
        if supersedes.is_none() {
            report.push(
                "R5",
                &path,
                None,
                "Supersedes metadata is missing",
                "add - **Supersedes:** — or ADR-NNNN",
            );
        }
        match milestone.as_deref() {
            Some(value) => {
                for version in value.split('→').map(str::trim) {
                    if !roadmap_versions.contains(version) {
                        report.push(
                            "R5",
                            &path,
                            metadata_line(&content, "Milestone"),
                            format!(
                                "Milestone references {version:?}, which is absent from roadmap.toml"
                            ),
                            "add the milestone to roadmap.toml or correct the ADR metadata",
                        );
                    }
                }
            }
            None => report.push(
                "R5",
                &path,
                None,
                "Milestone metadata is missing",
                "add - **Milestone:** <version> using a version from roadmap.toml",
            ),
        }
        metadata.insert(
            id.clone(),
            AdrMetadata {
                id,
                path,
                status: status.unwrap_or_default(),
                supersedes: supersedes.unwrap_or_default(),
            },
        );
    }

    for record in metadata.values() {
        let Some(target_id) = superseded_target(&record.status) else {
            continue;
        };
        let Some(target) = metadata.get(target_id) else {
            report.push(
                "R5",
                &record.path,
                metadata_line_for_path(repository_root, &record.path, "Status")?,
                format!(
                    "status {:?} but ADR-{target_id} does not exist",
                    record.status
                ),
                "create the replacement ADR or correct the status",
            );
            continue;
        };
        let expected = format!("ADR-{}", record.id);
        if target.supersedes != expected {
            report.push(
                "R5",
                &target.path,
                metadata_line_for_path(repository_root, &target.path, "Supersedes")?,
                format!(
                    "Supersedes is {:?}, expected {expected:?}",
                    target.supersedes
                ),
                format!(
                    "set Supersedes to {expected} to point back to ADR-{target_id}'s predecessor"
                ),
            );
        }
    }
    Ok(metadata)
}

fn check_evidence(
    repository_root: &Path,
    traceability: &Traceability,
    metadata: &BTreeMap<String, AdrMetadata>,
    report: &mut DocumentationReport,
) -> Result<()> {
    let scenario_ids = scenario_and_invariant_ids(repository_root)?;
    let exemptions = traceability
        .evidence_exemption
        .iter()
        .map(|exemption| {
            let _ = exemption.reason.as_str();
            (exemption.record.as_str(), exemption.citation.as_str())
        })
        .collect::<BTreeSet<_>>();

    for record in metadata
        .values()
        .filter(|record| record.status == "accepted")
    {
        let content = fs::read_to_string(repository_root.join(&record.path))
            .with_context(|| format!("failed to read {}", record.path))?;
        let Some((start_line, section)) = markdown_section(&content, "Verified by") else {
            report.push(
                "R6",
                &record.path,
                None,
                "accepted ADR has no Verified by section",
                "add a ## Verified by section with resolvable citations",
            );
            continue;
        };
        for (offset, line) in section.lines().enumerate() {
            for citation in backtick_tokens(line) {
                if exemptions.contains(&(record.id.as_str(), citation.as_str())) {
                    continue;
                }
                let line_number = start_line + offset;
                if let Some((file, test_name)) = named_test_citation(&citation) {
                    check_named_test_citation(
                        repository_root,
                        record,
                        line_number,
                        &citation,
                        file,
                        test_name,
                        report,
                    )?;
                } else if is_repository_path_citation(&citation) {
                    if !repository_root.join(&citation).exists() {
                        report.push(
                            "R6",
                            &record.path,
                            Some(line_number),
                            format!("Verified by cites `{citation}`"),
                            "the repository path does not exist; correct the citation or add an [[evidence_exemption]] with a reason",
                        );
                    }
                } else if is_scenario_or_invariant_citation(&citation)
                    && !scenario_ids.contains(&citation)
                {
                    report.push(
                        "R6",
                        &record.path,
                        Some(line_number),
                        format!("Verified by cites `{citation}`"),
                        "no matching scenario or invariant ID exists; rename the citation or add an [[evidence_exemption]] with a reason",
                    );
                }
            }
        }
    }
    Ok(())
}

fn check_named_test_citation(
    repository_root: &Path,
    record: &AdrMetadata,
    line_number: usize,
    citation: &str,
    file: &str,
    test_name: &str,
    report: &mut DocumentationReport,
) -> Result<()> {
    let path = repository_root.join(file);
    if !path.is_file() {
        report.push(
            "R6",
            &record.path,
            Some(line_number),
            format!("Verified by cites `{citation}`"),
            "the test file does not exist; correct the citation or add an [[evidence_exemption]] with a reason",
        );
        return Ok(());
    }
    let content = fs::read_to_string(&path).with_context(|| format!("failed to read {file}"))?;
    if !contains_test_function(&content, test_name) {
        report.push(
            "R6",
            &record.path,
            Some(line_number),
            format!("Verified by cites `{citation}`"),
            "the file exists but contains no test function with that name\nrename the citation, or add an [[evidence_exemption]] with a reason",
        );
    }
    Ok(())
}

fn scenario_and_invariant_ids(repository_root: &Path) -> Result<BTreeSet<String>> {
    let mut ids = BTreeSet::new();
    for entry in WalkDir::new(repository_root.join("harness/scenarios"))
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
    {
        if !matches!(
            entry.path().extension().and_then(|value| value.to_str()),
            Some("yaml" | "yml")
        ) {
            continue;
        }
        let content = fs::read_to_string(entry.path())
            .with_context(|| format!("failed to read {}", entry.path().display()))?;
        for line in content.lines() {
            if let Some(value) = line.trim().strip_prefix("id:") {
                ids.insert(value.trim().trim_matches(['\'', '"']).to_owned());
            }
        }
    }
    let validator = fs::read_to_string(repository_root.join("harness/src/validator.rs"))
        .context("failed to read harness/src/validator.rs")?;
    for token in backtick_or_quoted_tokens(&validator) {
        if token.starts_with("INVARIANT-") {
            ids.insert(token);
        }
    }
    Ok(ids)
}

fn check_assertions(
    repository_root: &Path,
    traceability: &Traceability,
    report: &mut DocumentationReport,
) -> Result<()> {
    let metrics = documentation_metrics(repository_root)?;
    for assertion in &traceability.assertion {
        let Some(value) = metrics.get(&assertion.metric) else {
            report.push(
                "R7",
                TRACEABILITY_PATH,
                None,
                format!("unknown metric {:?}", assertion.metric),
                "use unit_test_count, scenario_count, adr_count, process_suite_count, active_milestone, or project_version",
            );
            continue;
        };
        let expected = assertion.pattern.replacen("{}", value, 1);
        let path = repository_root.join(&assertion.document);
        if !path.is_file() {
            continue;
        }
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", assertion.document))?;
        if !content.contains(&expected) {
            report.push(
                "R7",
                &assertion.document,
                None,
                format!("expected to contain {expected:?}"),
                format!(
                    "metric {} is {value}; update the document",
                    assertion.metric
                ),
            );
        }
    }
    Ok(())
}

fn check_retained_artifact_redaction(
    repository_root: &Path,
    report: &mut DocumentationReport,
) -> Result<()> {
    let artifacts = repository_root.join(RETAINED_ARTIFACTS_DIRECTORY);
    if !artifacts.is_dir() {
        return Ok(());
    }
    for entry in WalkDir::new(&artifacts)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_type().is_file())
    {
        let relative = entry
            .path()
            .strip_prefix(repository_root)
            .context("retained artifact escaped repository root")?
            .to_string_lossy()
            .replace('\\', "/");
        let bytes = fs::read(entry.path())
            .with_context(|| format!("failed to read retained artifact {relative}"))?;
        let content = String::from_utf8_lossy(&bytes);
        for (index, line) in content.lines().enumerate() {
            if let Some(description) = forbidden_retained_artifact_pattern(line) {
                report.push(
                    "R8",
                    &relative,
                    Some(index + 1),
                    format!("contains a forbidden retained-artifact pattern: {description}"),
                    "regenerate the retained artifact with harness/environments/azure/build-live-evidence.py before signing it",
                );
            }
        }
    }
    Ok(())
}

fn forbidden_retained_artifact_pattern(line: &str) -> Option<&'static str> {
    let lower = line.to_ascii_lowercase();
    [
        ("/subscriptions/", "Azure subscription resource path"),
        (".azurefd.net", "Azure Front Door hostname"),
        (".vault.azure.net", "Azure Key Vault hostname"),
        (".blob.core.windows.net", "Azure Blob Storage hostname"),
        (".azurecontainerapps.io", "Azure Container Apps hostname"),
        (".azurecr.io", "Azure Container Registry hostname"),
        ("/users/", "macOS home directory path"),
        ("/home/", "Linux home directory path"),
        ("c:\\users\\", "Windows home directory path"),
        ("c:\\\\users\\\\", "serialized Windows home directory path"),
        ("authorization:", "Authorization header"),
        ("bearer ", "Bearer credential"),
        (".azcopy", "AzCopy job-log path"),
        ("azcopy job", "AzCopy job identifier"),
        ("\"jobid\"", "AzCopy job identifier"),
        ("\"job_id\"", "AzCopy job identifier"),
        ("\\u001b", "serialized ANSI escape sequence"),
        ("\\x1b", "serialized ANSI escape sequence"),
    ]
    .into_iter()
    .find(|(pattern, _)| lower.contains(pattern))
    .map(|(_, description)| description)
    .or_else(|| line.contains('\u{1b}').then_some("ANSI escape sequence"))
    .or_else(|| contains_guid(line).then_some("GUID"))
    .or_else(|| contains_ip_literal(line).then_some("IP address literal"))
    .or_else(|| contains_email_address(line).then_some("email address"))
    .or_else(|| contains_sas_fragment(&lower).then_some("SAS query fragment"))
}

fn contains_guid(value: &str) -> bool {
    value.as_bytes().windows(36).any(|candidate| {
        candidate
            .iter()
            .enumerate()
            .all(|(index, byte)| match index {
                8 | 13 | 18 | 23 => *byte == b'-',
                _ => byte.is_ascii_hexdigit(),
            })
    })
}

fn contains_ip_literal(value: &str) -> bool {
    value
        .split(|character: char| {
            !(character.is_ascii_hexdigit() || matches!(character, ':' | '.' | '[' | ']' | '%'))
        })
        .filter(|candidate| !candidate.is_empty())
        .any(|candidate| {
            let candidate = candidate.trim_matches(['[', ']']);
            Ipv4Addr::from_str(candidate).is_ok()
                || (candidate != "::"
                    && (candidate.len() >= 7 || candidate.starts_with("::"))
                    && candidate.split_once('%').map_or_else(
                        || Ipv6Addr::from_str(candidate).is_ok(),
                        |(address, _)| Ipv6Addr::from_str(address).is_ok(),
                    ))
        })
}

fn contains_email_address(value: &str) -> bool {
    value
        .split(|character: char| {
            character.is_ascii_whitespace()
                || matches!(
                    character,
                    '"' | '\'' | '<' | '>' | '(' | ')' | '[' | ']' | '{' | '}' | ',' | ';'
                )
        })
        .any(|candidate| {
            let Some((local, domain)) = candidate.rsplit_once('@') else {
                return false;
            };
            !local.is_empty()
                && domain.contains('.')
                && !domain.starts_with('.')
                && !domain.ends_with('.')
                && domain
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
        })
}

fn contains_sas_fragment(lower: &str) -> bool {
    ["sig=", "?sv=", "&sv=", "?se=", "&se="]
        .into_iter()
        .any(|pattern| lower.contains(pattern))
}

fn check_retained_artifact_provenance(
    repository_root: &Path,
    report: &mut DocumentationReport,
) -> Result<()> {
    let artifacts = repository_root.join(RETAINED_ARTIFACTS_DIRECTORY);
    if !artifacts.is_dir() {
        return Ok(());
    }
    let main_reference = ["refs/heads/main", "refs/remotes/origin/main"]
        .into_iter()
        .find(|reference| git_revision_exists(repository_root, reference));

    for entry in WalkDir::new(&artifacts)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|value| value == "json")
        })
    {
        let relative = entry
            .path()
            .strip_prefix(repository_root)
            .context("retained artifact escaped repository root")?
            .to_string_lossy()
            .replace('\\', "/");
        let content = fs::read_to_string(entry.path())
            .with_context(|| format!("failed to read retained artifact {relative}"))?;
        let Ok(document) = serde_json::from_str::<serde_json::Value>(&content) else {
            continue;
        };
        let Some(commit) = document
            .get("campaign")
            .and_then(|campaign| campaign.get("commit"))
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        if commit.len() != 40 || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            report.push(
                "R9",
                &relative,
                None,
                format!("campaign.commit {commit:?} is not a full Git commit SHA"),
                "retain the full 40-character commit SHA in campaign.commit",
            );
            continue;
        }
        let Some(main_reference) = main_reference else {
            report.push(
                "R9",
                &relative,
                None,
                "cannot verify campaign.commit because the main ref is unavailable",
                "fetch main history before running doc-check",
            );
            continue;
        };
        let status = Command::new("git")
            .args(["merge-base", "--is-ancestor", commit, main_reference])
            .current_dir(repository_root)
            .status()
            .with_context(|| format!("failed to check provenance for {relative}"))?;
        if !status.success() {
            report.push(
                "R9",
                &relative,
                None,
                format!("campaign.commit {commit} is not an ancestor of main"),
                "merge the campaign commit into main before retaining its evidence",
            );
        }
    }
    Ok(())
}

fn git_revision_exists(repository_root: &Path, revision: &str) -> bool {
    Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", revision])
        .current_dir(repository_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn documentation_metrics(repository_root: &Path) -> Result<BTreeMap<String, String>> {
    let unit_test_count = ["gateway", "harness", "reconciler"]
        .into_iter()
        .map(|crate_name| count_test_attributes(&repository_root.join(crate_name)))
        .sum::<Result<usize>>()?;
    let scenario_count = count_matching_files(&repository_root.join("harness/scenarios"), |path| {
        matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("yaml" | "yml")
        ) && !path
            .components()
            .any(|component| component.as_os_str() == "schema")
    });
    let adr_count = count_matching_files(&repository_root.join(ADR_DIRECTORY), |path| {
        path.file_name()
            .and_then(|value| value.to_str())
            .is_some_and(is_adr_filename)
    });
    let process_suite_count =
        count_matching_files(&repository_root.join("harness/scripts"), |path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.ends_with("-smoke.sh"))
        });
    let roadmap_text = fs::read_to_string(repository_root.join("roadmap.toml"))
        .context("failed to read roadmap.toml")?;
    let roadmap: Roadmap = toml::from_str(&roadmap_text).context("failed to parse roadmap.toml")?;
    let active_milestone = roadmap
        .milestones
        .iter()
        .find(|milestone| milestone.status == "active")
        .map(|milestone| milestone.version.clone())
        .unwrap_or_default();
    let project_version = fs::read_to_string(repository_root.join("VERSION"))
        .context("failed to read VERSION")?
        .trim()
        .to_owned();

    Ok(BTreeMap::from([
        ("unit_test_count".to_owned(), unit_test_count.to_string()),
        ("scenario_count".to_owned(), scenario_count.to_string()),
        ("adr_count".to_owned(), adr_count.to_string()),
        (
            "process_suite_count".to_owned(),
            process_suite_count.to_string(),
        ),
        ("active_milestone".to_owned(), active_milestone),
        ("project_version".to_owned(), project_version),
    ]))
}

fn count_test_attributes(directory: &Path) -> Result<usize> {
    let mut count = 0;
    for entry in WalkDir::new(directory)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_type().is_file()
                && entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "rs")
        })
    {
        let content = fs::read_to_string(entry.path())
            .with_context(|| format!("failed to read {}", entry.path().display()))?;
        count += content
            .lines()
            .filter(|line| matches!(line.trim(), "#[test]" | "#[tokio::test]"))
            .count();
    }
    Ok(count)
}

fn count_matching_files(directory: &Path, predicate: impl Fn(&Path) -> bool) -> usize {
    WalkDir::new(directory)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file() && predicate(entry.path()))
        .count()
}

fn is_safe_repository_path(path: &str) -> bool {
    let path = Path::new(path);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn display_repository_target(repository_root: &Path, path: &Path) -> String {
    path.strip_prefix(repository_root).map_or_else(
        |_| path.display().to_string(),
        |relative| format!("/{}", relative.display()),
    )
}

fn is_adr_filename(name: &str) -> bool {
    name.len() > 8
        && name.ends_with(".md")
        && name.as_bytes().get(4) == Some(&b'-')
        && name[..4].bytes().all(|byte| byte.is_ascii_digit())
}

fn adr_metadata_value(content: &str, name: &str) -> Option<String> {
    let prefix = format!("- **{name}:**");
    content
        .lines()
        .find_map(|line| line.strip_prefix(&prefix).map(str::trim))
        .map(str::to_owned)
}

fn metadata_line(content: &str, name: &str) -> Option<usize> {
    let prefix = format!("- **{name}:**");
    content
        .lines()
        .position(|line| line.starts_with(&prefix))
        .map(|index| index + 1)
}

fn metadata_line_for_path(repository_root: &Path, path: &str, name: &str) -> Result<Option<usize>> {
    let content = fs::read_to_string(repository_root.join(path))
        .with_context(|| format!("failed to read {path}"))?;
    Ok(metadata_line(&content, name))
}

fn superseded_target(status: &str) -> Option<&str> {
    let target = status.strip_prefix("superseded by ADR-")?;
    (target.len() == 4 && target.bytes().all(|byte| byte.is_ascii_digit())).then_some(target)
}

fn markdown_section<'a>(content: &'a str, heading: &str) -> Option<(usize, &'a str)> {
    let marker = format!("## {heading}");
    let start_offset = content.find(&marker)?;
    let section_start = start_offset + marker.len();
    let body = &content[section_start..];
    let body = body.strip_prefix('\n').unwrap_or(body);
    let section_end = body.find("\n## ").unwrap_or(body.len());
    let start_line = content[..section_start].lines().count() + 1;
    Some((start_line, &body[..section_end]))
}

fn backtick_tokens(line: &str) -> Vec<String> {
    delimited_tokens(line, '`')
}

fn backtick_or_quoted_tokens(content: &str) -> Vec<String> {
    ['`', '"']
        .into_iter()
        .flat_map(|delimiter| delimited_tokens(content, delimiter))
        .collect()
}

fn delimited_tokens(content: &str, delimiter: char) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut parts = content.split(delimiter);
    while let Some(_) = parts.next() {
        let Some(token) = parts.next() else {
            break;
        };
        if !token.is_empty() {
            tokens.push(token.to_owned());
        }
    }
    tokens
}

fn named_test_citation(citation: &str) -> Option<(&str, &str)> {
    let (file, test_name) = citation.split_once("::")?;
    (file.ends_with(".rs") && !test_name.is_empty()).then_some((file, test_name))
}

fn is_repository_path_citation(citation: &str) -> bool {
    if citation.chars().any(char::is_whitespace) || !is_safe_repository_path(citation) {
        return false;
    }
    let Some(first) = citation.split('/').next() else {
        return false;
    };
    matches!(
        first,
        ".github" | "deploy" | "docs" | "gateway" | "harness" | "infra" | "reconciler" | "site"
    ) || Path::new(citation).extension().is_some()
}

fn is_scenario_or_invariant_citation(citation: &str) -> bool {
    citation.starts_with("INVARIANT-")
        || (citation
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
            && citation.contains('-')
            && citation.rsplit('-').next().is_some_and(|suffix| {
                suffix.len() == 3 && suffix.bytes().all(|b| b.is_ascii_digit())
            }))
}

fn contains_test_function(content: &str, test_name: &str) -> bool {
    let lines = content.lines().collect::<Vec<_>>();
    lines.iter().enumerate().any(|(index, line)| {
        let trimmed = line.trim_start();
        let is_function = trimmed.starts_with(&format!("fn {test_name}("))
            || trimmed.starts_with(&format!("async fn {test_name}("));
        is_function
            && lines[..index]
                .iter()
                .rev()
                .take_while(|previous| {
                    let trimmed = previous.trim();
                    trimmed.is_empty() || trimmed.starts_with("#[")
                })
                .any(|previous| matches!(previous.trim(), "#[test]" | "#[tokio::test]"))
    })
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn extracts_relative_markdown_links() {
        let targets = markdown_link_targets(
            "See [spec](../SPEC.md), [section](#here), and [web](https://example.com).",
        );
        assert_eq!(targets, vec!["../SPEC.md", "#here", "https://example.com"]);
        assert_eq!(relative_link_path("../SPEC.md"), Some("../SPEC.md"));
        assert_eq!(relative_link_path("#here"), None);
        assert_eq!(relative_link_path("https://example.com"), None);
        assert_eq!(
            markdown_link_targets("[spec]: ../SPEC.md \"Specification\""),
            vec!["../SPEC.md"]
        );
    }

    #[test]
    fn recognizes_only_test_functions_with_test_attributes() {
        let content = r#"
#[test]
fn synchronous_test() {}

#[tokio::test]
async fn asynchronous_test() {}

fn helper() {}
"#;
        assert!(contains_test_function(content, "synchronous_test"));
        assert!(contains_test_function(content, "asynchronous_test"));
        assert!(!contains_test_function(content, "helper"));
        assert!(!contains_test_function(content, "missing"));
    }

    #[test]
    fn formats_all_violations_before_the_count() {
        let report = DocumentationReport {
            violations: vec![
                DocumentationViolation {
                    rule: "R1".to_owned(),
                    path: "a.md".to_owned(),
                    line: None,
                    message: "not registered".to_owned(),
                    hint: "register it".to_owned(),
                },
                DocumentationViolation {
                    rule: "R3".to_owned(),
                    path: "b.md".to_owned(),
                    line: Some(7),
                    message: "broken link".to_owned(),
                    hint: "fix it".to_owned(),
                },
            ],
        };
        let text = report.text();
        assert!(text.contains("a.md: not registered\n  register it"));
        assert!(text.contains("b.md:7: broken link\n  fix it"));
        assert!(text.ends_with("2 documentation violations\n"));
    }

    #[test]
    fn recognizes_adr_file_names() {
        assert!(is_adr_filename("0008-listing.md"));
        assert!(!is_adr_filename("README.md"));
        assert!(!is_adr_filename("008-listing.md"));
    }

    #[test]
    fn ignores_exchange_markdown_attachments_but_not_messages() {
        assert!(ignored_documentation_path(
            ".overmesh/exchange/0001-spec/attachments/protocol.md"
        ));
        assert!(!ignored_documentation_path(
            ".overmesh/exchange/0001-spec/message.md"
        ));
    }

    #[test]
    fn repository_fixture_passes_all_documentation_rules() {
        let fixture = documentation_fixture();
        let report = check(fixture.path()).expect("check fixture");
        assert_eq!(report.violations, Vec::new());
    }

    #[test]
    fn collects_all_rule_violations_in_one_run() {
        let fixture = documentation_fixture();
        write(
            fixture.path(),
            "unregistered.md",
            "# Unregistered document\n",
        );
        write(
            fixture.path(),
            "docs/WHY_OVERMESH.md",
            "# Why\n\nSee [missing](missing.md).\n",
        );
        append(
            fixture.path(),
            TRACEABILITY_PATH,
            r#"

[[excluded]]
path = "missing-excluded.md"
reason = "Intentional fixture."
"#,
        );
        write(
            fixture.path(),
            ADR_INDEX_PATH,
            "# ADRs\n\n- [0001](0001-first.md)\n",
        );
        write(
            fixture.path(),
            "docs/adr/0001-first.md",
            r#"# ADR-0001

- **Status:** accepted
- **Milestone:** 0.9.1
- **Supersedes:** —

## Verified by

- `gateway/src/lib.rs::missing_test`
"#,
        );
        write(
            fixture.path(),
            "docs/adr/0002-second.md",
            r#"# ADR-0002

- **Status:** obsolete
- **Milestone:** 0.9.1
- **Supersedes:** —

## Verified by

- `SCENARIO-001`
"#,
        );
        write(
            fixture.path(),
            "harness/artifacts/live/0.9.0/evidence.json",
            r#"{"subscription":"/subscriptions/11111111-2222-3333-4444-555555555555"}"#,
        );

        let report = check(fixture.path()).expect("check fixture");
        let rules = report
            .violations
            .iter()
            .map(|violation| violation.rule.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            rules,
            BTreeSet::from(["R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8"])
        );
    }

    #[test]
    fn rejects_unredacted_live_evidence() {
        let fixture = documentation_fixture();
        write(
            fixture.path(),
            "harness/artifacts/live/0.9.0/evidence.json",
            r#"{
  "subscription": "/subscriptions/11111111-2222-3333-4444-555555555555",
  "endpoint": "https://example.azurefd.net",
  "diagnostic": "\u001b[31mfailed\u001b[0m",
  "home": "/Users/operator/.azcopy/jobs.log",
  "address": "203.0.113.42",
  "addressV6": "2001:db8::1",
  "credential": "Authorization: Bearer secret"
}"#,
        );

        let report = check(fixture.path()).expect("check fixture");
        let violations = report
            .violations
            .iter()
            .filter(|violation| violation.rule == "R8")
            .collect::<Vec<_>>();
        assert_eq!(violations.len(), 7);
        assert!(
            violations.iter().all(|violation| {
                violation.path == "harness/artifacts/live/0.9.0/evidence.json"
            })
        );
        let descriptions = violations
            .iter()
            .map(|violation| violation.message.as_str())
            .collect::<Vec<_>>();
        assert!(
            descriptions
                .iter()
                .any(|message| message.contains("home directory"))
        );
        assert!(
            descriptions
                .iter()
                .any(|message| message.contains("IP address literal"))
        );
        assert!(
            descriptions
                .iter()
                .any(|message| message.contains("Authorization header"))
        );
    }

    #[test]
    fn retained_campaign_commit_must_be_an_ancestor_of_main() {
        let fixture = documentation_fixture();
        git(
            fixture.path(),
            &["config", "user.email", "fixture@example.test"],
        );
        git(fixture.path(), &["config", "user.name", "Fixture"]);
        git(fixture.path(), &["add", "."]);
        git(fixture.path(), &["commit", "--quiet", "-m", "main"]);
        git(fixture.path(), &["branch", "-M", "main"]);
        git(fixture.path(), &["switch", "--quiet", "-c", "campaign"]);
        write(fixture.path(), "campaign.txt", "unmerged\n");
        git(fixture.path(), &["add", "campaign.txt"]);
        git(
            fixture.path(),
            &["commit", "--quiet", "-m", "unmerged campaign"],
        );
        let campaign_commit = git_output(fixture.path(), &["rev-parse", "HEAD"]);
        git(fixture.path(), &["switch", "--quiet", "main"]);
        write(
            fixture.path(),
            "harness/artifacts/live/0.10.0/performance.json",
            &format!(r#"{{"campaign":{{"commit":"{campaign_commit}"}}}}"#),
        );

        let report = check(fixture.path()).expect("check fixture");
        assert!(report.violations.iter().any(|violation| {
            violation.rule == "R9" && violation.message.contains("is not an ancestor of main")
        }));
    }

    #[test]
    fn evidence_exemption_suppresses_only_the_named_citation() {
        let fixture = documentation_fixture();
        write(
            fixture.path(),
            "docs/adr/0001-first.md",
            r#"# ADR-0001

- **Status:** accepted
- **Milestone:** 0.9.1
- **Supersedes:** —

## Verified by

- `gateway/src/lib.rs::missing_test`
"#,
        );
        append(
            fixture.path(),
            TRACEABILITY_PATH,
            r#"

[[evidence_exemption]]
record = "0001"
citation = "gateway/src/lib.rs::missing_test"
reason = "Intentional fixture."
"#,
        );

        let report = check(fixture.path()).expect("check fixture");
        assert!(report.passed(), "{}", report.text());
    }

    #[test]
    fn superseded_adr_requires_a_reciprocal_supersedes_field() {
        let fixture = documentation_fixture();
        write(
            fixture.path(),
            "docs/adr/0001-first.md",
            r#"# ADR-0001

- **Status:** superseded by ADR-0002
- **Milestone:** 0.9.1
- **Supersedes:** —

## Verified by

- `gateway/src/lib.rs::documented_test`
"#,
        );

        let report = check(fixture.path()).expect("check fixture");
        assert!(report.violations.iter().any(|violation| {
            violation.rule == "R5" && violation.message.contains("expected \"ADR-0001\"")
        }));
    }

    #[test]
    fn adr_milestone_must_exist_in_the_roadmap() {
        let fixture = documentation_fixture();
        write(
            fixture.path(),
            "docs/adr/0001-first.md",
            r#"# ADR-0001

- **Status:** accepted
- **Milestone:** 0.10.1
- **Supersedes:** —

## Verified by

- `gateway/src/lib.rs::documented_test`
"#,
        );

        let report = check(fixture.path()).expect("check fixture");
        assert!(report.violations.iter().any(|violation| {
            violation.rule == "R5" && violation.message.contains("absent from roadmap.toml")
        }));
    }

    fn documentation_fixture() -> TempDir {
        let fixture = tempfile::tempdir().expect("temporary repository");
        for directory in [
            "docs/adr",
            "gateway/src",
            "harness/src",
            "harness/artifacts",
            "harness/scenarios/protocol",
            "harness/scripts",
            "reconciler/src",
        ] {
            fs::create_dir_all(fixture.path().join(directory)).expect("create fixture directory");
        }
        write(
            fixture.path(),
            TRACEABILITY_PATH,
            r#"schema_version = 1

[[document]]
path = "docs/WHY_OVERMESH.md"
section = "overview"
title = "Why"
weight = 10

[[document]]
path = "docs/adr/README.md"
section = "decisions"
title = "ADRs"
weight = 0

[[document]]
path = "docs/adr/0001-first.md"
section = "decisions"
title = "First"
weight = 10

[[document]]
path = "docs/adr/0002-second.md"
section = "decisions"
title = "Second"
weight = 20

[[assertion]]
document = "docs/WHY_OVERMESH.md"
metric = "unit_test_count"
pattern = "{} unit and integration tests"

[[assertion]]
document = "docs/WHY_OVERMESH.md"
metric = "scenario_count"
pattern = "{} declarative"

[[assertion]]
document = "docs/WHY_OVERMESH.md"
metric = "active_milestone"
pattern = "milestone {}"
"#,
        );
        write(
            fixture.path(),
            "docs/WHY_OVERMESH.md",
            "# Why\n\n1 unit and integration tests, 1 declarative scenario, milestone 0.9.1.\n",
        );
        write(
            fixture.path(),
            ADR_INDEX_PATH,
            "# ADRs\n\n- [0001](0001-first.md)\n- [0002](0002-second.md)\n",
        );
        write(
            fixture.path(),
            "docs/adr/0001-first.md",
            r#"# ADR-0001

- **Status:** accepted
- **Milestone:** 0.9.1
- **Supersedes:** —

## Verified by

- `gateway/src/lib.rs::documented_test`
- `SCENARIO-001`
- `INVARIANT-001`
"#,
        );
        write(
            fixture.path(),
            "docs/adr/0002-second.md",
            r#"# ADR-0002

- **Status:** accepted
- **Milestone:** 0.9.1
- **Supersedes:** —

## Verified by

- `gateway/src/lib.rs`
"#,
        );
        write(
            fixture.path(),
            "gateway/src/lib.rs",
            "#[test]\nfn documented_test() {}\n",
        );
        write(
            fixture.path(),
            "harness/src/validator.rs",
            "const ID: &str = \"INVARIANT-001\";\n",
        );
        write(
            fixture.path(),
            "harness/scenarios/protocol/scenario.yaml",
            "id: SCENARIO-001\n",
        );
        write(
            fixture.path(),
            "harness/scripts/gateway-smoke.sh",
            "#!/usr/bin/env bash\n",
        );
        write(
            fixture.path(),
            "roadmap.toml",
            "[[milestones]]\nversion = \"0.9.1\"\nstatus = \"active\"\n",
        );
        write(fixture.path(), "VERSION", "0.9.1\n");
        let status = Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(fixture.path())
            .status()
            .expect("initialize fixture repository");
        assert!(status.success());
        fixture
    }

    fn write(root: &Path, path: &str, content: &str) {
        let path = root.join(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        fs::write(path, content).expect("write fixture");
    }

    fn append(root: &Path, path: &str, content: &str) {
        let path = root.join(path);
        let mut existing = fs::read_to_string(&path).expect("read fixture");
        existing.push_str(content);
        fs::write(path, existing).expect("append fixture");
    }

    fn git(root: &Path, arguments: &[&str]) {
        let status = Command::new("git")
            .args(arguments)
            .current_dir(root)
            .status()
            .expect("run git");
        assert!(status.success(), "git {arguments:?} failed");
    }

    fn git_output(root: &Path, arguments: &[&str]) -> String {
        let output = Command::new("git")
            .args(arguments)
            .current_dir(root)
            .output()
            .expect("run git");
        assert!(output.status.success(), "git {arguments:?} failed");
        String::from_utf8(output.stdout)
            .expect("git output is UTF-8")
            .trim()
            .to_owned()
    }
}
