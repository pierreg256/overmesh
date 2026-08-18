use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use walkdir::WalkDir;

const TRACEABILITY_PATH: &str = "docs/traceability.toml";
const GENERATED_CONTENT_ROOT: &str = "site/content";
const ADR_INDEX_PATH: &str = "docs/adr/README.md";
const ADR_INDEX_SLUG: &str = "index-of-records";
const SITE_IMAGES: [(&str, &str); 2] = [
    ("docs/overmesh-icon.png", "site/static/og/overmesh-icon.png"),
    (
        "docs/overmesh-open-graph.png",
        "site/static/og/overmesh-open-graph.png",
    ),
];

#[derive(Debug, Deserialize)]
struct Traceability {
    document: Vec<Document>,
}

#[derive(Debug, Deserialize)]
struct Document {
    path: String,
    section: String,
    title: String,
    weight: i64,
}

#[derive(Debug)]
pub struct AssemblyOptions<'a> {
    pub repository_url: &'a str,
    pub commit: &'a str,
}

#[derive(Debug, Eq, PartialEq)]
pub struct AssemblyReport {
    pub document_count: usize,
    pub image_count: usize,
}

#[derive(Debug)]
struct Publication {
    output_path: PathBuf,
    zola_link: String,
}

pub fn assemble(repository_root: &Path, options: &AssemblyOptions<'_>) -> Result<AssemblyReport> {
    let traceability: Traceability = toml::from_str(
        &fs::read_to_string(repository_root.join(TRACEABILITY_PATH))
            .with_context(|| format!("failed to read {TRACEABILITY_PATH}"))?,
    )
    .with_context(|| format!("failed to parse {TRACEABILITY_PATH}"))?;
    validate_options(options)?;
    clean_generated_content(repository_root)?;

    let publications = publication_map(repository_root, &traceability)?;
    for document in &traceability.document {
        assemble_document(repository_root, document, &publications, options)?;
    }
    assemble_images(repository_root)?;

    Ok(AssemblyReport {
        document_count: traceability.document.len(),
        image_count: SITE_IMAGES.len(),
    })
}

fn validate_options(options: &AssemblyOptions<'_>) -> Result<()> {
    if !options.repository_url.starts_with("https://github.com/") {
        bail!(
            "site assembly requires an HTTPS GitHub repository URL, got {}",
            options.repository_url
        );
    }
    if options.commit.is_empty() || !options.commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("site assembly requires a hexadecimal Git commit");
    }
    Ok(())
}

fn publication_map(
    repository_root: &Path,
    traceability: &Traceability,
) -> Result<BTreeMap<PathBuf, Publication>> {
    let mut publications = BTreeMap::new();
    let mut output_sources = BTreeMap::new();
    for document in &traceability.document {
        let source = repository_root.join(&document.path);
        let canonical = source
            .canonicalize()
            .with_context(|| format!("failed to resolve published document {}", document.path))?;
        let slug = document_slug(document);
        let output_path = PathBuf::from(GENERATED_CONTENT_ROOT)
            .join(&document.section)
            .join(format!("{slug}.md"));
        if let Some(existing) = output_sources.insert(output_path.clone(), document.path.clone()) {
            bail!(
                "published documents {existing} and {} map to {}",
                document.path,
                output_path.display()
            );
        }
        let zola_link = format!("@/{}/{slug}.md", document.section);
        if publications
            .insert(
                canonical,
                Publication {
                    output_path,
                    zola_link,
                },
            )
            .is_some()
        {
            bail!("published document {} is registered twice", document.path);
        }
    }
    Ok(publications)
}

fn document_slug(document: &Document) -> String {
    if document.path == ADR_INDEX_PATH {
        return ADR_INDEX_SLUG.to_owned();
    }
    Path::new(&document.path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn clean_generated_content(repository_root: &Path) -> Result<()> {
    let content_root = repository_root.join(GENERATED_CONTENT_ROOT);
    if !content_root.exists() {
        return Ok(());
    }
    for entry in WalkDir::new(&content_root)
        .min_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
    {
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("md")
            && path.file_name().and_then(|name| name.to_str()) != Some("_index.md")
        {
            fs::remove_file(path)
                .with_context(|| format!("failed to remove stale {}", path.display()))?;
        }
    }
    Ok(())
}

fn assemble_document(
    repository_root: &Path,
    document: &Document,
    publications: &BTreeMap<PathBuf, Publication>,
    options: &AssemblyOptions<'_>,
) -> Result<()> {
    let source_path = repository_root.join(&document.path);
    let source = fs::read_to_string(&source_path)
        .with_context(|| format!("failed to read {}", document.path))?;
    let body = remove_first_h1(&source);
    let rewritten = rewrite_links(repository_root, &source_path, &body, publications, options)?;
    let publication = publications
        .get(
            &source_path
                .canonicalize()
                .with_context(|| format!("failed to resolve {}", document.path))?,
        )
        .with_context(|| format!("missing publication mapping for {}", document.path))?;
    let output_path = repository_root.join(&publication.output_path);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let title = toml::Value::String(document.title.clone()).to_string();
    let generated = format!(
        "+++\ntitle = {title}\nweight = {}\n+++\n\n{}",
        document.weight,
        rewritten.trim_start()
    );
    fs::write(&output_path, generated)
        .with_context(|| format!("failed to write {}", output_path.display()))
}

fn remove_first_h1(source: &str) -> String {
    let mut removed = false;
    source
        .lines()
        .filter(|line| {
            if !removed && line.starts_with("# ") {
                removed = true;
                false
            } else {
                true
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn rewrite_links(
    repository_root: &Path,
    source_path: &Path,
    source: &str,
    publications: &BTreeMap<PathBuf, Publication>,
    options: &AssemblyOptions<'_>,
) -> Result<String> {
    let mut in_fence = false;
    let mut rewritten = String::new();
    for line in source.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            in_fence = !in_fence;
            rewritten.push_str(line);
        } else if in_fence {
            rewritten.push_str(line);
        } else {
            rewritten.push_str(&rewrite_line(
                repository_root,
                source_path,
                line,
                publications,
                options,
            )?);
        }
        rewritten.push('\n');
    }
    Ok(rewritten)
}

fn rewrite_line(
    repository_root: &Path,
    source_path: &Path,
    line: &str,
    publications: &BTreeMap<PathBuf, Publication>,
    options: &AssemblyOptions<'_>,
) -> Result<String> {
    let mut rewritten = String::new();
    let mut cursor = 0;
    while let Some(relative_start) = line[cursor..].find("](") {
        let marker = cursor + relative_start;
        let target_start = marker + 2;
        let Some(relative_end) = line[target_start..].find(')') else {
            break;
        };
        let target_end = target_start + relative_end;
        if inside_code_span(line, marker) {
            rewritten.push_str(&line[cursor..target_end + 1]);
            cursor = target_end + 1;
            continue;
        }
        rewritten.push_str(&line[cursor..target_start]);
        rewritten.push_str(&rewrite_target_expression(
            repository_root,
            source_path,
            &line[target_start..target_end],
            publications,
            options,
        )?);
        rewritten.push(')');
        cursor = target_end + 1;
    }
    rewritten.push_str(&line[cursor..]);

    let definition_start = rewritten.len() - line[cursor..].len();
    if definition_start == 0 {
        rewrite_reference_definition(
            repository_root,
            source_path,
            &rewritten,
            publications,
            options,
        )
    } else {
        Ok(rewritten)
    }
}

fn rewrite_reference_definition(
    repository_root: &Path,
    source_path: &Path,
    line: &str,
    publications: &BTreeMap<PathBuf, Publication>,
    options: &AssemblyOptions<'_>,
) -> Result<String> {
    let leading = line.len() - line.trim_start().len();
    let trimmed = &line[leading..];
    let Some(marker) = trimmed.find("]:") else {
        return Ok(line.to_owned());
    };
    if !trimmed.starts_with('[') {
        return Ok(line.to_owned());
    }
    let expression_start = leading + marker + 2;
    let mut output = line[..expression_start].to_owned();
    output.push_str(&rewrite_target_expression(
        repository_root,
        source_path,
        &line[expression_start..],
        publications,
        options,
    )?);
    Ok(output)
}

fn rewrite_target_expression(
    repository_root: &Path,
    source_path: &Path,
    expression: &str,
    publications: &BTreeMap<PathBuf, Publication>,
    options: &AssemblyOptions<'_>,
) -> Result<String> {
    let leading = expression.len() - expression.trim_start().len();
    let value = &expression[leading..];
    let (target, target_offset, target_length) = if let Some(value) = value.strip_prefix('<') {
        let Some(end) = value.find('>') else {
            return Ok(expression.to_owned());
        };
        (&value[..end], leading + 1, end)
    } else {
        let length = value.find(char::is_whitespace).unwrap_or(value.len());
        (&value[..length], leading, length)
    };
    let Some(replacement) =
        rewrite_target(repository_root, source_path, target, publications, options)?
    else {
        return Ok(expression.to_owned());
    };
    let mut output = expression.to_owned();
    output.replace_range(target_offset..target_offset + target_length, &replacement);
    Ok(output)
}

fn rewrite_target(
    repository_root: &Path,
    source_path: &Path,
    target: &str,
    publications: &BTreeMap<PathBuf, Publication>,
    options: &AssemblyOptions<'_>,
) -> Result<Option<String>> {
    let path_end = target.find(['?', '#']).unwrap_or(target.len());
    let relative = &target[..path_end];
    if relative.is_empty()
        || relative.starts_with('/')
        || relative.contains("://")
        || relative.starts_with("mailto:")
        || relative.starts_with("tel:")
        || relative.starts_with("data:")
    {
        return Ok(None);
    }
    let suffix = &target[path_end..];
    let source_directory = source_path
        .parent()
        .context("published document has no parent directory")?;
    let mut resolved = source_directory.join(relative);
    if resolved.is_dir() {
        resolved = resolved.join("README.md");
    }
    let canonical = resolved
        .canonicalize()
        .with_context(|| format!("failed to resolve Markdown target {target:?}"))?;
    if let Some(publication) = publications.get(&canonical) {
        return Ok(Some(format!("{}{suffix}", publication.zola_link)));
    }
    let canonical_root = repository_root
        .canonicalize()
        .context("failed to resolve repository root")?;
    let repository_path = canonical
        .strip_prefix(&canonical_root)
        .with_context(|| format!("Markdown target {target:?} escapes the repository"))?;
    let kind = if canonical.is_dir() { "tree" } else { "blob" };
    Ok(Some(format!(
        "{}/{kind}/{}/{}{}",
        options.repository_url.trim_end_matches('/'),
        options.commit,
        repository_path.to_string_lossy().replace('\\', "/"),
        suffix
    )))
}

fn inside_code_span(line: &str, offset: usize) -> bool {
    line[..offset].bytes().filter(|byte| *byte == b'`').count() % 2 == 1
}

fn assemble_images(repository_root: &Path) -> Result<()> {
    for (source, destination) in SITE_IMAGES {
        let source_path = repository_root.join(source);
        let destination_path = repository_root.join(destination);
        if let Some(parent) = destination_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::copy(&source_path, &destination_path).with_context(|| {
            format!(
                "failed to copy {} to {}",
                source_path.display(),
                destination_path.display()
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn assembles_registered_documents_and_rewrites_links() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path();
        fs::create_dir_all(root.join("docs/adr")).unwrap();
        fs::create_dir_all(root.join("site/content/overview")).unwrap();
        fs::create_dir_all(root.join("site/static/og")).unwrap();
        fs::write(
            root.join(TRACEABILITY_PATH),
            r#"
                [[document]]
                path = "docs/guide.md"
                section = "overview"
                title = "Guide"
                weight = 10

                [[document]]
                path = "docs/adr/README.md"
                section = "decisions"
                title = "Records"
                weight = 0
            "#,
        )
        .unwrap();
        fs::write(
            root.join("docs/guide.md"),
            "# Old title\n\n[records](adr/README.md) [license](../LICENSE#terms)\n",
        )
        .unwrap();
        fs::write(
            root.join("docs/adr/README.md"),
            "# Records\n\n[guide](../guide.md)\n",
        )
        .unwrap();
        fs::write(root.join("LICENSE"), "terms").unwrap();
        fs::write(root.join("docs/overmesh-icon.png"), "icon").unwrap();
        fs::write(root.join("docs/overmesh-open-graph.png"), "og").unwrap();
        fs::write(
            root.join("site/content/overview/stale.md"),
            "stale generated page",
        )
        .unwrap();
        fs::write(
            root.join("site/content/overview/_index.md"),
            "tracked section",
        )
        .unwrap();

        let report = assemble(
            root,
            &AssemblyOptions {
                repository_url: "https://github.com/example/overmesh",
                commit: "abc123",
            },
        )
        .unwrap();

        assert_eq!(
            report,
            AssemblyReport {
                document_count: 2,
                image_count: 2
            }
        );
        let guide = fs::read_to_string(root.join("site/content/overview/guide.md")).unwrap();
        assert!(guide.starts_with("+++\ntitle = \"Guide\"\nweight = 10\n+++\n"));
        assert!(!guide.contains("# Old title"));
        assert!(guide.contains("[records](@/decisions/index-of-records.md)"));
        assert!(
            guide.contains(
                "[license](https://github.com/example/overmesh/blob/abc123/LICENSE#terms)"
            )
        );
        assert!(root.join("site/content/overview/_index.md").exists());
        assert!(!root.join("site/content/overview/stale.md").exists());
        assert!(
            root.join("site/content/decisions/index-of-records.md")
                .exists()
        );
        assert_eq!(
            fs::read(root.join("site/static/og/overmesh-icon.png")).unwrap(),
            b"icon"
        );
    }

    #[test]
    fn preserves_code_fences_and_inline_code() {
        let root = TempDir::new().unwrap();
        fs::write(root.path().join("target.md"), "target").unwrap();
        let source = "```\n[x](target.md)\n```\n`[x](target.md)`\n";
        let rewritten = rewrite_links(
            root.path(),
            &root.path().join("source.md"),
            source,
            &BTreeMap::new(),
            &AssemblyOptions {
                repository_url: "https://github.com/example/overmesh",
                commit: "abc123",
            },
        )
        .unwrap();
        assert_eq!(rewritten, source);
    }
}
