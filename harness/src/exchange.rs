use std::{
    collections::BTreeSet,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    process::Command,
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, macros::format_description};

pub const DEFAULT_CONSECUTIVE_LIMIT: usize = 5;
pub const MAX_BODY_BYTES: usize = 16 * 1024;
const SCHEMA_VERSION: u32 = 1;
const CONFIG_SCHEMA_VERSION: u32 = 1;
const CREATED_AT_FORMAT: &[time::format_description::BorrowedFormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MessageKind {
    Finding,
    Question,
    Correction,
    Spec,
    Report,
    Verdict,
    Approval,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RefKind {
    Code,
    Commit,
    Artifact,
    Record,
    Url,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum VerdictOutcome {
    Verified,
    NotVerified,
    Withdrawn,
    Superseded,
}

impl VerdictOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::NotVerified => "not-verified",
            Self::Withdrawn => "withdrawn",
            Self::Superseded => "superseded",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ThreadState {
    Open,
    AwaitingApproval,
    Escalated,
    Resolved,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct MessageRef {
    pub kind: RefKind,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeMessage {
    pub schema_version: u32,
    pub thread: String,
    pub seq: u32,
    pub author: String,
    pub kind: MessageKind,
    pub created_at: String,
    pub subject: String,
    pub body: String,
    pub replies_to: Option<u32>,
    #[serde(default)]
    pub refs: Vec<MessageRef>,
    pub answered_by: Option<String>,
    pub outcome: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostRequest {
    pub kind: MessageKind,
    pub subject: String,
    pub body: String,
    pub thread: Option<String>,
    #[serde(default)]
    pub refs: Vec<MessageRef>,
    pub replies_to: Option<u32>,
    pub answered_by: Option<String>,
    pub outcome: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostResult {
    pub thread: String,
    pub seq: u32,
    pub state: ThreadState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSummary {
    pub thread: String,
    pub subject: String,
    pub state: ThreadState,
    pub waiting_on: Option<String>,
    pub messages: usize,
    pub last_author: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadMessage {
    pub schema_version: u32,
    pub thread: String,
    pub seq: u32,
    pub author: String,
    pub kind: MessageKind,
    pub created_at: String,
    pub subject: String,
    pub body: Option<String>,
    pub replies_to: Option<u32>,
    pub refs: Vec<MessageRef>,
    pub answered_by: Option<String>,
    pub outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withheld: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadView {
    pub thread: String,
    pub subject: String,
    pub state: ThreadState,
    pub waiting_on: Option<String>,
    pub messages: Vec<ReadMessage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostOrigin {
    Cli,
    Mcp,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExchangeConfig {
    schema_version: u32,
    assistants: Vec<String>,
    consecutive_message_limit: usize,
}

#[derive(Debug, Clone)]
pub struct Exchange {
    repository_root: PathBuf,
    root: PathBuf,
    lock_root: PathBuf,
    allowed_assistants: BTreeSet<String>,
    consecutive_limit: usize,
}

impl Exchange {
    pub fn new(
        repository_root: impl Into<PathBuf>,
        allowed_assistants: impl IntoIterator<Item = String>,
        consecutive_limit: usize,
    ) -> Result<Self> {
        let repository_root = repository_root
            .into()
            .canonicalize()
            .context("failed to canonicalize exchange repository root")?;
        if consecutive_limit == 0 {
            bail!("exchange consecutive-message limit must be positive");
        }
        let allowed_assistants = allowed_assistants
            .into_iter()
            .map(|author| author.trim().to_owned())
            .filter(|author| !author.is_empty())
            .collect::<BTreeSet<_>>();
        if allowed_assistants.is_empty()
            || allowed_assistants.contains("human")
            || allowed_assistants
                .iter()
                .any(|author| !valid_author(author))
        {
            bail!("exchange assistant allowlist must contain non-human identifiers");
        }
        let lock_root = git_path(&repository_root, "overmesh-exchange-locks")?;
        Ok(Self {
            root: repository_root.join(".overmesh/exchange"),
            repository_root,
            lock_root,
            allowed_assistants,
            consecutive_limit,
        })
    }

    pub fn default_for_repository(repository_root: impl Into<PathBuf>) -> Result<Self> {
        Self::new(
            repository_root,
            ["claude".to_owned(), "copilot".to_owned()],
            DEFAULT_CONSECUTIVE_LIMIT,
        )
    }

    pub fn configured_for_repository(repository_root: impl Into<PathBuf>) -> Result<Self> {
        let repository_root = repository_root.into();
        let path = repository_root.join(".overmesh/exchange/config.json");
        let config: ExchangeConfig = serde_json::from_slice(
            &fs::read(&path)
                .with_context(|| format!("failed to read exchange config {}", path.display()))?,
        )
        .with_context(|| format!("failed to parse exchange config {}", path.display()))?;
        if config.schema_version != CONFIG_SCHEMA_VERSION {
            bail!(
                "unsupported exchange config schemaVersion {}; expected {}",
                config.schema_version,
                CONFIG_SCHEMA_VERSION
            );
        }
        Self::new(
            repository_root,
            config.assistants,
            config.consecutive_message_limit,
        )
    }

    pub fn validate_mcp_author(&self, author: &str) -> Result<()> {
        if author == "human" {
            bail!("the MCP exchange server cannot write as human");
        }
        if !self.allowed_assistants.contains(author) {
            bail!("exchange author {author:?} is not in the configured allowlist");
        }
        Ok(())
    }

    pub fn list(&self) -> Result<Vec<ThreadSummary>> {
        if !self.ensure_exchange_root(false)? {
            return Ok(Vec::new());
        }
        let mut summaries = fs::read_dir(&self.root)
            .with_context(|| format!("failed to list {}", self.root.display()))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .filter_map(|entry| {
                let thread = entry.file_name().to_string_lossy().into_owned();
                match self.load_messages(&thread) {
                    Ok(messages) if messages.is_empty() => None,
                    Ok(messages) => Some(self.summary(&thread, &messages)),
                    Err(error) => Some(Err(error)),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        summaries.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.thread.cmp(&right.thread))
        });
        Ok(summaries)
    }

    pub fn read(&self, thread: &str, since: u32) -> Result<ThreadView> {
        self.read_with_visibility(thread, since, false)
    }

    pub fn read_operator(&self, thread: &str, since: u32) -> Result<ThreadView> {
        self.read_with_visibility(thread, since, true)
    }

    fn read_with_visibility(
        &self,
        thread: &str,
        since: u32,
        operator_visibility: bool,
    ) -> Result<ThreadView> {
        validate_thread_id(thread)?;
        let messages = self.load_messages(thread)?;
        if messages.is_empty() {
            bail!("exchange thread {thread:?} does not exist or has no messages");
        }
        let summary = self.summary(thread, &messages)?;
        let visible = messages
            .iter()
            .filter(|message| message.seq > since)
            .map(|message| {
                let withheld = !operator_visibility
                    && message.kind == MessageKind::Spec
                    && approval_outcome(&messages, message.seq) != Some("approved");
                ReadMessage {
                    schema_version: message.schema_version,
                    thread: message.thread.clone(),
                    seq: message.seq,
                    author: message.author.clone(),
                    kind: message.kind,
                    created_at: message.created_at.clone(),
                    subject: message.subject.clone(),
                    body: (!withheld).then(|| message.body.clone()),
                    replies_to: message.replies_to,
                    refs: message.refs.clone(),
                    answered_by: message.answered_by.clone(),
                    outcome: message.outcome.clone(),
                    withheld: withheld.then(|| "awaiting approval".to_owned()),
                }
            })
            .collect();
        Ok(ThreadView {
            thread: thread.to_owned(),
            subject: summary.subject,
            state: summary.state,
            waiting_on: summary.waiting_on,
            messages: visible,
        })
    }

    pub fn post(
        &self,
        author: &str,
        origin: PostOrigin,
        request: PostRequest,
    ) -> Result<PostResult> {
        self.validate_author(author, origin)?;
        validate_message_fields(&request)?;
        if request.kind == MessageKind::Approval {
            bail!("approval is only postable through exchange approve or reject");
        }
        self.validate_refs(request.thread.as_deref(), &request.refs)?;

        let (thread, created_thread) = match request.thread.as_deref() {
            Some(thread) => {
                validate_thread_id(thread)?;
                self.ensure_thread_directory(thread, true)?;
                (thread.to_owned(), false)
            }
            None => (self.allocate_thread(&request.subject)?, true),
        };
        let _lock = self.lock_thread(&thread, true)?;
        self.validate_refs(Some(&thread), &request.refs)?;
        let existing = self.load_messages_unlocked(&thread)?;
        if created_thread && !existing.is_empty() {
            bail!("new exchange thread unexpectedly contains messages");
        }
        self.validate_against_thread(author, &request, &existing)?;
        self.write_message_unlocked(author, &thread, request, &existing)
    }

    pub fn resolve(
        &self,
        author: &str,
        origin: PostOrigin,
        thread: &str,
        outcome: VerdictOutcome,
        body: String,
        refs: Vec<MessageRef>,
    ) -> Result<PostResult> {
        self.post(
            author,
            origin,
            PostRequest {
                kind: MessageKind::Verdict,
                subject: "Thread verdict".to_owned(),
                body,
                thread: Some(thread.to_owned()),
                refs,
                replies_to: None,
                answered_by: None,
                outcome: Some(outcome.as_str().to_owned()),
            },
        )
    }

    pub fn approve(
        &self,
        thread: &str,
        seq: Option<u32>,
        approved: bool,
        body: String,
    ) -> Result<PostResult> {
        validate_thread_id(thread)?;
        self.ensure_thread_directory(thread, false)?
            .with_context(|| format!("exchange thread {thread:?} does not exist"))?;
        let _lock = self.lock_thread(thread, true)?;
        let messages = self.load_messages_unlocked(thread)?;
        let target = match seq {
            Some(seq) => messages
                .iter()
                .find(|message| message.seq == seq)
                .context("approval target sequence does not exist")?,
            None => messages
                .iter()
                .rev()
                .find(|message| {
                    matches!(message.kind, MessageKind::Spec | MessageKind::Verdict)
                        && approval_outcome(&messages, message.seq).is_none()
                })
                .context("thread has no unapproved spec or verdict")?,
        };
        if !matches!(target.kind, MessageKind::Spec | MessageKind::Verdict) {
            bail!("approval target must be a spec or verdict");
        }
        let action = if approved { "approved" } else { "rejected" };
        if body.len() > MAX_BODY_BYTES {
            bail!(
                "exchange body exceeds 16 KiB; put long content under attachments/ and cite it as an artifact ref"
            );
        }
        self.write_message_unlocked(
            "human",
            thread,
            PostRequest {
                kind: MessageKind::Approval,
                subject: format!("{action}: {}", target.subject),
                body,
                thread: Some(thread.to_owned()),
                refs: Vec::new(),
                replies_to: Some(target.seq),
                answered_by: None,
                outcome: Some(action.to_owned()),
            },
            &messages,
        )
    }

    fn validate_author(&self, author: &str, origin: PostOrigin) -> Result<()> {
        if !valid_author(author) {
            bail!("exchange author must be a lowercase identifier");
        }
        match origin {
            PostOrigin::Mcp => self.validate_mcp_author(author),
            PostOrigin::Cli if author == "human" => Ok(()),
            PostOrigin::Cli => bail!("operator CLI messages must be authored by human"),
        }
    }

    fn validate_against_thread(
        &self,
        author: &str,
        request: &PostRequest,
        messages: &[ExchangeMessage],
    ) -> Result<()> {
        if let Some(replies_to) = request.replies_to
            && !messages.iter().any(|message| message.seq == replies_to)
        {
            bail!("repliesTo sequence {replies_to} does not exist");
        }
        if author != "human" && trailing_non_human(messages) >= self.consecutive_limit {
            bail!(
                "thread is escalated after {} consecutive non-human messages; a human message is required",
                self.consecutive_limit
            );
        }
        if request.kind == MessageKind::Verdict {
            let last_non_verdict = messages
                .iter()
                .rev()
                .find(|message| message.kind != MessageKind::Verdict)
                .context("a verdict requires an earlier non-verdict message")?;
            if last_non_verdict.author == author {
                bail!("a thread cannot be closed by the side that last spoke");
            }
        }
        Ok(())
    }

    fn validate_refs(&self, thread: Option<&str>, refs: &[MessageRef]) -> Result<()> {
        for reference in refs {
            match reference.kind {
                RefKind::Url => {}
                RefKind::Commit => {
                    let output = Command::new("git")
                        .args(["cat-file", "-t", &reference.value])
                        .current_dir(&self.repository_root)
                        .output()
                        .context("failed to validate exchange commit ref")?;
                    if !output.status.success() {
                        bail!("commit ref {:?} does not resolve", reference.value);
                    }
                }
                RefKind::Code => {
                    let path = code_ref_path(&reference.value)?;
                    self.require_existing_relative(path, None)?;
                }
                RefKind::Artifact => {
                    let path = Path::new(&reference.value);
                    let harness_artifacts = Path::new("harness/artifacts");
                    let thread_attachments = thread.map(|thread| {
                        Path::new(".overmesh/exchange")
                            .join(thread)
                            .join("attachments")
                    });
                    let allowed_root = if path.starts_with(harness_artifacts) {
                        harness_artifacts
                    } else if let Some(attachments) = thread_attachments
                        .as_deref()
                        .filter(|attachments| path.starts_with(attachments))
                    {
                        attachments
                    } else {
                        bail!(
                            "artifact ref {:?} must be under harness/artifacts/ or the thread attachments/",
                            reference.value
                        );
                    };
                    self.require_existing_relative(path, Some(allowed_root))?;
                }
                RefKind::Record => {
                    self.require_existing_relative(
                        Path::new(&reference.value),
                        Some(Path::new("docs/adr")),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn require_existing_relative(&self, path: &Path, prefix: Option<&Path>) -> Result<()> {
        if !safe_relative_path(path) {
            bail!("exchange ref path {:?} must be repository-relative", path);
        }
        if prefix.is_some_and(|prefix| !path.starts_with(prefix)) {
            bail!(
                "exchange ref path {:?} is outside {}",
                path,
                prefix.unwrap().display()
            );
        }
        let absolute = self.repository_root.join(path);
        if !absolute.exists() {
            bail!("exchange ref path {:?} does not exist", path);
        }
        let canonical_path = absolute
            .canonicalize()
            .with_context(|| format!("failed to canonicalize ref {}", absolute.display()))?;
        let allowed_root = if let Some(prefix) = prefix {
            reject_symlink_components(&self.repository_root, prefix)?;
            self.repository_root.join(prefix)
        } else {
            self.repository_root.clone()
        }
        .canonicalize()
        .context("failed to canonicalize exchange ref root")?;
        if !allowed_root.starts_with(&self.repository_root) {
            bail!("exchange ref root escapes the repository");
        }
        if !canonical_path.starts_with(&allowed_root) {
            bail!("exchange ref path {:?} escapes the repository", path);
        }
        Ok(())
    }

    fn allocate_thread(&self, subject: &str) -> Result<String> {
        self.ensure_exchange_root(true)?;
        let next = self
            .existing_thread_numbers()?
            .into_iter()
            .max()
            .unwrap_or(0)
            + 1;
        let slug = slug(subject);
        for number in next.. {
            let thread = format!("{number:04}-{slug}");
            match fs::create_dir(self.root.join(&thread)) {
                Ok(()) => return Ok(thread),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to allocate exchange thread {thread}"));
                }
            }
        }
        unreachable!("unbounded thread allocation loop")
    }

    fn existing_thread_numbers(&self) -> Result<Vec<u32>> {
        if !self.ensure_exchange_root(false)? {
            return Ok(Vec::new());
        }
        Ok(fs::read_dir(&self.root)
            .with_context(|| format!("failed to list {}", self.root.display()))?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.split_once('-'))
                    .and_then(|(number, _)| number.parse().ok())
            })
            .collect())
    }

    fn write_message_unlocked(
        &self,
        author: &str,
        thread: &str,
        request: PostRequest,
        existing: &[ExchangeMessage],
    ) -> Result<PostResult> {
        let directory = self
            .ensure_thread_directory(thread, false)?
            .with_context(|| format!("exchange thread {thread:?} does not exist"))?;
        let seq = existing
            .iter()
            .map(|message| message.seq)
            .max()
            .unwrap_or(0)
            + 1;
        let reservation = directory.join(format!(".{seq:03}.pending"));
        if reservation
            .symlink_metadata()
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            bail!(
                "exchange reservation {} must not be a symlink",
                reservation.display()
            );
        }
        if reservation.exists() {
            fs::remove_file(&reservation).with_context(|| {
                format!(
                    "failed to remove stale exchange reservation {}",
                    reservation.display()
                )
            })?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&reservation)
            .with_context(|| format!("failed to create {}", reservation.display()))?;
        let path = directory.join(format!("{seq:03}-{author}.json"));
        let message = ExchangeMessage {
            schema_version: SCHEMA_VERSION,
            thread: thread.to_owned(),
            seq,
            author: author.to_owned(),
            kind: request.kind,
            created_at: now_rfc3339()?,
            subject: request.subject,
            body: request.body,
            replies_to: request.replies_to,
            refs: request.refs,
            answered_by: request.answered_by,
            outcome: request.outcome,
        };
        let bytes = serde_json::to_vec_pretty(&message)?;
        file.write_all(&bytes)
            .and_then(|()| file.write_all(b"\n"))
            .with_context(|| format!("failed to write {}", reservation.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to sync {}", reservation.display()))?;
        drop(file);
        if let Err(error) = fs::hard_link(&reservation, &path) {
            let _ = fs::remove_file(&reservation);
            return Err(error)
                .with_context(|| format!("failed to publish exchange message {}", path.display()));
        }
        fs::remove_file(&reservation)
            .with_context(|| format!("failed to remove {}", reservation.display()))?;
        if let Err(error) = self.stage(&path) {
            let _ = fs::remove_file(&path);
            return Err(error);
        }
        let messages = match self.load_messages_unlocked(thread) {
            Ok(messages) => messages,
            Err(error) => {
                let _ = self.unstage(&path);
                let _ = fs::remove_file(&path);
                return Err(error).context("published exchange message failed validation");
            }
        };
        Ok(PostResult {
            thread: thread.to_owned(),
            seq,
            state: derive_state(&messages, self.consecutive_limit),
        })
    }

    fn stage(&self, path: &Path) -> Result<()> {
        let relative = path
            .strip_prefix(&self.repository_root)
            .context("exchange message is outside the repository")?;
        for attempt in 0..50 {
            let output = Command::new("git")
                .arg("add")
                .arg("--")
                .arg(relative)
                .current_dir(&self.repository_root)
                .output()
                .context("failed to stage exchange message")?;
            if output.status.success() {
                return Ok(());
            }
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("index.lock") && attempt < 49 {
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            bail!("git add failed: {}", stderr.trim());
        }
        unreachable!("bounded git staging retry loop")
    }

    fn unstage(&self, path: &Path) -> Result<()> {
        let relative = path
            .strip_prefix(&self.repository_root)
            .context("exchange message is outside the repository")?;
        let output = Command::new("git")
            .args(["reset", "--quiet", "--"])
            .arg(relative)
            .current_dir(&self.repository_root)
            .output()
            .context("failed to unstage exchange message")?;
        if !output.status.success() {
            bail!(
                "git reset failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }

    fn ensure_exchange_root(&self, create: bool) -> Result<bool> {
        let metadata_root = self.repository_root.join(".overmesh");
        if !ensure_directory(&metadata_root, create, "exchange metadata root")? {
            return Ok(false);
        }
        ensure_directory(&self.root, create, "exchange root")
    }

    fn ensure_thread_directory(&self, thread: &str, create: bool) -> Result<Option<PathBuf>> {
        validate_thread_id(thread)?;
        if !self.ensure_exchange_root(create)? {
            return Ok(None);
        }
        let directory = self.root.join(thread);
        if !ensure_directory(&directory, create, "exchange thread")? {
            return Ok(None);
        }
        let canonical_root = self
            .root
            .canonicalize()
            .context("failed to canonicalize exchange root")?;
        let canonical_directory = directory
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", directory.display()))?;
        if canonical_directory.parent() != Some(canonical_root.as_path()) {
            bail!(
                "exchange thread {} escapes the exchange root",
                directory.display()
            );
        }
        Ok(Some(directory))
    }

    fn lock_thread(&self, thread: &str, exclusive: bool) -> Result<File> {
        fs::create_dir_all(&self.lock_root)
            .with_context(|| format!("failed to create {}", self.lock_root.display()))?;
        let path = self.lock_root.join(format!("{thread}.lock"));
        if path
            .symlink_metadata()
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            bail!("exchange lock {} must not be a symlink", path.display());
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("failed to open exchange lock {}", path.display()))?;
        if exclusive {
            file.lock()
        } else {
            file.lock_shared()
        }
        .with_context(|| format!("failed to acquire exchange lock {}", path.display()))?;
        Ok(file)
    }

    fn load_messages(&self, thread: &str) -> Result<Vec<ExchangeMessage>> {
        if self.ensure_thread_directory(thread, false)?.is_none() {
            return Ok(Vec::new());
        }
        let _lock = self.lock_thread(thread, false)?;
        self.load_messages_unlocked(thread)
    }

    fn load_messages_unlocked(&self, thread: &str) -> Result<Vec<ExchangeMessage>> {
        let directory = self
            .ensure_thread_directory(thread, false)?
            .with_context(|| format!("exchange thread {thread:?} does not exist"))?;
        let mut messages = Vec::new();
        let mut sequences = BTreeSet::new();
        for entry in fs::read_dir(&directory)
            .with_context(|| format!("failed to read exchange thread {thread}"))?
        {
            let entry = entry?;
            if !entry.file_type()?.is_file() || entry.path().extension() != Some(OsStr::new("json"))
            {
                continue;
            }
            let message: ExchangeMessage = serde_json::from_slice(
                &fs::read(entry.path())
                    .with_context(|| format!("failed to read {}", entry.path().display()))?,
            )
            .with_context(|| format!("failed to parse {}", entry.path().display()))?;
            if message.schema_version != SCHEMA_VERSION
                || message.thread != thread
                || !valid_author(&message.author)
            {
                bail!(
                    "exchange message {} has inconsistent identity",
                    entry.path().display()
                );
            }
            let expected_name = format!("{:03}-{}.json", message.seq, message.author);
            if entry.file_name() != OsStr::new(&expected_name) || !sequences.insert(message.seq) {
                bail!(
                    "exchange message {} has inconsistent sequence",
                    entry.path().display()
                );
            }
            messages.push(message);
        }
        messages.sort_by_key(|message| message.seq);
        self.validate_history(thread, &messages)?;
        Ok(messages)
    }

    fn validate_history(&self, thread: &str, messages: &[ExchangeMessage]) -> Result<()> {
        let mut history: Vec<ExchangeMessage> = Vec::with_capacity(messages.len());
        for (index, message) in messages.iter().enumerate() {
            let expected_seq = u32::try_from(index + 1).context("exchange sequence overflow")?;
            if message.seq != expected_seq {
                bail!(
                    "exchange thread {thread:?} has a sequence gap before {}",
                    message.seq
                );
            }
            if message.author != "human" && !self.allowed_assistants.contains(&message.author) {
                bail!(
                    "exchange message {} has unknown author {:?}",
                    message.seq,
                    message.author
                );
            }
            if message.kind == MessageKind::Approval {
                if message.subject.trim().is_empty()
                    || message.body.len() > MAX_BODY_BYTES
                    || message.author != "human"
                    || message.replies_to.is_none()
                    || !matches!(message.outcome.as_deref(), Some("approved" | "rejected"))
                    || !message.refs.is_empty()
                    || message.answered_by.is_some()
                {
                    bail!(
                        "exchange approval {} violates operator approval invariants",
                        message.seq
                    );
                }
                let target = history
                    .iter()
                    .find(|candidate| Some(candidate.seq) == message.replies_to)
                    .context("exchange approval target does not exist earlier in the thread")?;
                if !matches!(target.kind, MessageKind::Spec | MessageKind::Verdict) {
                    bail!("exchange approval target must be a spec or verdict");
                }
            } else {
                validate_message_fields(&PostRequest {
                    kind: message.kind,
                    subject: message.subject.clone(),
                    body: message.body.clone(),
                    thread: Some(thread.to_owned()),
                    refs: message.refs.clone(),
                    replies_to: message.replies_to,
                    answered_by: message.answered_by.clone(),
                    outcome: message.outcome.clone(),
                })?;
                self.validate_against_thread(
                    &message.author,
                    &PostRequest {
                        kind: message.kind,
                        subject: message.subject.clone(),
                        body: message.body.clone(),
                        thread: Some(thread.to_owned()),
                        refs: message.refs.clone(),
                        replies_to: message.replies_to,
                        answered_by: message.answered_by.clone(),
                        outcome: message.outcome.clone(),
                    },
                    &history,
                )?;
            }
            history.push(message.clone());
        }
        Ok(())
    }

    fn summary(&self, thread: &str, messages: &[ExchangeMessage]) -> Result<ThreadSummary> {
        let first = messages
            .first()
            .context("exchange thread has no messages")?;
        let last = messages.last().context("exchange thread has no messages")?;
        let state = derive_state(messages, self.consecutive_limit);
        Ok(ThreadSummary {
            thread: thread.to_owned(),
            subject: first.subject.clone(),
            state,
            waiting_on: waiting_on(messages, state, &self.allowed_assistants),
            messages: messages.len(),
            last_author: last.author.clone(),
            updated_at: last.created_at.clone(),
        })
    }
}

fn ensure_directory(path: &Path, create: bool, label: &str) -> Result<bool> {
    match path.symlink_metadata() {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("{label} {} must not be a symlink", path.display());
            }
            if !metadata.is_dir() {
                bail!("{label} {} must be a directory", path.display());
            }
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !create => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => match fs::create_dir(path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                ensure_directory(path, false, label)
            }
            Err(error) => {
                Err(error).with_context(|| format!("failed to create {label} {}", path.display()))
            }
        },
        Err(error) => Err(error).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

fn reject_symlink_components(base: &Path, relative: &Path) -> Result<()> {
    let mut current = base.to_owned();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            bail!("exchange ref root must be repository-relative");
        };
        current.push(component);
        let metadata = current.symlink_metadata().with_context(|| {
            format!("failed to inspect exchange ref root {}", current.display())
        })?;
        if metadata.file_type().is_symlink() {
            bail!(
                "exchange ref root component {} must not be a symlink",
                current.display()
            );
        }
    }
    Ok(())
}

fn git_path(repository_root: &Path, name: &str) -> Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--git-path", name])
        .current_dir(repository_root)
        .output()
        .context("failed to resolve Git internal path")?;
    if !output.status.success() {
        bail!(
            "git rev-parse --git-path failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let value = String::from_utf8(output.stdout)
        .context("Git internal path is not UTF-8")?
        .trim()
        .to_owned();
    if value.is_empty() {
        bail!("Git returned an empty internal path");
    }
    let path = PathBuf::from(value);
    Ok(if path.is_absolute() {
        path
    } else {
        repository_root.join(path)
    })
}

fn validate_message_fields(request: &PostRequest) -> Result<()> {
    if request.subject.trim().is_empty() {
        bail!("exchange message subject must be non-empty");
    }
    if request.body.len() > MAX_BODY_BYTES {
        bail!(
            "exchange body exceeds 16 KiB; put long content under attachments/ and cite it as an artifact ref"
        );
    }
    let has_non_url = request
        .refs
        .iter()
        .any(|reference| reference.kind != RefKind::Url);
    if request.kind != MessageKind::Verdict && request.outcome.is_some() {
        bail!("only verdict and approval messages may set outcome");
    }
    match request.kind {
        MessageKind::Finding if !has_non_url => {
            bail!("a finding requires at least one non-url ref")
        }
        MessageKind::Question
            if request
                .answered_by
                .as_deref()
                .is_none_or(|value| value.trim().is_empty()) =>
        {
            bail!("a question requires a non-empty answeredBy")
        }
        MessageKind::Correction if request.replies_to.is_none() || !has_non_url => {
            bail!("a correction requires repliesTo and at least one non-url ref")
        }
        MessageKind::Report
            if !request.refs.iter().any(|reference| {
                matches!(
                    reference.kind,
                    RefKind::Code | RefKind::Artifact | RefKind::Record
                )
            }) =>
        {
            bail!("a report requires at least one changed-file ref")
        }
        MessageKind::Verdict
            if !has_non_url
                || request.outcome.as_deref().is_none_or(|outcome| {
                    !matches!(
                        outcome,
                        "verified" | "not-verified" | "withdrawn" | "superseded"
                    )
                }) =>
        {
            bail!("a verdict requires a non-url ref and a valid outcome")
        }
        MessageKind::Approval => {
            bail!("approval must use the operator approval path")
        }
        _ => {}
    }
    Ok(())
}

fn derive_state(messages: &[ExchangeMessage], limit: usize) -> ThreadState {
    if trailing_non_human(messages) >= limit {
        return ThreadState::Escalated;
    }
    if let Some(gated) = messages
        .iter()
        .rev()
        .find(|message| matches!(message.kind, MessageKind::Spec | MessageKind::Verdict))
    {
        match approval_outcome(messages, gated.seq) {
            None => return ThreadState::AwaitingApproval,
            Some("approved") if gated.kind == MessageKind::Verdict => {
                return ThreadState::Resolved;
            }
            Some("approved" | "rejected") => {}
            Some(_) => unreachable!("loaded approvals have validated outcomes"),
        }
    }
    ThreadState::Open
}

fn approval_outcome(messages: &[ExchangeMessage], seq: u32) -> Option<&str> {
    messages
        .iter()
        .rev()
        .find(|message| message.kind == MessageKind::Approval && message.replies_to == Some(seq))
        .and_then(|message| message.outcome.as_deref())
}

fn trailing_non_human(messages: &[ExchangeMessage]) -> usize {
    messages
        .iter()
        .rev()
        .take_while(|message| message.author != "human")
        .count()
}

fn waiting_on(
    messages: &[ExchangeMessage],
    state: ThreadState,
    allowed_assistants: &BTreeSet<String>,
) -> Option<String> {
    if state == ThreadState::Resolved {
        return None;
    }
    if matches!(
        state,
        ThreadState::AwaitingApproval | ThreadState::Escalated
    ) {
        return Some("human".to_owned());
    }
    let Some(last) = messages.last() else {
        return Some("participants".to_owned());
    };
    if last.author != "human" {
        return Some(
            allowed_assistants
                .iter()
                .find(|author| **author != last.author)
                .cloned()
                .unwrap_or_else(|| "human".to_owned()),
        );
    }
    if let Some(target_author) = last.replies_to.and_then(|seq| {
        messages
            .iter()
            .find(|message| message.seq == seq)
            .map(|message| message.author.as_str())
    }) {
        return Some(
            allowed_assistants
                .iter()
                .find(|author| author.as_str() != target_author)
                .cloned()
                .unwrap_or_else(|| "participants".to_owned()),
        );
    }
    Some("participants".to_owned())
}

fn code_ref_path(value: &str) -> Result<&Path> {
    let path = value
        .rsplit_once(':')
        .filter(|(_, line)| line.parse::<u32>().is_ok())
        .map_or(value, |(path, _)| path);
    if path.is_empty() {
        bail!("code ref path must be non-empty");
    }
    Ok(Path::new(path))
}

fn safe_relative_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn validate_thread_id(thread: &str) -> Result<()> {
    let Some((number, slug)) = thread.split_once('-') else {
        bail!("exchange thread must be <nnnn>-<slug>");
    };
    if number.len() < 4
        || !number.bytes().all(|byte| byte.is_ascii_digit())
        || slug.is_empty()
        || slug.starts_with('-')
        || slug.ends_with('-')
        || !slug
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        bail!("exchange thread must be <nnnn>-<slug>");
    }
    Ok(())
}

fn valid_author(author: &str) -> bool {
    !author.is_empty()
        && author.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

fn slug(subject: &str) -> String {
    let mut value = String::new();
    let mut pending_separator = false;
    for byte in subject.bytes() {
        if byte.is_ascii_alphanumeric() {
            if pending_separator && !value.is_empty() {
                value.push('-');
            }
            value.push(byte.to_ascii_lowercase() as char);
            pending_separator = false;
        } else {
            pending_separator = true;
        }
        if value.len() >= 64 {
            break;
        }
    }
    value
        .trim_matches('-')
        .to_owned()
        .chars()
        .take(64)
        .collect::<String>()
        .trim_matches('-')
        .to_owned()
        .pipe_if_empty("thread")
}

trait EmptyFallback {
    fn pipe_if_empty(self, fallback: &str) -> String;
}

impl EmptyFallback for String {
    fn pipe_if_empty(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_owned()
        } else {
            self
        }
    }
}

fn now_rfc3339() -> Result<String> {
    OffsetDateTime::now_utc()
        .format(CREATED_AT_FORMAT)
        .context("failed to format exchange timestamp")
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Barrier},
        thread,
    };

    use tempfile::TempDir;

    use super::*;

    fn fixture() -> TempDir {
        let root = tempfile::tempdir().expect("temp repository");
        git(root.path(), &["init", "-q"]);
        fs::create_dir_all(root.path().join("gateway/src")).unwrap();
        fs::write(
            root.path().join("gateway/src/backend.rs"),
            "fn backend() {}\n",
        )
        .unwrap();
        fs::create_dir_all(root.path().join("harness/artifacts")).unwrap();
        fs::write(root.path().join("harness/artifacts/evidence.json"), "{}\n").unwrap();
        fs::create_dir_all(root.path().join("docs/adr")).unwrap();
        fs::write(root.path().join("docs/adr/0001-test.md"), "# Test\n").unwrap();
        root
    }

    fn exchange(root: &TempDir) -> Exchange {
        Exchange::default_for_repository(root.path()).unwrap()
    }

    fn code_ref() -> MessageRef {
        MessageRef {
            kind: RefKind::Code,
            value: "gateway/src/backend.rs:1".to_owned(),
        }
    }

    fn finding(thread: Option<String>, body: &str) -> PostRequest {
        PostRequest {
            kind: MessageKind::Finding,
            subject: "Backend finding".to_owned(),
            body: body.to_owned(),
            thread,
            refs: vec![code_ref()],
            replies_to: None,
            answered_by: None,
            outcome: None,
        }
    }

    #[test]
    fn rejects_invalid_kinds_and_refs_without_writing() {
        let root = fixture();
        let exchange = exchange(&root);
        let error = exchange
            .post(
                "copilot",
                PostOrigin::Mcp,
                PostRequest {
                    refs: Vec::new(),
                    ..finding(None, "finding")
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("requires at least one"));
        assert!(!root.path().join(".overmesh/exchange").exists());

        let error = exchange
            .post(
                "copilot",
                PostOrigin::Mcp,
                PostRequest {
                    refs: vec![MessageRef {
                        kind: RefKind::Code,
                        value: "missing.rs:1".to_owned(),
                    }],
                    ..finding(None, "finding")
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("does not exist"));
        assert!(!root.path().join(".overmesh/exchange").exists());
    }

    #[test]
    fn validates_correction_question_report_and_body_limit() {
        let root = fixture();
        let exchange = exchange(&root);
        let correction = PostRequest {
            kind: MessageKind::Correction,
            subject: "Correction".to_owned(),
            body: "Wrong.".to_owned(),
            thread: None,
            refs: vec![code_ref()],
            replies_to: None,
            answered_by: None,
            outcome: None,
        };
        assert!(
            exchange
                .post("copilot", PostOrigin::Mcp, correction)
                .unwrap_err()
                .to_string()
                .contains("repliesTo")
        );
        let question = PostRequest {
            kind: MessageKind::Question,
            subject: "Question".to_owned(),
            body: "?".to_owned(),
            thread: None,
            refs: Vec::new(),
            replies_to: None,
            answered_by: Some(" ".to_owned()),
            outcome: None,
        };
        assert!(
            exchange
                .post("copilot", PostOrigin::Mcp, question)
                .unwrap_err()
                .to_string()
                .contains("answeredBy")
        );
        let mut oversized = finding(None, &"x".repeat(MAX_BODY_BYTES + 1));
        oversized.subject = "Large".to_owned();
        assert!(
            exchange
                .post("copilot", PostOrigin::Mcp, oversized)
                .unwrap_err()
                .to_string()
                .contains("attachments/")
        );
    }

    #[test]
    fn escalates_at_limit_and_human_resets_counter() {
        let root = fixture();
        let exchange = exchange(&root);
        let first = exchange
            .post("copilot", PostOrigin::Mcp, finding(None, "one"))
            .unwrap();
        for index in 2..=5 {
            let author = if index % 2 == 0 { "claude" } else { "copilot" };
            exchange
                .post(
                    author,
                    PostOrigin::Mcp,
                    finding(Some(first.thread.clone()), &index.to_string()),
                )
                .unwrap();
        }
        assert_eq!(
            exchange.read(&first.thread, 0).unwrap().state,
            ThreadState::Escalated
        );
        assert!(
            exchange
                .post(
                    "claude",
                    PostOrigin::Mcp,
                    finding(Some(first.thread.clone()), "six"),
                )
                .unwrap_err()
                .to_string()
                .contains("human message")
        );
        exchange
            .post(
                "human",
                PostOrigin::Cli,
                finding(Some(first.thread.clone()), "reviewed"),
            )
            .unwrap();
        exchange
            .post(
                "claude",
                PostOrigin::Mcp,
                finding(Some(first.thread), "resumed"),
            )
            .unwrap();
    }

    #[test]
    fn spec_body_is_withheld_until_human_approval() {
        let root = fixture();
        let exchange = exchange(&root);
        let posted = exchange
            .post(
                "copilot",
                PostOrigin::Mcp,
                PostRequest {
                    kind: MessageKind::Spec,
                    subject: "Implement exchange".to_owned(),
                    body: "Secret instruction".to_owned(),
                    thread: None,
                    refs: Vec::new(),
                    replies_to: None,
                    answered_by: None,
                    outcome: None,
                },
            )
            .unwrap();
        let view = exchange.read(&posted.thread, 0).unwrap();
        assert_eq!(view.state, ThreadState::AwaitingApproval);
        assert_eq!(view.messages[0].body, None);
        assert_eq!(
            view.messages[0].withheld.as_deref(),
            Some("awaiting approval")
        );
        exchange
            .approve(&posted.thread, None, true, "Approved".to_owned())
            .unwrap();
        let view = exchange.read(&posted.thread, 0).unwrap();
        assert_eq!(view.messages[0].body.as_deref(), Some("Secret instruction"));
    }

    #[test]
    fn verdict_requires_other_author_and_approval_to_resolve() {
        let root = fixture();
        let exchange = exchange(&root);
        let first = exchange
            .post("copilot", PostOrigin::Mcp, finding(None, "finding"))
            .unwrap();
        let error = exchange
            .resolve(
                "copilot",
                PostOrigin::Mcp,
                &first.thread,
                VerdictOutcome::Verified,
                "Verified".to_owned(),
                vec![code_ref()],
            )
            .unwrap_err();
        assert!(error.to_string().contains("side that last spoke"));
        let verdict = exchange
            .resolve(
                "claude",
                PostOrigin::Mcp,
                &first.thread,
                VerdictOutcome::Verified,
                "Verified".to_owned(),
                vec![code_ref()],
            )
            .unwrap();
        assert_eq!(verdict.state, ThreadState::AwaitingApproval);
        let approved = exchange
            .approve(
                &first.thread,
                Some(verdict.seq),
                true,
                "Approved".to_owned(),
            )
            .unwrap();
        assert_eq!(approved.state, ThreadState::Resolved);
        let view = exchange.read(&first.thread, 0).unwrap();
        assert_eq!(view.state, ThreadState::Resolved);
        assert_eq!(view.waiting_on, None);
    }

    #[test]
    fn concurrent_posts_create_distinct_staged_files() {
        let root = fixture();
        let exchange = Arc::new(exchange(&root));
        let first = exchange
            .post("copilot", PostOrigin::Mcp, finding(None, "one"))
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let handles = ["copilot", "claude"].map(|author| {
            let exchange = Arc::clone(&exchange);
            let barrier = Arc::clone(&barrier);
            let thread = first.thread.clone();
            thread::spawn(move || {
                barrier.wait();
                exchange.post(author, PostOrigin::Mcp, finding(Some(thread), author))
            })
        });
        barrier.wait();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_ne!(results[0].seq, results[1].seq);
        let staged = git_output(root.path(), &["diff", "--cached", "--name-only"]);
        assert_eq!(
            staged
                .lines()
                .filter(|path| path.ends_with(".json"))
                .count(),
            3
        );
    }

    #[test]
    fn concurrent_posts_cannot_cross_escalation_limit() {
        let root = fixture();
        let exchange = Arc::new(exchange(&root));
        let first = exchange
            .post("copilot", PostOrigin::Mcp, finding(None, "one"))
            .unwrap();
        for index in 2..=4 {
            let author = if index % 2 == 0 { "claude" } else { "copilot" };
            exchange
                .post(
                    author,
                    PostOrigin::Mcp,
                    finding(Some(first.thread.clone()), &index.to_string()),
                )
                .unwrap();
        }
        let barrier = Arc::new(Barrier::new(3));
        let handles = ["copilot", "claude"].map(|author| {
            let exchange = Arc::clone(&exchange);
            let barrier = Arc::clone(&barrier);
            let thread = first.thread.clone();
            thread::spawn(move || {
                barrier.wait();
                exchange.post(author, PostOrigin::Mcp, finding(Some(thread), author))
            })
        });
        barrier.wait();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        let view = exchange.read(&first.thread, 0).unwrap();
        assert_eq!(view.messages.len(), 5);
        assert_eq!(view.state, ThreadState::Escalated);
    }

    #[test]
    fn failed_staging_is_never_visible_to_readers() {
        let root = fixture();
        let exchange = Arc::new(exchange(&root));
        let thread_id = "0001-staging-failure";
        fs::create_dir_all(root.path().join(".overmesh/exchange").join(thread_id)).unwrap();
        fs::write(root.path().join(".git/index.lock"), "").unwrap();

        let writer = {
            let exchange = Arc::clone(&exchange);
            thread::spawn(move || {
                exchange.post(
                    "copilot",
                    PostOrigin::Mcp,
                    finding(Some(thread_id.to_owned()), "not staged"),
                )
            })
        };
        let published = root
            .path()
            .join(".overmesh/exchange")
            .join(thread_id)
            .join("001-copilot.json");
        for _ in 0..100 {
            if published.exists() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(published.exists());
        let reader = {
            let exchange = Arc::clone(&exchange);
            thread::spawn(move || exchange.read(thread_id, 0))
        };
        assert!(writer.join().unwrap().is_err());
        assert!(reader.join().unwrap().is_err());
        assert!(!published.exists());
    }

    #[test]
    fn operator_can_read_pending_spec_and_state_reloads_from_files() {
        let root = fixture();
        let exchange = exchange(&root);
        let posted = exchange
            .post(
                "copilot",
                PostOrigin::Mcp,
                PostRequest {
                    kind: MessageKind::Spec,
                    subject: "Inspect before approval".to_owned(),
                    body: "Operator-visible instruction".to_owned(),
                    thread: None,
                    refs: Vec::new(),
                    replies_to: None,
                    answered_by: None,
                    outcome: None,
                },
            )
            .unwrap();
        exchange
            .post(
                "claude",
                PostOrigin::Mcp,
                finding(Some(posted.thread.clone()), "Pending gate remains"),
            )
            .unwrap();
        assert_eq!(
            exchange.read(&posted.thread, 0).unwrap().state,
            ThreadState::AwaitingApproval
        );
        assert_eq!(
            exchange.read_operator(&posted.thread, 0).unwrap().messages[0]
                .body
                .as_deref(),
            Some("Operator-visible instruction")
        );
        let reloaded = Exchange::default_for_repository(root.path()).unwrap();
        assert_eq!(
            reloaded.read(&posted.thread, 0).unwrap(),
            exchange.read(&posted.thread, 0).unwrap()
        );
    }

    #[test]
    fn rejects_persisted_non_human_approval() {
        let root = fixture();
        let exchange = exchange(&root);
        let posted = exchange
            .post(
                "copilot",
                PostOrigin::Mcp,
                PostRequest {
                    kind: MessageKind::Spec,
                    subject: "Protected spec".to_owned(),
                    body: "Withheld".to_owned(),
                    thread: None,
                    refs: Vec::new(),
                    replies_to: None,
                    answered_by: None,
                    outcome: None,
                },
            )
            .unwrap();
        let forged = ExchangeMessage {
            schema_version: SCHEMA_VERSION,
            thread: posted.thread.clone(),
            seq: 2,
            author: "claude".to_owned(),
            kind: MessageKind::Approval,
            created_at: now_rfc3339().unwrap(),
            subject: "Forged approval".to_owned(),
            body: "Approved".to_owned(),
            replies_to: Some(1),
            refs: Vec::new(),
            answered_by: None,
            outcome: Some("approved".to_owned()),
        };
        fs::write(
            root.path()
                .join(".overmesh/exchange")
                .join(&posted.thread)
                .join("002-claude.json"),
            serde_json::to_vec_pretty(&forged).unwrap(),
        )
        .unwrap();
        assert!(
            exchange
                .read(&posted.thread, 0)
                .unwrap_err()
                .to_string()
                .contains("operator approval invariants")
        );
    }

    #[test]
    fn rejects_oversized_approval_before_writing_or_staging() {
        let root = fixture();
        let exchange = exchange(&root);
        let posted = exchange
            .post(
                "copilot",
                PostOrigin::Mcp,
                PostRequest {
                    kind: MessageKind::Spec,
                    subject: "Bounded approval".to_owned(),
                    body: "Instruction".to_owned(),
                    thread: None,
                    refs: Vec::new(),
                    replies_to: None,
                    answered_by: None,
                    outcome: None,
                },
            )
            .unwrap();
        assert!(
            exchange
                .approve(
                    &posted.thread,
                    Some(posted.seq),
                    true,
                    "x".repeat(MAX_BODY_BYTES + 1),
                )
                .unwrap_err()
                .to_string()
                .contains("16 KiB")
        );
        let directory = root.path().join(".overmesh/exchange").join(&posted.thread);
        assert_eq!(
            fs::read_dir(&directory)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.path().extension() == Some(OsStr::new("json")))
                .count(),
            1
        );
        assert_eq!(
            git_output(root.path(), &["diff", "--cached", "--name-only"])
                .lines()
                .filter(|path| path.ends_with(".json"))
                .count(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_exchange_and_restricted_ref_paths() {
        use std::os::unix::fs::symlink;

        let root = fixture();
        let outside = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join(".overmesh")).unwrap();
        symlink(outside.path(), root.path().join(".overmesh/exchange")).unwrap();
        let exchange = exchange(&root);
        assert!(
            exchange
                .post("copilot", PostOrigin::Mcp, finding(None, "escape"))
                .unwrap_err()
                .to_string()
                .contains("must not be a symlink")
        );
        assert!(fs::read_dir(outside.path()).unwrap().next().is_none());

        fs::remove_file(root.path().join(".overmesh/exchange")).unwrap();
        fs::create_dir_all(root.path().join(".overmesh/exchange")).unwrap();
        fs::remove_file(root.path().join("harness/artifacts/evidence.json")).unwrap();
        fs::remove_dir(root.path().join("harness/artifacts")).unwrap();
        fs::write(outside.path().join("external.json"), "{}\n").unwrap();
        symlink(outside.path(), root.path().join("harness/artifacts")).unwrap();
        assert!(
            exchange
                .post(
                    "copilot",
                    PostOrigin::Mcp,
                    PostRequest {
                        refs: vec![MessageRef {
                            kind: RefKind::Artifact,
                            value: "harness/artifacts/external.json".to_owned(),
                        }],
                        ..finding(None, "symlinked artifact root")
                    },
                )
                .unwrap_err()
                .to_string()
                .contains("must not be a symlink")
        );

        fs::remove_file(root.path().join("harness/artifacts")).unwrap();
        fs::create_dir(root.path().join("harness/artifacts")).unwrap();
        symlink(
            root.path().join("docs/adr"),
            root.path().join("harness/artifacts/escaped"),
        )
        .unwrap();
        assert!(
            exchange
                .post(
                    "copilot",
                    PostOrigin::Mcp,
                    PostRequest {
                        refs: vec![MessageRef {
                            kind: RefKind::Artifact,
                            value: "harness/artifacts/escaped/0001-test.md".to_owned(),
                        }],
                        ..finding(None, "escaped artifact")
                    },
                )
                .unwrap_err()
                .to_string()
                .contains("escapes the repository")
        );
    }

    #[test]
    fn mcp_author_cannot_be_human() {
        let root = fixture();
        let exchange = exchange(&root);
        assert!(
            exchange
                .validate_mcp_author("human")
                .unwrap_err()
                .to_string()
                .contains("cannot write as human")
        );
    }

    #[test]
    fn loads_assistants_and_limit_from_committed_config() {
        let root = fixture();
        let config_path = root.path().join(".overmesh/exchange/config.json");
        fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        fs::write(
            &config_path,
            r#"{
                "schemaVersion": 1,
                "assistants": ["reviewer", "writer"],
                "consecutiveMessageLimit": 1
            }"#,
        )
        .unwrap();
        let exchange = Exchange::configured_for_repository(root.path()).unwrap();
        exchange.validate_mcp_author("reviewer").unwrap();
        assert!(exchange.validate_mcp_author("copilot").is_err());
        let posted = exchange
            .post("writer", PostOrigin::Mcp, finding(None, "one"))
            .unwrap();
        assert_eq!(posted.state, ThreadState::Escalated);
        assert!(
            exchange
                .post(
                    "reviewer",
                    PostOrigin::Mcp,
                    finding(Some(posted.thread), "two"),
                )
                .unwrap_err()
                .to_string()
                .contains("human message")
        );
    }

    fn git(root: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(root)
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn git_output(root: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(root)
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap()
    }
}
