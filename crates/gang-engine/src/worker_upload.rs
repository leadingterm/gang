//! Output collection, hashing, CAS upload, and `ActionCache` publication (`WP E4`).
//!
//! Collects declared REAPI outputs (`output_paths` for v2.1+, `output_files` and
//! `output_directories` otherwise) along with captured `stdout`/`stderr`, rebases absolute
//! internal symlinks relative to their parent from a once-canonicalized action root, hashes files
//! with bounded parallelism (`hash_concurrency`), builds deterministic `Tree` protos, inserts
//! outputs into [`DiskCas`] while accounting `cas_shared_bytes`, uploads missing blobs to the slow
//! store through [`Tiered`], and writes cacheable results to [`ActionStore`].

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read as _};
use std::num::NonZeroUsize;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{FileExt as _, MetadataExt as _, PermissionsExt as _};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use gang_cas::{BlobMode, BlockingScope, CacheKey, DiskCas, Tiered, WriteOutcome, blob_path};
use gang_clock::{Clock, Timestamp};
use gang_config::WorkerConfig;
use gang_reapi::build::bazel::remote::execution::v2 as reapi;
use gang_reapi::to_proto;
use gang_spi::{ActionStore, BlobStream, ContentStore, PortError};
use gang_types::Digest;
use prost::Message as _;
use rustix::fs::{self as fd_fs, AtFlags, Dir, FileType, Mode, OFlags};
use sha2::{Digest as _, Sha256};

/// Streaming read buffer size used for hashing and uploading output files.
const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// Maximum descriptor-anchored output directory traversal depth.
const MAX_OUTPUT_DEPTH: usize = 256;

/// Default concurrent slow-store blob uploads when not overridden.
const DEFAULT_UPLOAD_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(64).expect("64 is non-zero");

/// Configuration governing [`upload_outputs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadConfig {
    /// Maximum concurrent CPU-bound file hashing tasks (`min(vCPUs, 4)` by default).
    pub hash_concurrency: NonZeroUsize,
    /// Maximum concurrent slow-store blob uploads.
    pub upload_concurrency: NonZeroUsize,
    /// Maximum `stdout`/`stderr` byte length to inline into `ActionResult` (`0` = always upload).
    pub inline_stdio_max_bytes: u64,
}

impl Default for UploadConfig {
    fn default() -> Self {
        let defaults = WorkerConfig::default();
        Self::from_worker_config(&defaults)
    }
}

impl UploadConfig {
    /// Builds an [`UploadConfig`] from [`WorkerConfig`].
    #[must_use]
    pub fn from_worker_config(config: &WorkerConfig) -> Self {
        Self {
            hash_concurrency: config.effective_hash_concurrency(),
            upload_concurrency: config.download_concurrency.max(DEFAULT_UPLOAD_CONCURRENCY),
            inline_stdio_max_bytes: 0,
        }
    }
}

/// Captured standard output or standard error from an executed action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapturedOutput {
    /// Output bytes already buffered in memory.
    Bytes(Bytes),
    /// Path to a temporary file holding the captured stream.
    File(PathBuf),
}

/// Execution lifecycle timestamps and worker identity recorded into `ExecutedActionMetadata`.
#[derive(Debug, Clone, Default)]
pub struct ExecutionTiming {
    /// Identifier of the worker executing the attempt.
    pub worker: String,
    /// When the operation entered the scheduler queue.
    pub queued: Option<Timestamp>,
    /// When the worker received the attempt assignment.
    pub worker_start: Option<Timestamp>,
    /// When input staging started.
    pub input_fetch_start: Option<Timestamp>,
    /// When input staging completed.
    pub input_fetch_completed: Option<Timestamp>,
    /// When action process execution started.
    pub execution_start: Option<Timestamp>,
    /// When action process execution finished.
    pub execution_completed: Option<Timestamp>,
}

/// Inputs required to collect, hash, and publish an executed attempt's outputs.
#[derive(Debug)]
pub struct UploadInput<'a> {
    /// Digest of the executed REAPI `Action`.
    pub action_digest: Digest,
    /// Whether `ActionCache` publication is forbidden for this attempt.
    pub do_not_cache: bool,
    /// Process exit code reported by the sandbox.
    pub exit_code: i32,
    /// Decoded REAPI `Command` specifying declared outputs and format preferences.
    pub command: &'a reapi::Command,
    /// Action input/output root (`<work>/<attempt_id>/work`).
    pub work_dir: &'a Path,
    /// Command working directory (`<work>/<attempt_id>/work/<cwd>`).
    pub working_dir: &'a Path,
    /// Captured `stdout`, if any.
    pub stdout: Option<CapturedOutput>,
    /// Captured `stderr`, if any.
    pub stderr: Option<CapturedOutput>,
    /// Worker and execution timestamps for `ExecutedActionMetadata`.
    pub timing: ExecutionTiming,
}

/// Result of uploading an attempt's outputs and conditionally publishing to `ActionCache`.
#[derive(Debug, Clone, PartialEq)]
pub struct UploadOutcome {
    /// Constructed REAPI v2 `ActionResult`.
    pub action_result: reapi::ActionResult,
    /// Logical bytes of collected outputs whose inode is shared with [`DiskCas`].
    pub cas_shared_bytes: u64,
    /// Whether `ActionStore::put` was performed (`exit_code == 0 && !do_not_cache`).
    pub cached_action_result: bool,
}

/// Errors produced while collecting, hashing, or uploading action outputs.
#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    /// A declared output path in `Command` was invalid or escaped the working directory.
    #[error("invalid declared output path `{path}`: {reason}")]
    InvalidPath {
        /// Rejected path string.
        path: String,
        /// Explanation of why the path was rejected.
        reason: &'static str,
    },
    /// A declared output existed on disk with an incompatible file type (`FAILED_PRECONDITION`).
    #[error("output `{path}` was {actual}, expected {expected}")]
    OutputKindMismatch {
        /// Declared output path.
        path: String,
        /// Expected type (`file`, `directory`, or `non-special`).
        expected: &'static str,
        /// Observed filesystem type.
        actual: &'static str,
    },
    /// A symlink target could not be represented as valid UTF-8 for REAPI.
    #[error("invalid symlink target at `{}`: {reason}", path.display())]
    InvalidSymlinkTarget {
        /// Path of the symlink on disk.
        path: PathBuf,
        /// Explanation of the invalid target.
        reason: &'static str,
    },
    /// Converting a digest to REAPI protobuf overflowed `i64`.
    #[error("digest conversion error: {0}")]
    DigestConversion(#[from] gang_reapi::DigestConversionError),
    /// A filesystem operation failed while inspecting or reading outputs.
    #[error("filesystem error at `{}`: {source}", path.display())]
    Io {
        /// Path associated with the I/O failure.
        path: PathBuf,
        /// Underlying OS error.
        #[source]
        source: io::Error,
    },
    /// A CAS or `ActionCache` store operation failed.
    #[error("store failure: {0}")]
    Store(#[from] PortError),
    /// Upload was cancelled cooperatively before completing.
    #[error("output upload cancelled")]
    Cancelled,
}

#[derive(Debug)]
struct RootContext {
    working_fd: OwnedFd,
    canonical_work_dir: PathBuf,
    lexical_work_dir: PathBuf,
    cwd_rel_to_work: PathBuf,
}

impl RootContext {
    fn new(work_dir: &Path, working_dir: &Path) -> Result<Self, UploadError> {
        let canonical_work_dir = fs::canonicalize(work_dir).map_err(|source| UploadError::Io {
            path: work_dir.to_path_buf(),
            source,
        })?;
        let lexical_work_dir = make_absolute_lexical(work_dir)?;
        // Only the trusted worker root may contain host-level aliases. Every action-controlled
        // component below the opened work root is resolved without following symlinks.
        let work_fd = fd_fs::open(work_dir, directory_flags(), Mode::empty())
            .map_err(|source| output_io(work_dir, source))?;
        let cwd_rel_to_work = working_dir
            .strip_prefix(work_dir)
            .map_err(|_| UploadError::InvalidPath {
                path: working_dir.display().to_string(),
                reason: "working directory is outside work root",
            })?
            .to_path_buf();
        let working_fd = open_directory_path(&work_fd, &cwd_rel_to_work)?;
        Ok(Self {
            working_fd,
            canonical_work_dir,
            lexical_work_dir,
            cwd_rel_to_work,
        })
    }

    fn rebase_symlink_target(
        &self,
        link_abs: &Path,
        link_rel_to_cwd: &Path,
        raw_target: &Path,
    ) -> Result<String, UploadError> {
        let target_str = raw_target
            .to_str()
            .ok_or_else(|| UploadError::InvalidSymlinkTarget {
                path: link_abs.to_path_buf(),
                reason: "symlink target is not valid UTF-8",
            })?;
        let target_path = Path::new(target_str);
        if !target_path.is_absolute() {
            return Ok(target_str.to_owned());
        }

        let normalized_abs = normalize_lexical_absolute(target_path);
        let rel_to_work = normalized_abs
            .strip_prefix(&self.canonical_work_dir)
            .or_else(|_| normalized_abs.strip_prefix(&self.lexical_work_dir))
            .ok()
            .map(Path::to_path_buf)
            .or_else(|| {
                canonicalize_longest_existing_prefix(&normalized_abs).and_then(|canon| {
                    canon
                        .strip_prefix(&self.canonical_work_dir)
                        .ok()
                        .map(Path::to_path_buf)
                })
            });

        let Some(target_rel_to_work) = rel_to_work else {
            return Ok(target_str.to_owned());
        };

        let link_rel_to_work = join_rel(&self.cwd_rel_to_work, link_rel_to_cwd);
        let link_parent = link_rel_to_work.parent().unwrap_or_else(|| Path::new(""));
        Ok(relative_path_between(link_parent, &target_rel_to_work))
    }
}

#[derive(Debug, Clone)]
struct CollectedFile {
    abs_path: PathBuf,
    executable: bool,
    size: u64,
    dev: u64,
    ino: u64,
    digest: Digest,
}

#[derive(Debug)]
struct OpenedFile {
    metadata: CollectedFile,
    handle: Arc<File>,
}

struct FileCollector<'a, F, S> {
    tiered: &'a Tiered<F, S>,
    config: &'a UploadConfig,
    blocking: &'a BlockingScope,
    #[cfg(test)]
    before_hash: Option<Arc<dyn Fn() + Send + Sync>>,
    files: Vec<CollectedFile>,
    pending: Vec<OpenedFile>,
    counted_inodes: HashSet<(u64, u64)>,
    cas_shared_bytes: u64,
}

impl<'a, F, S> FileCollector<'a, F, S>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    fn new(
        tiered: &'a Tiered<F, S>,
        config: &'a UploadConfig,
        blocking: &'a BlockingScope,
        #[cfg(test)] before_hash: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Self {
        Self {
            tiered,
            config,
            blocking,
            #[cfg(test)]
            before_hash,
            files: Vec::new(),
            pending: Vec::with_capacity(config.hash_concurrency.get()),
            counted_inodes: HashSet::new(),
            cas_shared_bytes: 0,
        }
    }

    async fn push(&mut self, path: PathBuf, handle: File) -> Result<usize, UploadError> {
        if self.blocking.is_cancelled() {
            return Err(UploadError::Cancelled);
        }
        let meta = handle
            .metadata()
            .map_err(|source| output_io(&path, source))?;
        let index = self.files.len() + self.pending.len();
        self.pending.push(OpenedFile {
            metadata: CollectedFile {
                abs_path: path,
                executable: meta.permissions().mode() & 0o111 != 0,
                size: meta.len(),
                dev: meta.dev(),
                ino: meta.ino(),
                digest: Digest::EMPTY,
            },
            handle: Arc::new(handle),
        });
        if self.pending.len() == self.config.hash_concurrency.get() {
            self.flush().await?;
        }
        Ok(index)
    }

    async fn flush(&mut self) -> Result<(), UploadError> {
        if self.blocking.is_cancelled() {
            return Err(UploadError::Cancelled);
        }
        hash_files_parallel(
            &mut self.pending,
            self.config.hash_concurrency,
            self.blocking,
            #[cfg(test)]
            self.before_hash.clone(),
        )
        .await?;
        let shared = insert_outputs_into_disk_cas(
            self.tiered.disk(),
            &self.pending,
            &mut self.counted_inodes,
            self.blocking,
        )
        .await?;
        self.cas_shared_bytes = self.cas_shared_bytes.saturating_add(shared);
        upload_missing_to_slow(
            self.tiered,
            &self.pending,
            &[],
            self.config.upload_concurrency,
            self.blocking,
        )
        .await?;
        // Only immutable metadata survives a batch, including when DiskCas bypasses a blob.
        self.files
            .extend(self.pending.drain(..).map(|file| file.metadata));
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct CollectedSymlink {
    name: String,
    target: String,
}

#[derive(Debug)]
struct CollectedDirNode {
    files: Vec<(String, usize)>,
    subdirs: BTreeMap<String, CollectedDirNode>,
    symlinks: Vec<CollectedSymlink>,
}

impl CollectedDirNode {
    const fn new() -> Self {
        Self {
            files: Vec::new(),
            subdirs: BTreeMap::new(),
            symlinks: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct TopLevelFile {
    rel_path: String,
    file_index: usize,
}

#[derive(Debug)]
struct TopLevelSymlink {
    rel_path: String,
    target: String,
    is_dir_field: bool,
}

#[derive(Debug)]
struct TopLevelDir {
    rel_path: String,
    tree: CollectedDirNode,
}

#[derive(Debug)]
enum StdioPlan {
    Empty,
    Inline(Bytes),
    UploadBytes { bytes: Bytes, digest: Digest },
    UploadFile { file_index: usize },
}

enum UploadPayload {
    File { handle: Arc<File>, size: u64 },
    Bytes(Bytes),
}

/// Collects, hashes, inserts into `DiskCas`, and uploads outputs for an executed attempt,
/// publishing to `ActionStore` when `exit_code == 0 && !do_not_cache`.
///
/// # Errors
///
/// Returns [`UploadError`] if declared output paths are invalid, an output has an unexpected
/// filesystem type, symlink targets are non-UTF-8, hashing fails, or CAS / `ActionCache` writes
/// fail.
#[allow(deprecated)]
pub async fn upload_outputs<F, S, A>(
    input: UploadInput<'_>,
    tiered: &Tiered<F, S>,
    action_store: Option<&A>,
    clock: &dyn Clock,
    config: &UploadConfig,
) -> Result<UploadOutcome, UploadError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
    A: ActionStore,
{
    let blocking = BlockingScope::new();
    upload_outputs_inner(
        input,
        tiered,
        action_store,
        clock,
        config,
        &blocking,
        #[cfg(test)]
        None,
    )
    .await
}

/// Worker-owned upload entry point tracking all blocking hashing and file-read work in `blocking`.
pub(crate) async fn upload_outputs_with_scope<F, S, A>(
    input: UploadInput<'_>,
    tiered: &Tiered<F, S>,
    action_store: Option<&A>,
    clock: &dyn Clock,
    config: &UploadConfig,
    blocking: &BlockingScope,
) -> Result<UploadOutcome, UploadError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
    A: ActionStore,
{
    upload_outputs_inner(
        input,
        tiered,
        action_store,
        clock,
        config,
        blocking,
        #[cfg(test)]
        None,
    )
    .await
}

#[allow(deprecated)]
#[allow(clippy::too_many_arguments)]
async fn upload_outputs_inner<F, S, A>(
    input: UploadInput<'_>,
    tiered: &Tiered<F, S>,
    action_store: Option<&A>,
    clock: &dyn Clock,
    config: &UploadConfig,
    blocking: &BlockingScope,
    #[cfg(test)] before_hash: Option<Arc<dyn Fn() + Send + Sync>>,
) -> Result<UploadOutcome, UploadError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
    A: ActionStore,
{
    let upload_start = clock.wall();
    let roots = RootContext::new(input.work_dir, input.working_dir)?;
    let use_output_paths = !input.command.output_paths.is_empty();

    let mut collector = FileCollector::new(
        tiered,
        config,
        blocking,
        #[cfg(test)]
        before_hash,
    );
    let mut top_files = Vec::new();
    let mut top_symlinks = Vec::new();
    let mut top_dirs = Vec::new();

    collect_declared_outputs(
        input.command,
        input.working_dir,
        &roots,
        use_output_paths,
        &mut collector,
        &mut top_files,
        &mut top_symlinks,
        &mut top_dirs,
    )
    .await?;

    let stdout_plan =
        plan_stdio(input.stdout, config.inline_stdio_max_bytes, &mut collector).await?;
    let stderr_plan =
        plan_stdio(input.stderr, config.inline_stdio_max_bytes, &mut collector).await?;
    collector.flush().await?;
    let files = &collector.files;

    let BuiltDirectories {
        output_directories,
        mut in_memory_blobs,
    } = build_output_directories(input.command, top_dirs, files)?;

    if let StdioPlan::UploadBytes { digest, ref bytes } = stdout_plan {
        in_memory_blobs.push((digest, bytes.clone()));
    }
    if let StdioPlan::UploadBytes { digest, ref bytes } = stderr_plan {
        in_memory_blobs.push((digest, bytes.clone()));
    }

    insert_memory_blobs(tiered.disk(), &in_memory_blobs).await?;
    upload_missing_to_slow(
        tiered,
        &[],
        &in_memory_blobs,
        config.upload_concurrency,
        blocking,
    )
    .await?;

    let action_result = build_action_result(
        input.exit_code,
        input.timing,
        use_output_paths,
        files,
        top_files,
        top_symlinks,
        output_directories,
        &stdout_plan,
        &stderr_plan,
        upload_start,
        clock.wall(),
    )?;

    let cached_action_result = if input.exit_code == 0 && !input.do_not_cache {
        if let Some(store) = action_store {
            let encoded = Bytes::from(action_result.encode_to_vec());
            store.put(input.action_digest, encoded).await?;
            true
        } else {
            false
        }
    } else {
        false
    };

    Ok(UploadOutcome {
        action_result,
        cas_shared_bytes: collector.cas_shared_bytes,
        cached_action_result,
    })
}

struct BuiltDirectories {
    output_directories: Vec<reapi::OutputDirectory>,
    in_memory_blobs: Vec<(Digest, Bytes)>,
}

fn build_output_directories(
    command: &reapi::Command,
    top_dirs: Vec<TopLevelDir>,
    files: &[CollectedFile],
) -> Result<BuiltDirectories, UploadError> {
    let dir_format =
        reapi::command::OutputDirectoryFormat::try_from(command.output_directory_format)
            .unwrap_or(reapi::command::OutputDirectoryFormat::TreeOnly);
    let include_individual_dirs = matches!(
        dir_format,
        reapi::command::OutputDirectoryFormat::DirectoryOnly
            | reapi::command::OutputDirectoryFormat::TreeAndDirectory
    );

    let mut in_memory_blobs = Vec::new();
    let mut output_directories = Vec::with_capacity(top_dirs.len());
    for top_dir in top_dirs {
        let built = build_reapi_tree(&top_dir.tree, files)?;
        let tree_bytes = Bytes::from(built.tree.encode_to_vec());
        let tree_digest = Digest::from_bytes(&tree_bytes);
        in_memory_blobs.push((tree_digest, tree_bytes));

        let root_proto_digest = if include_individual_dirs {
            for (dir_digest, dir_bytes) in built.encoded_dirs {
                in_memory_blobs.push((dir_digest, dir_bytes));
            }
            Some(to_proto(&built.root_digest)?)
        } else {
            None
        };

        output_directories.push(reapi::OutputDirectory {
            path: top_dir.rel_path,
            tree_digest: Some(to_proto(&tree_digest)?),
            is_topologically_sorted: true,
            root_directory_digest: root_proto_digest,
        });
    }
    Ok(BuiltDirectories {
        output_directories,
        in_memory_blobs,
    })
}

#[allow(deprecated)]
#[allow(clippy::too_many_arguments)]
fn build_action_result(
    exit_code: i32,
    timing: ExecutionTiming,
    use_output_paths: bool,
    files: &[CollectedFile],
    top_files: Vec<TopLevelFile>,
    top_symlinks: Vec<TopLevelSymlink>,
    mut output_directories: Vec<reapi::OutputDirectory>,
    stdout_plan: &StdioPlan,
    stderr_plan: &StdioPlan,
    upload_start: Timestamp,
    upload_end: Timestamp,
) -> Result<reapi::ActionResult, UploadError> {
    let mut output_files = Vec::with_capacity(top_files.len());
    for top_file in top_files {
        let file = &files[top_file.file_index];
        output_files.push(reapi::OutputFile {
            path: top_file.rel_path,
            digest: Some(to_proto(&file.digest)?),
            is_executable: file.executable,
            contents: Bytes::new(),
            node_properties: None,
        });
    }

    let mut output_symlinks = Vec::new();
    let mut output_file_symlinks = Vec::new();
    let mut output_directory_symlinks = Vec::new();
    for sym in top_symlinks {
        let proto = reapi::OutputSymlink {
            path: sym.rel_path,
            target: sym.target,
            node_properties: None,
        };
        if use_output_paths {
            output_symlinks.push(proto);
        } else if sym.is_dir_field {
            output_directory_symlinks.push(proto);
        } else {
            output_file_symlinks.push(proto);
        }
    }

    output_files.sort_by(|a, b| a.path.cmp(&b.path));
    output_directories.sort_by(|a, b| a.path.cmp(&b.path));
    output_symlinks.sort_by(|a, b| a.path.cmp(&b.path));
    output_file_symlinks.sort_by(|a, b| a.path.cmp(&b.path));
    output_directory_symlinks.sort_by(|a, b| a.path.cmp(&b.path));

    let (stdout_raw, stdout_digest) = finalize_stdio(stdout_plan, files)?;
    let (stderr_raw, stderr_digest) = finalize_stdio(stderr_plan, files)?;

    let execution_metadata = reapi::ExecutedActionMetadata {
        worker: timing.worker,
        queued_timestamp: timing.queued.map(to_proto_timestamp),
        worker_start_timestamp: timing.worker_start.map(to_proto_timestamp),
        worker_completed_timestamp: Some(to_proto_timestamp(upload_end)),
        input_fetch_start_timestamp: timing.input_fetch_start.map(to_proto_timestamp),
        input_fetch_completed_timestamp: timing.input_fetch_completed.map(to_proto_timestamp),
        execution_start_timestamp: timing.execution_start.map(to_proto_timestamp),
        execution_completed_timestamp: timing.execution_completed.map(to_proto_timestamp),
        virtual_execution_duration: None,
        output_upload_start_timestamp: Some(to_proto_timestamp(upload_start)),
        output_upload_completed_timestamp: Some(to_proto_timestamp(upload_end)),
        auxiliary_metadata: Vec::new(),
    };

    Ok(reapi::ActionResult {
        output_files,
        output_file_symlinks,
        output_symlinks,
        output_directories,
        output_directory_symlinks,
        exit_code,
        stdout_raw,
        stdout_digest,
        stderr_raw,
        stderr_digest,
        execution_metadata: Some(execution_metadata),
    })
}

fn directory_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

fn output_io(path: &Path, source: impl Into<io::Error>) -> UploadError {
    UploadError::Io {
        path: path.to_path_buf(),
        source: source.into(),
    }
}

fn open_directory_path(root: &OwnedFd, path: &Path) -> Result<OwnedFd, UploadError> {
    let mut directory = fd_fs::openat(root, ".", directory_flags(), Mode::empty())
        .map_err(|source| output_io(path, source))?;
    for component in path.components() {
        let Component::Normal(name) = component else {
            return Err(UploadError::InvalidPath {
                path: path.display().to_string(),
                reason: "invalid directory component",
            });
        };
        directory = fd_fs::openat(&directory, name, directory_flags(), Mode::empty()).map_err(
            |source| match source {
                rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => UploadError::InvalidPath {
                    path: path.display().to_string(),
                    reason: "output ancestor is not a real directory",
                },
                _ => output_io(path, source),
            },
        )?;
    }
    Ok(directory)
}

enum OpenOutput {
    Symlink(PathBuf),
    File(File),
    Directory(OwnedFd),
}

fn open_output(
    directory: &OwnedFd,
    name: &Path,
    diagnostic: &Path,
) -> Result<Option<OpenOutput>, UploadError> {
    let stat = match fd_fs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => stat,
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(source) => return Err(output_io(diagnostic, source)),
    };
    let output = match FileType::from_raw_mode(stat.st_mode) {
        FileType::Symlink => {
            let target = fd_fs::readlinkat(directory, name, Vec::new())
                .map_err(|source| output_io(diagnostic, source))?;
            OpenOutput::Symlink(
                Path::new(std::ffi::OsStr::from_bytes(target.to_bytes())).to_path_buf(),
            )
        }
        FileType::Directory => OpenOutput::Directory(
            fd_fs::openat(directory, name, directory_flags(), Mode::empty())
                .map_err(|source| output_io(diagnostic, source))?,
        ),
        FileType::RegularFile => {
            // NONBLOCK prevents a concurrent replacement with a FIFO from blocking the worker.
            let fd = fd_fs::openat(
                directory,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|source| output_io(diagnostic, source))?;
            let stat = fd_fs::fstat(&fd).map_err(|source| output_io(diagnostic, source))?;
            if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
                return Err(UploadError::OutputKindMismatch {
                    path: diagnostic.display().to_string(),
                    expected: "regular file",
                    actual: "non-regular file",
                });
            }
            OpenOutput::File(File::from(fd))
        }
        _ => {
            return Err(UploadError::OutputKindMismatch {
                path: diagnostic.display().to_string(),
                expected: "file, directory, or symlink",
                actual: "special file",
            });
        }
    };
    Ok(Some(output))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DeclaredOutputKind {
    Any,
    File,
    Directory,
}

#[allow(deprecated, clippy::too_many_arguments)]
async fn collect_declared_outputs<F, S>(
    command: &reapi::Command,
    working_dir: &Path,
    roots: &RootContext,
    use_output_paths: bool,
    collector: &mut FileCollector<'_, F, S>,
    top_files: &mut Vec<TopLevelFile>,
    top_symlinks: &mut Vec<TopLevelSymlink>,
    top_dirs: &mut Vec<TopLevelDir>,
) -> Result<(), UploadError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    let declarations = if use_output_paths {
        vec![(&command.output_paths, DeclaredOutputKind::Any)]
    } else {
        vec![
            (&command.output_files, DeclaredOutputKind::File),
            (&command.output_directories, DeclaredOutputKind::Directory),
        ]
    };
    for (paths, expected) in declarations {
        let mut seen = HashSet::new();
        for raw in paths {
            if !seen.insert(raw.as_str()) {
                continue;
            }
            let rel = validate_output_path(raw, expected == DeclaredOutputKind::Directory)?;
            let parent = rel.parent().unwrap_or_else(|| Path::new(""));
            let directory = match open_directory_path(&roots.working_fd, parent) {
                Ok(fd) => fd,
                Err(UploadError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                    continue;
                }
                Err(error) => return Err(error),
            };
            let name = rel.file_name().map_or_else(|| Path::new("."), Path::new);
            let abs = working_dir.join(&rel);
            let Some(output) = open_output(&directory, name, &abs)? else {
                continue;
            };
            match output {
                OpenOutput::Symlink(target) => top_symlinks.push(TopLevelSymlink {
                    rel_path: raw.clone(),
                    target: roots.rebase_symlink_target(&abs, &rel, &target)?,
                    is_dir_field: expected == DeclaredOutputKind::Directory,
                }),
                OpenOutput::File(file) if expected != DeclaredOutputKind::Directory => {
                    let idx = collector.push(abs, file).await?;
                    top_files.push(TopLevelFile {
                        rel_path: raw.clone(),
                        file_index: idx,
                    });
                }
                OpenOutput::Directory(fd) if expected != DeclaredOutputKind::File => {
                    let tree = walk_output_directory(&fd, &rel, roots, collector, 0).await?;
                    top_dirs.push(TopLevelDir {
                        rel_path: raw.clone(),
                        tree,
                    });
                }
                _ => {
                    return Err(UploadError::OutputKindMismatch {
                        path: raw.clone(),
                        expected: if expected == DeclaredOutputKind::Directory {
                            "directory"
                        } else {
                            "file"
                        },
                        actual: if expected == DeclaredOutputKind::File {
                            "directory"
                        } else {
                            "file"
                        },
                    });
                }
            }
        }
    }
    Ok(())
}

async fn walk_output_directory<F, S>(
    fd: &OwnedFd,
    dir_rel_to_cwd: &Path,
    roots: &RootContext,
    collector: &mut FileCollector<'_, F, S>,
    depth: usize,
) -> Result<CollectedDirNode, UploadError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    if depth >= MAX_OUTPUT_DEPTH {
        return Err(UploadError::InvalidPath {
            path: dir_rel_to_cwd.display().to_string(),
            reason: "output directory nesting is too deep",
        });
    }
    let mut node = CollectedDirNode::new();
    let listing_fd = fd_fs::openat(fd, ".", directory_flags(), Mode::empty())
        .map_err(|source| output_io(dir_rel_to_cwd, source))?;
    let mut directory = Dir::new(listing_fd).map_err(|source| output_io(dir_rel_to_cwd, source))?;
    let mut names = Vec::new();
    while let Some(entry) = directory.read() {
        let entry = entry.map_err(|source| output_io(dir_rel_to_cwd, source))?;
        let name = entry.file_name();
        if name == c"." || name == c".." {
            continue;
        }
        let name = name.to_str().map_err(|_| UploadError::InvalidPath {
            path: dir_rel_to_cwd.display().to_string(),
            reason: "output filename is not valid UTF-8",
        })?;
        names.push(name.to_owned());
    }
    drop(directory);
    names.sort();
    for name in names {
        let rel = dir_rel_to_cwd.join(&name);
        let Some(output) = open_output(fd, Path::new(&name), &rel)? else {
            continue;
        };
        match output {
            OpenOutput::Symlink(target) => node.symlinks.push(CollectedSymlink {
                target: roots.rebase_symlink_target(&rel, &rel, &target)?,
                name,
            }),
            OpenOutput::File(file) => {
                let idx = collector.push(rel, file).await?;
                node.files.push((name, idx));
            }
            OpenOutput::Directory(child) => {
                let subtree = Box::pin(walk_output_directory(
                    &child,
                    &rel,
                    roots,
                    collector,
                    depth + 1,
                ))
                .await?;
                node.subdirs.insert(name, subtree);
            }
        }
    }
    Ok(node)
}

async fn plan_stdio<F, S>(
    captured: Option<CapturedOutput>,
    inline_max: u64,
    collector: &mut FileCollector<'_, F, S>,
) -> Result<StdioPlan, UploadError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    let Some(output) = captured else {
        return Ok(StdioPlan::Empty);
    };
    match output {
        CapturedOutput::Bytes(bytes) => {
            if bytes.is_empty() {
                return Ok(StdioPlan::Empty);
            }
            let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if len <= inline_max {
                Ok(StdioPlan::Inline(bytes))
            } else {
                let digest = Digest::from_bytes(&bytes);
                Ok(StdioPlan::UploadBytes { bytes, digest })
            }
        }
        CapturedOutput::File(path) => {
            let fd = fd_fs::open(
                &path,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|source| output_io(&path, source))?;
            let stat = fd_fs::fstat(&fd).map_err(|source| output_io(&path, source))?;
            if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
                return Err(UploadError::OutputKindMismatch {
                    path: path.display().to_string(),
                    expected: "regular file",
                    actual: "non-regular file",
                });
            }
            let mut file = File::from(fd);
            let meta = file.metadata().map_err(|source| output_io(&path, source))?;
            if meta.len() == 0 {
                return Ok(StdioPlan::Empty);
            }
            if meta.len() <= inline_max {
                let mut raw = Vec::new();
                file.read_to_end(&mut raw)
                    .map_err(|source| output_io(&path, source))?;
                return Ok(StdioPlan::Inline(Bytes::from(raw)));
            }
            let idx = collector.push(path, file).await?;
            Ok(StdioPlan::UploadFile { file_index: idx })
        }
    }
}

fn finalize_stdio(
    plan: &StdioPlan,
    files: &[CollectedFile],
) -> Result<(Bytes, Option<reapi::Digest>), UploadError> {
    match plan {
        StdioPlan::Empty => Ok((Bytes::new(), None)),
        StdioPlan::Inline(bytes) => Ok((bytes.clone(), None)),
        StdioPlan::UploadBytes { digest, .. } => Ok((Bytes::new(), Some(to_proto(digest)?))),
        StdioPlan::UploadFile { file_index } => {
            let digest = files[*file_index].digest;
            Ok((Bytes::new(), Some(to_proto(&digest)?)))
        }
    }
}

async fn hash_files_parallel(
    files: &mut [OpenedFile],
    concurrency: NonZeroUsize,
    blocking: &BlockingScope,
    #[cfg(test)] before_hash: Option<Arc<dyn Fn() + Send + Sync>>,
) -> Result<(), UploadError> {
    let jobs: Vec<(usize, PathBuf, Arc<File>)> = files
        .iter()
        .enumerate()
        .filter(|(_, file)| file.metadata.size > 0)
        .map(|(idx, file)| {
            (
                idx,
                file.metadata.abs_path.clone(),
                Arc::clone(&file.handle),
            )
        })
        .collect();

    if jobs.is_empty() {
        return Ok(());
    }

    let results: Vec<Result<(usize, Digest), UploadError>> = futures::stream::iter(jobs)
        .map(|(idx, path, handle)| {
            #[cfg(test)]
            let hook = before_hash.clone();
            async move {
                let path_for_join = path.clone();
                let digest = blocking
                    .run(move |cancelled| {
                        #[cfg(test)]
                        if let Some(hook) = hook {
                            hook();
                        }
                        hash_file_streaming(&handle, &path, cancelled)
                    })
                    .await
                    .map_err(|err| UploadError::Io {
                        path: path_for_join,
                        source: io::Error::other(format!("hash task failed: {err}")),
                    })??;
                Ok((idx, digest))
            }
        })
        .buffer_unordered(concurrency.get())
        .collect()
        .await;

    for result in results {
        let (idx, digest) = result?;
        files[idx].metadata.digest = digest;
    }
    Ok(())
}

fn hash_file_streaming(
    file: &File,
    path: &Path,
    cancelled: &AtomicBool,
) -> Result<Digest, UploadError> {
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buf = vec![0_u8; STREAM_CHUNK_BYTES];
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err(UploadError::Cancelled);
        }
        let read = file
            .read_at(&mut buf, total)
            .map_err(|source| UploadError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        total = total.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
    }
    if total == 0 {
        return Ok(Digest::EMPTY);
    }
    Ok(Digest::new(hasher.finalize().into(), total))
}

struct BuiltTree {
    tree: reapi::Tree,
    root_digest: Digest,
    encoded_dirs: Vec<(Digest, Bytes)>,
}

struct BuiltNode {
    dir: reapi::Directory,
    digest: Digest,
    descendants: Vec<(Digest, reapi::Directory, Bytes)>,
}

fn build_reapi_tree(
    root: &CollectedDirNode,
    files: &[CollectedFile],
) -> Result<BuiltTree, UploadError> {
    fn build_node(
        node: &CollectedDirNode,
        files: &[CollectedFile],
    ) -> Result<BuiltNode, UploadError> {
        let mut dir_nodes = Vec::with_capacity(node.subdirs.len());
        let mut descendants = Vec::new();
        for (name, child) in &node.subdirs {
            let built_child = build_node(child, files)?;
            let child_bytes = Bytes::from(built_child.dir.encode_to_vec());
            dir_nodes.push(reapi::DirectoryNode {
                name: name.clone(),
                digest: Some(to_proto(&built_child.digest)?),
            });
            descendants.push((built_child.digest, built_child.dir, child_bytes));
            descendants.extend(built_child.descendants);
        }

        let mut file_nodes = Vec::with_capacity(node.files.len());
        for (name, file_idx) in &node.files {
            let file = &files[*file_idx];
            file_nodes.push(reapi::FileNode {
                name: name.clone(),
                digest: Some(to_proto(&file.digest)?),
                is_executable: file.executable,
                node_properties: None,
            });
        }
        file_nodes.sort_by(|a, b| a.name.cmp(&b.name));

        let mut symlink_nodes: Vec<reapi::SymlinkNode> = node
            .symlinks
            .iter()
            .map(|sym| reapi::SymlinkNode {
                name: sym.name.clone(),
                target: sym.target.clone(),
                node_properties: None,
            })
            .collect();
        symlink_nodes.sort_by(|a, b| a.name.cmp(&b.name));
        dir_nodes.sort_by(|a, b| a.name.cmp(&b.name));

        let dir = reapi::Directory {
            files: file_nodes,
            directories: dir_nodes,
            symlinks: symlink_nodes,
            node_properties: None,
        };
        let digest = Digest::from_bytes(&dir.encode_to_vec());
        Ok(BuiltNode {
            dir,
            digest,
            descendants,
        })
    }

    let built_root = build_node(root, files)?;
    let root_bytes = Bytes::from(built_root.dir.encode_to_vec());

    // Emit children in deterministic topological order (parents before children), deduplicated.
    let mut seen = HashSet::new();
    let mut children = Vec::new();
    let mut encoded_dirs = vec![(built_root.digest, root_bytes)];
    seen.insert(built_root.digest);

    for (digest, dir, bytes) in built_root.descendants {
        if seen.insert(digest) {
            children.push(dir);
            encoded_dirs.push((digest, bytes));
        }
    }

    Ok(BuiltTree {
        tree: reapi::Tree {
            root: Some(built_root.dir),
            children,
        },
        root_digest: built_root.digest,
        encoded_dirs,
    })
}

async fn insert_outputs_into_disk_cas(
    disk: &DiskCas,
    files: &[OpenedFile],
    counted_inodes: &mut HashSet<(u64, u64)>,
    blocking: &BlockingScope,
) -> Result<u64, UploadError> {
    let mut cas_shared_bytes = 0_u64;
    let mut inserted_keys: HashSet<CacheKey> = HashSet::new();

    for opened in files {
        let file = &opened.metadata;
        if file.digest.is_empty() {
            continue;
        }
        if is_inode_shared_with_disk_cas(disk, file)? {
            if counted_inodes.insert((file.dev, file.ino)) {
                cas_shared_bytes = cas_shared_bytes.saturating_add(file.size);
            }
            continue;
        }

        let key = CacheKey::new(file.digest, BlobMode::from_executable(file.executable));
        if !inserted_keys.insert(key) || !disk.can_admit(file.size) {
            continue;
        }
        if let Some(_pin) = disk.pin_attempt(file.digest, file.executable) {
            continue;
        }
        let stream = file_blob_stream(Arc::clone(&opened.handle), file.size, blocking.clone());
        let _outcome: WriteOutcome = disk
            .write_with_mode(file.digest, stream, file.executable)
            .await?;
    }

    Ok(cas_shared_bytes)
}

async fn insert_memory_blobs(
    disk: &DiskCas,
    in_memory_blobs: &[(Digest, Bytes)],
) -> Result<(), UploadError> {
    let mut inserted_keys = HashSet::new();
    for (digest, bytes) in in_memory_blobs {
        if digest.is_empty() {
            continue;
        }
        let key = CacheKey::new(*digest, BlobMode::Plain);
        if !inserted_keys.insert(key) || !disk.can_admit(digest.size()) {
            continue;
        }
        if let Some(_pin) = disk.pin_attempt(*digest, false) {
            continue;
        }
        let _outcome = disk
            .write_with_mode(*digest, BlobStream::from_bytes(bytes.clone()), false)
            .await?;
    }

    Ok(())
}

fn is_inode_shared_with_disk_cas(
    disk: &DiskCas,
    file: &CollectedFile,
) -> Result<bool, UploadError> {
    for executable in [file.executable, !file.executable] {
        let mode = BlobMode::from_executable(executable);
        let path = blob_path(disk.root(), CacheKey::new(file.digest, mode));
        if let Some(cas_meta) = symlink_metadata_optional(&path)?
            && cas_meta.dev() == file.dev
            && cas_meta.ino() == file.ino
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn upload_missing_to_slow<F, S>(
    tiered: &Tiered<F, S>,
    files: &[OpenedFile],
    in_memory_blobs: &[(Digest, Bytes)],
    concurrency: NonZeroUsize,
    blocking: &BlockingScope,
) -> Result<(), UploadError>
where
    F: Borrow<DiskCas> + fmt::Debug + Send + Sync + 'static,
    S: ContentStore,
{
    let mut payloads: HashMap<Digest, UploadPayload> = HashMap::new();
    let mut unique_digests = Vec::new();

    for opened in files {
        let file = &opened.metadata;
        if file.digest.is_empty() || payloads.contains_key(&file.digest) {
            continue;
        }
        unique_digests.push(file.digest);
        payloads.insert(
            file.digest,
            UploadPayload::File {
                handle: Arc::clone(&opened.handle),
                size: file.size,
            },
        );
    }

    for (digest, bytes) in in_memory_blobs {
        if digest.is_empty() || payloads.contains_key(digest) {
            continue;
        }
        unique_digests.push(*digest);
        payloads.insert(*digest, UploadPayload::Bytes(bytes.clone()));
    }

    if unique_digests.is_empty() {
        return Ok(());
    }

    let mut missing = Vec::new();
    tiered
        .slow()
        .find_missing(&unique_digests, &mut missing)
        .await?;
    if missing.is_empty() {
        return Ok(());
    }

    let jobs: Vec<(Digest, UploadPayload)> = missing
        .into_iter()
        .filter_map(|digest| payloads.remove(&digest).map(|payload| (digest, payload)))
        .collect();

    let slow = tiered.slow();
    let results: Vec<Result<(), UploadError>> = futures::stream::iter(jobs)
        .map(|(digest, payload)| {
            let scope = blocking.clone();
            async move {
                let stream = match payload {
                    UploadPayload::Bytes(bytes) => BlobStream::from_bytes(bytes),
                    UploadPayload::File { handle, size } => file_blob_stream(handle, size, scope),
                };
                slow.write(digest, stream).await?;
                Ok(())
            }
        })
        .buffer_unordered(concurrency.get())
        .collect()
        .await;

    for result in results {
        result?;
    }
    Ok(())
}

fn file_blob_stream(file: Arc<File>, size: u64, blocking: BlockingScope) -> BlobStream {
    if size == 0 {
        return BlobStream::empty();
    }
    let chunks: BoxStream<'static, Result<Bytes, PortError>> = Box::pin(stream::unfold(
        Some((file, size, blocking)),
        move |state| async move {
            let (file, remaining, blocking) = state?;
            if remaining == 0 {
                return None;
            }
            if blocking.is_cancelled() {
                return Some((Err(PortError::unavailable("output upload cancelled")), None));
            }
            let to_read = usize::try_from(remaining.min(STREAM_CHUNK_BYTES as u64))
                .unwrap_or(STREAM_CHUNK_BYTES);

            let step = blocking
                .run(move |cancelled| {
                    if cancelled.load(Ordering::Acquire) {
                        return Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "output upload cancelled",
                        ));
                    }
                    let mut buf = vec![0_u8; to_read];
                    let read = file.read_at(&mut buf, size - remaining)?;
                    buf.truncate(read);
                    Ok::<_, io::Error>((file, Bytes::from(buf)))
                })
                .await;

            match step {
                Ok(Ok((file, bytes))) => {
                    if bytes.is_empty() {
                        Some((
                            Err(PortError::backend(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "output file truncated during upload",
                            ))),
                            None,
                        ))
                    } else {
                        let read_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                        let next_remaining = remaining.saturating_sub(read_len);
                        Some((Ok(bytes), Some((file, next_remaining, blocking))))
                    }
                }
                Ok(Err(error)) => Some((Err(PortError::backend(error)), None)),
                Err(join_err) => Some((Err(PortError::backend(join_err)), None)),
            }
        },
    ));

    BlobStream { size, chunks }
}

fn symlink_metadata_optional(path: &Path) -> Result<Option<fs::Metadata>, UploadError> {
    match fs::symlink_metadata(path) {
        Ok(meta) => Ok(Some(meta)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(UploadError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn validate_output_path(raw: &str, allow_empty: bool) -> Result<PathBuf, UploadError> {
    if raw.is_empty() {
        if allow_empty {
            return Ok(PathBuf::new());
        }
        return Err(UploadError::InvalidPath {
            path: raw.to_owned(),
            reason: "output path must not be empty",
        });
    }
    if raw.starts_with('/') || raw.ends_with('/') || raw.contains('\0') {
        return Err(UploadError::InvalidPath {
            path: raw.to_owned(),
            reason: "output path must be relative without leading/trailing '/' or NUL",
        });
    }
    let mut out = PathBuf::new();
    for segment in raw.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(UploadError::InvalidPath {
                path: raw.to_owned(),
                reason: "output path contains empty, `.`, or `..` segment",
            });
        }
        out.push(segment);
    }
    Ok(out)
}

fn make_absolute_lexical(path: &Path) -> Result<PathBuf, UploadError> {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|source| UploadError::Io {
                path: path.to_path_buf(),
                source,
            })?
            .join(path)
    };
    Ok(normalize_lexical_absolute(&joined))
}

fn normalize_lexical_absolute(path: &Path) -> PathBuf {
    let mut stack: Vec<std::ffi::OsString> = Vec::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                stack.pop();
            }
            Component::Normal(seg) => stack.push(seg.to_os_string()),
            Component::Prefix(prefix) => stack.push(prefix.as_os_str().to_os_string()),
        }
    }
    let mut out = PathBuf::from("/");
    for seg in stack {
        out.push(seg);
    }
    out
}

fn canonicalize_longest_existing_prefix(path: &Path) -> Option<PathBuf> {
    let mut cur = path;
    let mut suffix = Vec::new();
    loop {
        if let Ok(canon) = fs::canonicalize(cur) {
            let mut out = canon;
            for seg in suffix.into_iter().rev() {
                out.push(seg);
            }
            return Some(out);
        }
        let name = cur.file_name()?;
        suffix.push(name.to_os_string());
        cur = cur.parent()?;
    }
}

fn relative_path_between(from_dir: &Path, to_path: &Path) -> String {
    let from_segs: Vec<&str> = from_dir
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect();
    let to_segs: Vec<&str> = to_path
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect();

    let common = from_segs
        .iter()
        .zip(to_segs.iter())
        .take_while(|(a, b)| a == b)
        .count();

    let up_count = from_segs.len().saturating_sub(common);
    let mut parts = Vec::with_capacity(up_count + to_segs.len().saturating_sub(common));
    parts.resize(up_count, "..");
    parts.extend_from_slice(&to_segs[common..]);
    if parts.is_empty() {
        ".".to_owned()
    } else {
        parts.join("/")
    }
}

fn join_rel(base: &Path, rel: &Path) -> PathBuf {
    if base.as_os_str().is_empty() {
        rel.to_path_buf()
    } else if rel.as_os_str().is_empty() {
        base.to_path_buf()
    } else {
        base.join(rel)
    }
}

const fn to_proto_timestamp(ts: Timestamp) -> prost_types::Timestamp {
    let millis = ts.as_unix_millis();
    #[allow(clippy::cast_possible_wrap)]
    let seconds = (millis / 1000) as i64;
    #[allow(clippy::cast_possible_truncation)]
    let nanos = ((millis % 1000) * 1_000_000) as i32;
    prost_types::Timestamp { seconds, nanos }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::num::NonZeroUsize;
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use gang_cas::{DiskCas, Tiered};
    use gang_clock::{TestClock, Timestamp};
    use gang_reapi::build::bazel::remote::execution::v2 as reapi;
    use gang_reapi::from_proto;
    use gang_spi::{ActionStore, BlobStream, ByteRange, ContentStore, EngineStore, PortError};
    use gang_testkit::{MemoryActionStore, MemoryContentStore, TestResult};
    use gang_types::Digest;
    use prost::Message as _;
    use tempfile::TempDir;

    use super::{
        BlockingScope, CapturedOutput, ExecutionTiming, STREAM_CHUNK_BYTES, UploadConfig,
        UploadError, UploadInput, upload_outputs, upload_outputs_inner,
    };

    fn test_config() -> UploadConfig {
        UploadConfig {
            hash_concurrency: NonZeroUsize::new(2).expect("2 is non-zero"),
            upload_concurrency: NonZeroUsize::new(4).expect("4 is non-zero"),
            inline_stdio_max_bytes: 0,
        }
    }

    fn seed_test_outputs(working_dir: &std::path::Path) -> TestResult {
        fs::write(working_dir.join("plain.txt"), b"plain-content")?;
        let exec_path = working_dir.join("run.sh");
        fs::write(&exec_path, b"#!/bin/sh\necho hi\n")?;
        fs::set_permissions(&exec_path, fs::Permissions::from_mode(0o755))?;
        fs::write(working_dir.join("empty.bin"), b"")?;
        symlink("plain.txt", working_dir.join("top-link"))?;

        let out_dir = working_dir.join("symlink-output");
        let nested = out_dir.join("nested");
        fs::create_dir_all(&nested)?;
        fs::write(out_dir.join("payload"), b"hello")?;
        let abs_out = out_dir.canonicalize()?;
        symlink(abs_out.join("payload"), nested.join("absolute-internal"))?;
        symlink(abs_out.join("missing"), nested.join("absolute-dangling"))?;
        symlink("../payload", nested.join("relative"))?;
        symlink("/outside-action-root", nested.join("absolute-external"))?;
        symlink(
            abs_out.join("nested/../payload"),
            nested.join("absolute-lexical"),
        )?;
        Ok(())
    }

    fn assert_symlink_tree_roundtrip(
        slow: &MemoryContentStore,
        out_dir_res: &reapi::OutputDirectory,
    ) -> TestResult {
        assert!(out_dir_res.is_topologically_sorted);
        assert!(out_dir_res.root_directory_digest.is_some());
        let tree_digest = from_proto(out_dir_res.tree_digest.as_ref().expect("tree_digest"))?;
        let tree_bytes = slow.get_bytes(tree_digest).expect("tree uploaded to slow");
        let decoded_tree = reapi::Tree::decode(tree_bytes)?;
        let root_dir = decoded_tree.root.expect("tree root");
        assert_eq!(root_dir.files.len(), 1);
        assert_eq!(root_dir.files[0].name, "payload");
        assert_eq!(decoded_tree.children.len(), 1);
        let nested_dir = &decoded_tree.children[0];
        let targets: BTreeMap<&str, &str> = nested_dir
            .symlinks
            .iter()
            .map(|s| (s.name.as_str(), s.target.as_str()))
            .collect();
        assert_eq!(
            targets,
            BTreeMap::from([
                ("absolute-dangling", "../missing"),
                ("absolute-external", "/outside-action-root"),
                ("absolute-internal", "../payload"),
                ("absolute-lexical", "../payload"),
                ("relative", "../payload"),
            ])
        );
        Ok(())
    }

    #[tokio::test]
    async fn uploads_files_empty_dirs_tree_symlinks_and_caches_only_zero_exit() -> TestResult {
        let tmp = TempDir::new()?;
        let work_dir = tmp.path().join("attempt/work");
        let working_dir = work_dir.clone();
        fs::create_dir_all(&working_dir)?;
        seed_test_outputs(&working_dir)?;

        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk.clone(), slow.clone());
        let ac = MemoryActionStore::new();
        let clock = TestClock::new(Timestamp::from_unix_millis(10_000));

        let command = reapi::Command {
            output_paths: vec![
                "plain.txt".to_owned(),
                "run.sh".to_owned(),
                "empty.bin".to_owned(),
                "missing-optional.txt".to_owned(),
                "top-link".to_owned(),
                "symlink-output".to_owned(),
            ],
            output_directory_format: reapi::command::OutputDirectoryFormat::TreeAndDirectory as i32,
            ..Default::default()
        };
        let action_digest = Digest::from_bytes(b"test-action-1");

        let outcome = upload_outputs(
            UploadInput {
                action_digest,
                do_not_cache: false,
                exit_code: 0,
                command: &command,
                work_dir: &work_dir,
                working_dir: &working_dir,
                stdout: Some(CapturedOutput::Bytes(Bytes::from_static(b"stdout-data"))),
                stderr: Some(CapturedOutput::Bytes(Bytes::new())),
                timing: ExecutionTiming {
                    worker: "worker-1".to_owned(),
                    queued: Some(Timestamp::from_unix_millis(1_000)),
                    worker_start: Some(Timestamp::from_unix_millis(2_000)),
                    input_fetch_start: Some(Timestamp::from_unix_millis(3_000)),
                    input_fetch_completed: Some(Timestamp::from_unix_millis(4_000)),
                    execution_start: Some(Timestamp::from_unix_millis(5_000)),
                    execution_completed: Some(Timestamp::from_unix_millis(6_000)),
                },
            },
            &tiered,
            Some(&ac),
            &clock,
            &test_config(),
        )
        .await?;

        assert!(outcome.cached_action_result);
        assert_eq!(outcome.cas_shared_bytes, 0);
        let result = &outcome.action_result;
        assert_eq!(result.exit_code, 0);
        assert_eq!(result.output_files.len(), 3);
        assert_eq!(result.output_files[0].path, "empty.bin");
        assert_eq!(
            from_proto(result.output_files[0].digest.as_ref().expect("digest"))?,
            Digest::EMPTY
        );
        assert!(!result.output_files[0].is_executable);

        assert_eq!(result.output_files[1].path, "plain.txt");
        assert!(!result.output_files[1].is_executable);
        let plain_digest = Digest::from_bytes(b"plain-content");
        assert_eq!(
            from_proto(result.output_files[1].digest.as_ref().expect("digest"))?,
            plain_digest
        );
        assert_eq!(
            slow.get_bytes(plain_digest),
            Some(Bytes::from_static(b"plain-content"))
        );
        assert!(disk.pin_attempt(plain_digest, false).is_some());

        assert_eq!(result.output_files[2].path, "run.sh");
        assert!(result.output_files[2].is_executable);

        // Top-level symlink.
        assert_eq!(result.output_symlinks.len(), 1);
        assert_eq!(result.output_symlinks[0].path, "top-link");
        assert_eq!(result.output_symlinks[0].target, "plain.txt");

        // Output directory Tree roundtrip + symlink rebasing.
        assert_eq!(result.output_directories.len(), 1);
        assert_symlink_tree_roundtrip(&slow, &result.output_directories[0])?;

        // Stdout uploaded to CAS; empty stderr omitted.
        let stdout_digest = from_proto(result.stdout_digest.as_ref().expect("stdout digest"))?;
        assert_eq!(
            slow.get_bytes(stdout_digest),
            Some(Bytes::from_static(b"stdout-data"))
        );
        assert!(result.stderr_digest.is_none());

        // ActionResult stored in ActionStore.
        let cached = ac.get(action_digest).await?.expect("action cached");
        assert_eq!(reapi::ActionResult::decode(cached)?, *result);

        // Metadata timestamps populated.
        let meta = result.execution_metadata.as_ref().expect("metadata");
        assert_eq!(meta.worker, "worker-1");
        assert!(meta.output_upload_start_timestamp.is_some());
        assert!(meta.output_upload_completed_timestamp.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn skips_action_cache_on_nonzero_exit_or_do_not_cache_and_tracks_cas_shared_bytes()
    -> TestResult {
        let tmp = TempDir::new()?;
        let work_dir = tmp.path().join("attempt/work");
        fs::create_dir_all(&work_dir)?;

        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk.clone(), slow.clone());
        let ac = MemoryActionStore::new();
        let clock = TestClock::new(Timestamp::from_unix_millis(5_000));

        // Simulate a hardlinked input file inside `work_dir` that is also declared as an output.
        let shared_bytes = Bytes::from_static(b"shared-input-inode-bytes");
        let shared_digest = Digest::from_bytes(&shared_bytes);
        disk.write(shared_digest, BlobStream::from_bytes(shared_bytes.clone()))
            .await?;
        // Force a hardlink directly from the DiskCas blob path so dev/ino match even on macOS APFS.
        let cas_file = gang_cas::blob_path(
            disk.root(),
            gang_cas::CacheKey::new(shared_digest, gang_cas::BlobMode::Plain),
        );
        fs::hard_link(&cas_file, work_dir.join("shared.dat"))?;
        fs::hard_link(&cas_file, work_dir.join("again.dat"))?;
        fs::write(work_dir.join("fresh.dat"), b"fresh-bytes")?;

        let command = reapi::Command {
            output_paths: vec![
                "shared.dat".to_owned(),
                "fresh.dat".to_owned(),
                "again.dat".to_owned(),
            ],
            ..Default::default()
        };

        // Case 1: non-zero exit code -> do NOT write ActionStore, still tracks cas_shared_bytes.
        let d1 = Digest::from_bytes(b"action-nonzero");
        let out_nonzero = upload_outputs(
            UploadInput {
                action_digest: d1,
                do_not_cache: false,
                exit_code: 2,
                command: &command,
                work_dir: &work_dir,
                working_dir: &work_dir,
                stdout: None,
                stderr: None,
                timing: ExecutionTiming::default(),
            },
            &tiered,
            Some(&ac),
            &clock,
            &test_config(),
        )
        .await?;
        assert!(!out_nonzero.cached_action_result);
        assert!(ac.get(d1).await?.is_none());
        assert_eq!(out_nonzero.cas_shared_bytes, shared_digest.size());

        // Case 2: exit 0 with `do_not_cache = true` -> do NOT write ActionStore.
        let d2 = Digest::from_bytes(b"action-dnc");
        let out_dnc = upload_outputs(
            UploadInput {
                action_digest: d2,
                do_not_cache: true,
                exit_code: 0,
                command: &command,
                work_dir: &work_dir,
                working_dir: &work_dir,
                stdout: None,
                stderr: None,
                timing: ExecutionTiming::default(),
            },
            &tiered,
            Some(&ac),
            &clock,
            &test_config(),
        )
        .await?;
        assert!(!out_dnc.cached_action_result);
        assert!(ac.get(d2).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn rebases_symlinks_through_symlinked_work_root_and_inlines_stdio_when_configured()
    -> TestResult {
        let tmp = TempDir::new()?;
        let physical = tmp.path().join("physical");
        fs::create_dir_all(physical.join("attempt/work/sub/out"))?;
        let alias = tmp.path().join("alias");
        symlink(physical.canonicalize()?, &alias)?;

        let work_dir = alias.join("attempt/work");
        let working_dir = work_dir.join("sub");
        fs::write(working_dir.join("target.txt"), b"data")?;
        // Action saw canonical path from `current_dir()`.
        let canonical_target = working_dir.join("target.txt").canonicalize()?;
        symlink(&canonical_target, working_dir.join("out/link"))?;

        let stdout_file = tmp.path().join("stdout.log");
        fs::write(&stdout_file, b"hi-from-file")?;

        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk, slow);
        let clock = TestClock::new(Timestamp::from_unix_millis(1_000));

        let command = reapi::Command {
            working_directory: "sub".to_owned(),
            output_paths: vec!["out".to_owned()],
            ..Default::default()
        };
        let cfg = UploadConfig {
            inline_stdio_max_bytes: 1024,
            ..test_config()
        };
        let outcome = upload_outputs(
            UploadInput {
                action_digest: Digest::from_bytes(b"alias-action"),
                do_not_cache: true,
                exit_code: 0,
                command: &command,
                work_dir: &work_dir,
                working_dir: &working_dir,
                stdout: Some(CapturedOutput::File(stdout_file)),
                stderr: None,
                timing: ExecutionTiming::default(),
            },
            &tiered,
            None::<&MemoryActionStore>,
            &clock,
            &cfg,
        )
        .await?;

        assert_eq!(
            outcome.action_result.stdout_raw,
            Bytes::from_static(b"hi-from-file")
        );
        assert!(outcome.action_result.stdout_digest.is_none());

        let tree_digest = from_proto(
            outcome.action_result.output_directories[0]
                .tree_digest
                .as_ref()
                .expect("tree"),
        )?;
        let tree_bytes = tiered.slow().get_bytes(tree_digest).expect("tree uploaded");
        let tree = reapi::Tree::decode(tree_bytes)?;
        let root = tree.root.expect("root");
        assert_eq!(root.symlinks.len(), 1);
        assert_eq!(root.symlinks[0].target, "../target.txt");
        Ok(())
    }

    #[derive(Debug, Default)]
    struct BoundedSlow {
        inner: MemoryContentStore,
        inflight: Arc<AtomicUsize>,
        max_inflight: Arc<AtomicUsize>,
    }

    impl ContentStore for BoundedSlow {
        async fn find_missing(
            &self,
            digests: &[Digest],
            missing: &mut Vec<Digest>,
        ) -> Result<(), PortError> {
            self.inner.find_missing(digests, missing).await
        }

        async fn read(
            &self,
            digest: Digest,
            range: ByteRange,
        ) -> Result<Option<BlobStream>, PortError> {
            self.inner.read(digest, range).await
        }

        async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
            let current = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_inflight.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(15)).await;
            let res = self.inner.write(digest, body).await;
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            res
        }

        fn usage(&self) -> Option<EngineStore> {
            self.inner.usage()
        }
    }

    #[tokio::test]
    async fn parallel_upload_respects_concurrency_bound() -> TestResult {
        let tmp = TempDir::new()?;
        let work_dir = tmp.path().join("attempt/work");
        fs::create_dir_all(&work_dir)?;

        let mut output_paths = Vec::new();
        for idx in 0..8_usize {
            let name = format!("f_{idx}.txt");
            fs::write(work_dir.join(&name), format!("payload-{idx}").as_bytes())?;
            output_paths.push(name);
        }

        let slow = Arc::new(BoundedSlow::default());
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk, Arc::clone(&slow));
        let clock = TestClock::new(Timestamp::from_unix_millis(1_000));

        let command = reapi::Command {
            output_paths,
            ..Default::default()
        };
        let cfg = UploadConfig {
            hash_concurrency: NonZeroUsize::new(2).expect("2"),
            upload_concurrency: NonZeroUsize::new(3).expect("3"),
            inline_stdio_max_bytes: 0,
        };
        let _outcome = upload_outputs(
            UploadInput {
                action_digest: Digest::from_bytes(b"bounded"),
                do_not_cache: true,
                exit_code: 0,
                command: &command,
                work_dir: &work_dir,
                working_dir: &work_dir,
                stdout: None,
                stderr: None,
                timing: ExecutionTiming::default(),
            },
            &tiered,
            None::<&MemoryActionStore>,
            &clock,
            &cfg,
        )
        .await?;
        assert!(slow.max_inflight.load(Ordering::SeqCst) <= 3);
        assert_eq!(slow.inner.len(), 8);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[derive(Debug)]
    struct OutputFdStore {
        inner: MemoryContentStore,
        work: std::path::PathBuf,
        peak_output_fds: AtomicUsize,
    }

    #[cfg(target_os = "linux")]
    impl ContentStore for OutputFdStore {
        async fn find_missing(
            &self,
            digests: &[Digest],
            missing: &mut Vec<Digest>,
        ) -> Result<(), PortError> {
            self.inner.find_missing(digests, missing).await
        }

        async fn read(
            &self,
            digest: Digest,
            range: ByteRange,
        ) -> Result<Option<BlobStream>, PortError> {
            self.inner.read(digest, range).await
        }

        async fn write(&self, digest: Digest, body: BlobStream) -> Result<(), PortError> {
            let open = fs::read_dir("/proc/self/fd")
                .map_err(PortError::backend)?
                .filter_map(Result::ok)
                .filter_map(|entry| fs::read_link(entry.path()).ok())
                .filter(|path| path.starts_with(&self.work))
                .count();
            self.peak_output_fds.fetch_max(open, Ordering::SeqCst);
            self.inner.write(digest, body).await
        }
    }

    #[cfg(target_os = "linux")]
    async fn upload_many_outputs(budget: u64) -> TestResult {
        let tmp = TempDir::new()?;
        let work = tmp.path().join("work");
        fs::create_dir_all(work.join("out/nested"))?;
        for index in 0..1500 {
            fs::write(
                work.join(format!("out/nested/f{index:04}")),
                format!("blob-{index}"),
            )?;
        }
        symlink("nested/f0000", work.join("out/link"))?;
        let slow = OutputFdStore {
            inner: MemoryContentStore::new(),
            work: work.canonicalize()?,
            peak_output_fds: AtomicUsize::new(0),
        };
        let tiered = Tiered::new(DiskCas::open(tmp.path().join("cas"), budget).await?, slow);
        let command = reapi::Command {
            output_paths: vec!["out".to_owned()],
            ..Default::default()
        };
        let clock = TestClock::new(Timestamp::from_unix_millis(1_000));
        let config = test_config();
        let outcome = upload_outputs(
            UploadInput {
                action_digest: Digest::from_bytes(b"many-outputs"),
                do_not_cache: false,
                exit_code: 0,
                command: &command,
                work_dir: &work,
                working_dir: &work,
                stdout: None,
                stderr: None,
                timing: ExecutionTiming::default(),
            },
            &tiered,
            Some(&MemoryActionStore::new()),
            &clock,
            &config,
        )
        .await?;
        assert!(outcome.cached_action_result);
        assert_eq!(
            tiered
                .disk()
                .pin_attempt(Digest::from_bytes(b"blob-0"), false)
                .is_some(),
            budget > 1,
            "fixture did not exercise the requested DiskCas admission path"
        );
        let peak = tiered.slow().peak_output_fds.load(Ordering::SeqCst);
        assert!(
            peak > 0,
            "descriptor measurement did not observe the output tree"
        );
        assert!(
            peak <= config.hash_concurrency.get() + 6,
            "unbounded output descriptors: {peak}"
        );
        assert_many_outputs(&tiered.slow().inner, &outcome.action_result)?;
        assert_eq!(outcome.cas_shared_bytes, 0);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn assert_many_outputs(slow: &MemoryContentStore, result: &reapi::ActionResult) -> TestResult {
        assert_eq!(result.output_directories.len(), 1);
        let digest = from_proto(
            result.output_directories[0]
                .tree_digest
                .as_ref()
                .ok_or("no Tree digest")?,
        )?;
        let tree = reapi::Tree::decode(slow.get_bytes(digest).ok_or("no Tree")?)?;
        let root = tree.root.ok_or("no root")?;
        assert_eq!(root.symlinks[0].target, "nested/f0000");
        assert_eq!(root.directories[0].name, "nested");
        assert_eq!(tree.children.len(), 1);
        let files = &tree.children[0].files;
        assert_eq!(files.len(), 1500);
        for (index, file) in files.iter().enumerate() {
            assert_eq!(file.name, format!("f{index:04}"));
            let digest = from_proto(file.digest.as_ref().ok_or("missing file digest")?)?;
            assert_eq!(
                slow.get_bytes(digest),
                Some(Bytes::from(format!("blob-{index}")))
            );
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn many_outputs_bound_descriptors_with_disk_cas_and_with_bypass() -> TestResult {
        for budget in [1, 1024 * 1024] {
            tokio::time::timeout(Duration::from_secs(30), upload_many_outputs(budget)).await??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn uploads_verified_handles_after_output_paths_are_replaced() -> TestResult {
        let tmp = TempDir::new()?;
        let work = tmp.path().join("work");
        let outside = tmp.path().join("outside");
        fs::create_dir_all(work.join("out"))?;
        fs::create_dir_all(&outside)?;
        fs::write(work.join("out/data"), b"verified output")?;
        fs::write(outside.join("data"), b"outside secret")?;
        let roots = super::RootContext::new(&work, &work)?;
        let directory = super::open_directory_path(&roots.working_fd, std::path::Path::new("out"))?;
        let Some(super::OpenOutput::File(file)) = super::open_output(
            &directory,
            std::path::Path::new("data"),
            &work.join("out/data"),
        )?
        else {
            return Err("expected verified file descriptor".into());
        };
        fs::rename(work.join("out"), work.join("saved"))?;
        symlink(&outside, work.join("out"))?;
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let slow = MemoryContentStore::new();
        let tiered = Tiered::new(disk.clone(), slow.clone());
        let config = test_config();
        let blocking = BlockingScope::new();
        let mut collector = super::FileCollector::new(&tiered, &config, &blocking, None);
        collector.push(work.join("out/data"), file).await?;
        collector.flush().await?;
        let files = &collector.files;
        assert_eq!(files[0].digest, Digest::from_bytes(b"verified output"));
        assert_eq!(
            slow.get_bytes(files[0].digest),
            Some(Bytes::from_static(b"verified output"))
        );
        assert!(disk.pin_attempt(files[0].digest, false).is_some());
        assert!(
            slow.get_bytes(Digest::from_bytes(b"outside secret"))
                .is_none()
        );
        Ok(())
    }

    #[allow(deprecated)]
    #[tokio::test]
    async fn rejects_intermediate_output_symlinks() -> TestResult {
        let tmp = TempDir::new()?;
        let work = tmp.path().join("work");
        let outside = tmp.path().join("outside");
        fs::create_dir_all(&work)?;
        fs::create_dir_all(outside.join("secret-dir"))?;
        let secret = b"host secret must never reach CAS";
        fs::write(outside.join("secret.txt"), secret)?;
        fs::write(outside.join("secret-dir/secret.txt"), secret)?;
        symlink(&outside, work.join("out"))?;
        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk, slow.clone());
        let clock = TestClock::new(Timestamp::from_unix_millis(1_000));
        for command in [
            reapi::Command {
                output_paths: vec!["out/secret.txt".into()],
                ..Default::default()
            },
            reapi::Command {
                output_paths: vec!["out/secret-dir".into()],
                ..Default::default()
            },
            reapi::Command {
                output_files: vec!["out/secret.txt".into()],
                ..Default::default()
            },
            reapi::Command {
                output_directories: vec!["out/secret-dir".into()],
                ..Default::default()
            },
        ] {
            let result = upload_outputs(
                UploadInput {
                    action_digest: Digest::EMPTY,
                    do_not_cache: true,
                    exit_code: 0,
                    command: &command,
                    work_dir: &work,
                    working_dir: &work,
                    stdout: None,
                    stderr: None,
                    timing: ExecutionTiming::default(),
                },
                &tiered,
                None::<&MemoryActionStore>,
                &clock,
                &test_config(),
            )
            .await;
            assert!(
                matches!(
                    result,
                    Err(UploadError::InvalidPath { .. } | UploadError::OutputKindMismatch { .. })
                ),
                "intermediate symlink accepted: {result:?}"
            );
            assert!(
                slow.get_bytes(Digest::from_bytes(secret)).is_none(),
                "outside secret was exfiltrated"
            );
        }
        Ok(())
    }

    #[allow(deprecated)]
    #[tokio::test]
    async fn rejects_output_type_mismatch() -> TestResult {
        let tmp = TempDir::new()?;
        let work_dir = tmp.path().join("attempt/work");
        fs::create_dir_all(work_dir.join("is-a-dir"))?;
        let slow = MemoryContentStore::new();
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let tiered = Tiered::new(disk, slow);
        let clock = TestClock::new(Timestamp::from_unix_millis(1_000));

        let mismatch_cmd = reapi::Command {
            output_files: vec!["is-a-dir".to_owned()],
            ..Default::default()
        };
        let err = upload_outputs(
            UploadInput {
                action_digest: Digest::from_bytes(b"mismatch"),
                do_not_cache: true,
                exit_code: 0,
                command: &mismatch_cmd,
                work_dir: &work_dir,
                working_dir: &work_dir,
                stdout: None,
                stderr: None,
                timing: ExecutionTiming::default(),
            },
            &tiered,
            None::<&MemoryActionStore>,
            &clock,
            &test_config(),
        )
        .await
        .expect_err("directory for output_files fails");
        assert!(matches!(err, UploadError::OutputKindMismatch { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_joins_blocking_hash_before_cleanup()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::future::Future as _;
        use std::task::Poll;

        struct ReleaseLatch {
            done: parking_lot::Mutex<bool>,
            cv: parking_lot::Condvar,
        }
        impl ReleaseLatch {
            fn wait(&self) {
                let mut guard = self.done.lock();
                while !*guard {
                    self.cv.wait(&mut guard);
                }
            }
            fn release(&self) {
                *self.done.lock() = true;
                self.cv.notify_all();
            }
        }
        struct LatchGuard(Arc<ReleaseLatch>);
        impl Drop for LatchGuard {
            fn drop(&mut self) {
                self.0.release();
            }
        }

        let tmp = tempfile::tempdir()?;
        let disk = DiskCas::open(tmp.path().join("cas"), 1024 * 1024).await?;
        let slow = MemoryContentStore::new();
        let tiered = Tiered::new(disk, slow.clone());
        let work_dir = tmp.path().join("work");
        fs::create_dir_all(&work_dir)?;
        let payload = vec![b'h'; STREAM_CHUNK_BYTES * 2 + 13];
        fs::write(work_dir.join("big.bin"), &payload)?;
        let payload_digest = Digest::from_bytes(&payload);

        let command = reapi::Command {
            output_paths: vec!["big.bin".to_owned()],
            ..Default::default()
        };
        let clock = gang_clock::SystemClock::new();
        let blocking = BlockingScope::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let latch = Arc::new(ReleaseLatch {
            done: parking_lot::Mutex::new(false),
            cv: parking_lot::Condvar::new(),
        });
        let _guard = LatchGuard(Arc::clone(&latch));
        let entered_hook = Arc::clone(&entered);
        let latch_hook = Arc::clone(&latch);
        let hook: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            entered_hook.notify_one();
            latch_hook.wait();
        });

        let upload_cfg = test_config();
        let upload_future = upload_outputs_inner(
            UploadInput {
                action_digest: Digest::from_bytes(b"cancel-hash"),
                do_not_cache: true,
                exit_code: 0,
                command: &command,
                work_dir: &work_dir,
                working_dir: &work_dir,
                stdout: None,
                stderr: None,
                timing: ExecutionTiming::default(),
            },
            &tiered,
            None::<&MemoryActionStore>,
            &clock,
            &upload_cfg,
            &blocking,
            Some(hook),
        );
        let outcome = tokio::select! {
            biased;
            () = entered.notified() => None,
            res = upload_future => Some(res),
        };
        assert!(outcome.is_none(), "cancellation must interrupt upload");

        let mut join_fut = std::pin::pin!(blocking.cancel_and_join());
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(join_fut.as_mut().poll(cx).is_pending())).await;
        latch.release();
        join_fut.await;

        assert!(
            pending,
            "cancel_and_join must remain Pending while blocking hash task is held"
        );

        let mut missing = Vec::new();
        slow.find_missing(&[payload_digest], &mut missing).await?;
        assert_eq!(
            missing,
            vec![payload_digest],
            "cancelled hash must never publish the blob"
        );
        Ok(())
    }
}
