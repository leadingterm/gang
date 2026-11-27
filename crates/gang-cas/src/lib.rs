//! Content-addressed storage mechanics for Gang's native execution engine.
//!
//! Provides the local disk CAS (`DiskCas`), single-flight deduplication, tiered fast/slow store
//! composition, directory tree cache, bounded zstd transport codec, and controller-backed remote
//! CAS gRPC client.

mod codec;
mod disk;
mod flight;
mod layout;
mod lru;
mod pins;
mod remote;
mod resource;
mod stats;
mod tiered;
mod trees;

pub use codec::{
    Compression, DEFAULT_ZSTD_LEVEL, STREAM_CHUNK_BYTES, compress_zstd, compress_zstd_stream,
    decompress_zstd_bounded, decompress_zstd_stream, verify_identity_stream,
};
pub use disk::{BlockingScope, DiskCas, WriteOutcome};
pub use flight::SingleFlight;
pub use layout::{
    DETERMINISTIC_MTIME_SECS, ScanReport, ScannedEntry, TMP_DIR_NAME, blob_path,
    deterministic_mtime, entry_file_name, file_mode, hardlink_or_reflink_file, parse_entry_name,
    scan_cas_root, shard_dir_name,
};
pub use lru::{BlobMode, CacheKey, EvictedEntry, InsertOutcome, PinKind, SHARD_COUNT, ShardedLru};
pub use pins::CasPin;
pub use remote::{RemoteCas, RemoteCasConfig};
pub use resource::{ResourceName, ResourceNameError};
pub use stats::CasStats;
pub use tiered::{FetchOutcome, Tiered};
pub use trees::{EvictedTree, TreeCache, TreeCacheConfig, TreeLease};
