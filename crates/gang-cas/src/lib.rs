//! Content-addressed storage mechanics for Gang's native execution engine.
//!
//! Provides the local disk CAS (`DiskCas`), single-flight deduplication, tiered fast/slow store
//! composition, directory tree cache, bounded zstd transport codec, and controller-backed remote
//! CAS gRPC client.
