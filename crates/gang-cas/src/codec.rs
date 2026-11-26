//! Bounded ZSTD transport codec and streaming SHA-256 verification.
//!
//! Storage in Gang is uncompressed; ZSTD is strictly a transport encoding. Every decoder enforces
//! a hard output limit of `digest.size().saturating_add(1)` bytes so a compression bomb fails
//! immediately with [`PortError::Rejected`] ([`Rejection::DigestMismatch`]) without buffering the
//! full payload.

use std::io;

use bytes::{Bytes, BytesMut};
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use gang_spi::{BlobStream, PortError, Rejection};
use gang_types::Digest;
use sha2::{Digest as _, Sha256};
use zstd::stream::raw::{Decoder, Encoder, InBuffer, Operation, OutBuffer};

/// Default ZSTD compression level for transport encoding.
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// Transport compression format supported by Gang CAS streams and batch RPCs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Compression {
    /// Uncompressed raw bytes.
    #[default]
    Identity,
    /// Zstandard compressed transport stream.
    Zstd,
}

/// Compresses `input` into a ZSTD frame at `level`.
///
/// # Errors
///
/// Returns [`PortError::Backend`] if the ZSTD encoder rejects the configuration or fails.
pub fn compress_zstd(input: &[u8], level: i32) -> Result<Bytes, PortError> {
    let encoded = zstd::stream::encode_all(input, level).map_err(PortError::backend)?;
    Ok(Bytes::from(encoded))
}

/// Compresses a stream of byte chunks into a ZSTD frame without buffering the full payload.
#[must_use]
pub fn compress_zstd_stream(
    input: BoxStream<'static, Result<Bytes, PortError>>,
    level: i32,
) -> BoxStream<'static, Result<Bytes, PortError>> {
    struct State {
        input: BoxStream<'static, Result<Bytes, PortError>>,
        encoder: Encoder<'static>,
        scratch: Vec<u8>,
        done: bool,
    }

    let encoder = Encoder::new(level);
    let state = match encoder {
        Ok(encoder) => State {
            input,
            encoder,
            scratch: vec![0_u8; STREAM_CHUNK_BYTES],
            done: false,
        },
        Err(err) => {
            return Box::pin(stream::once(std::future::ready(Err(PortError::backend(
                err,
            )))));
        }
    };

    Box::pin(stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }

        while let Some(next) = state.input.next().await {
            let chunk = match next {
                Ok(chunk) => chunk,
                Err(err) => {
                    state.done = true;
                    return Some((Err(err), state));
                }
            };
            if chunk.is_empty() {
                continue;
            }
            match encode_chunk(&mut state.encoder, &chunk, &mut state.scratch) {
                Ok(out) if !out.is_empty() => return Some((Ok(out), state)),
                Ok(_) => {}
                Err(err) => {
                    state.done = true;
                    return Some((Err(err), state));
                }
            }
        }

        // Input finished: flush and finish the ZSTD frame.
        state.done = true;
        match finish_encoder(&mut state.encoder, &mut state.scratch) {
            Ok(out) if !out.is_empty() => Some((Ok(out), state)),
            Ok(_) => None,
            Err(err) => Some((Err(err), state)),
        }
    }))
}

fn encode_chunk(
    encoder: &mut Encoder<'static>,
    input: &[u8],
    scratch: &mut [u8],
) -> Result<Bytes, PortError> {
    let mut out = BytesMut::new();
    let mut in_buf = InBuffer::around(input);
    while in_buf.pos() < input.len() {
        let mut out_buf = OutBuffer::around(scratch);
        encoder
            .run(&mut in_buf, &mut out_buf)
            .map_err(PortError::backend)?;
        let written = out_buf.as_slice();
        if !written.is_empty() {
            out.extend_from_slice(written);
        }
    }
    Ok(out.freeze())
}

fn finish_encoder(encoder: &mut Encoder<'static>, scratch: &mut [u8]) -> Result<Bytes, PortError> {
    let mut out = BytesMut::new();
    loop {
        let mut out_buf = OutBuffer::around(scratch);
        let remaining = encoder
            .finish(&mut out_buf, true)
            .map_err(PortError::backend)?;
        let written = out_buf.as_slice();
        if !written.is_empty() {
            out.extend_from_slice(written);
        }
        if remaining == 0 {
            break;
        }
    }
    Ok(out.freeze())
}

struct BoundedDecoder {
    decoder: Decoder<'static>,
    scratch: Vec<u8>,
    hasher: Sha256,
    decoded_bytes: u64,
    expected: Digest,
    frame_remaining: usize,
}

impl BoundedDecoder {
    fn new(expected: Digest) -> Result<Self, PortError> {
        let decoder = Decoder::new().map_err(PortError::backend)?;
        Ok(Self {
            decoder,
            scratch: vec![0_u8; STREAM_CHUNK_BYTES],
            hasher: Sha256::new(),
            decoded_bytes: 0,
            expected,
            frame_remaining: 0,
        })
    }

    fn decode_chunk(&mut self, chunk: &[u8]) -> Result<Bytes, PortError> {
        let mut out = BytesMut::new();
        let mut in_buf = InBuffer::around(chunk);
        let hard_cap = self.expected.size().saturating_add(1);

        loop {
            let before_in = in_buf.pos();
            let allow = allowed_write_len(self.decoded_bytes, hard_cap, self.scratch.len());
            let mut out_buf = OutBuffer::around(&mut self.scratch[..allow]);
            self.frame_remaining = self
                .decoder
                .run(&mut in_buf, &mut out_buf)
                .map_err(map_zstd_decode_err)?;

            let written = out_buf.as_slice();
            if !written.is_empty() {
                let written_u64 = u64::try_from(written.len()).unwrap_or(u64::MAX);
                self.decoded_bytes = self.decoded_bytes.saturating_add(written_u64);
                if self.decoded_bytes > self.expected.size() {
                    return Err(PortError::refused(Rejection::DigestMismatch));
                }
                self.hasher.update(written);
                out.extend_from_slice(written);
            }

            let consumed = in_buf.pos() > before_in;
            let produced = !written.is_empty();
            if in_buf.pos() >= chunk.len() && (self.frame_remaining == 0 || !produced) {
                break;
            }
            if !consumed && !produced {
                break;
            }
        }

        Ok(out.freeze())
    }

    fn finish(&mut self) -> Result<Bytes, PortError> {
        let tail = if self.frame_remaining != 0 {
            self.decode_chunk(&[])?
        } else {
            Bytes::new()
        };
        if self.frame_remaining != 0 || self.decoded_bytes != self.expected.size() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }
        let actual_hash: [u8; 32] = self.hasher.finalize_reset().into();
        if &actual_hash != self.expected.hash() {
            return Err(PortError::refused(Rejection::DigestMismatch));
        }
        Ok(tail)
    }
}

/// Decompresses `compressed` into memory while enforcing a hard cap at
/// `digest.size().saturating_add(1)` bytes and verifying the SHA-256 [`Digest`].
///
/// Reaches at most `digest.size() + 1` decompressed bytes before aborting immediately with
/// [`PortError::Rejected`] ([`Rejection::DigestMismatch`]).
///
/// # Errors
///
/// Returns [`PortError::Rejected`] with [`Rejection::DigestMismatch`] on decompression bomb,
/// size mismatch, SHA-256 mismatch, truncated ZSTD frame, or malformed ZSTD data.
pub fn decompress_zstd_bounded(digest: Digest, compressed: &[u8]) -> Result<Bytes, PortError> {
    let mut decoder = BoundedDecoder::new(digest)?;
    let first = decoder.decode_chunk(compressed)?;
    let tail = decoder.finish()?;
    if tail.is_empty() {
        return Ok(first);
    }
    let mut out = BytesMut::with_capacity(first.len() + tail.len());
    out.extend_from_slice(&first);
    out.extend_from_slice(&tail);
    Ok(out.freeze())
}

/// Streams and decompresses ZSTD chunks while enforcing a hard cap at
/// `digest.size().saturating_add(1)` bytes and verifying the SHA-256 [`Digest`] on the fly.
///
/// Never buffers the full payload. Any stream exceeding `digest.size()` fails immediately upon
/// reaching the `digest.size().saturating_add(1)`th decompressed byte with
/// [`PortError::Rejected`] ([`Rejection::DigestMismatch`]).
#[must_use]
pub fn decompress_zstd_stream(
    digest: Digest,
    compressed: BoxStream<'static, Result<Bytes, PortError>>,
) -> BlobStream {
    struct State {
        input: BoxStream<'static, Result<Bytes, PortError>>,
        decoder: BoundedDecoder,
        done: bool,
    }

    let decoder = match BoundedDecoder::new(digest) {
        Ok(decoder) => decoder,
        Err(err) => {
            return BlobStream {
                size: digest.size(),
                chunks: Box::pin(stream::once(std::future::ready(Err(err)))),
            };
        }
    };

    let state = State {
        input: compressed,
        decoder,
        done: false,
    };

    let chunks = Box::pin(stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }
        while let Some(next) = state.input.next().await {
            let chunk = match next {
                Ok(chunk) => chunk,
                Err(err) => {
                    state.done = true;
                    return Some((Err(err), state));
                }
            };
            if chunk.is_empty() {
                continue;
            }
            match state.decoder.decode_chunk(&chunk) {
                Ok(out) if !out.is_empty() => return Some((Ok(out), state)),
                Ok(_) => {}
                Err(err) => {
                    state.done = true;
                    return Some((Err(err), state));
                }
            }
        }

        state.done = true;
        match state.decoder.finish() {
            Ok(tail) if !tail.is_empty() => Some((Ok(tail), state)),
            Ok(_) => None,
            Err(err) => Some((Err(err), state)),
        }
    }));

    BlobStream {
        size: digest.size(),
        chunks,
    }
}

/// Streams uncompressed chunks while verifying exact byte count and SHA-256 against `digest`.
#[must_use]
pub fn verify_identity_stream(
    digest: Digest,
    body: BoxStream<'static, Result<Bytes, PortError>>,
) -> BlobStream {
    struct State {
        input: BoxStream<'static, Result<Bytes, PortError>>,
        hasher: Sha256,
        seen: u64,
        expected: Digest,
        done: bool,
    }

    let state = State {
        input: body,
        hasher: Sha256::new(),
        seen: 0,
        expected: digest,
        done: false,
    };

    let chunks = Box::pin(stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }
        while let Some(next) = state.input.next().await {
            let chunk = match next {
                Ok(chunk) => chunk,
                Err(err) => {
                    state.done = true;
                    return Some((Err(err), state));
                }
            };
            if chunk.is_empty() {
                continue;
            }
            let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            let next_seen = state.seen.saturating_add(len);
            if next_seen > state.expected.size() {
                state.done = true;
                return Some((Err(PortError::refused(Rejection::DigestMismatch)), state));
            }
            state.seen = next_seen;
            state.hasher.update(&chunk);
            return Some((Ok(chunk), state));
        }

        state.done = true;
        if state.seen != state.expected.size() {
            return Some((Err(PortError::refused(Rejection::DigestMismatch)), state));
        }
        let actual_hash: [u8; 32] = state.hasher.finalize_reset().into();
        if &actual_hash != state.expected.hash() {
            return Some((Err(PortError::refused(Rejection::DigestMismatch)), state));
        }
        None
    }));

    BlobStream {
        size: digest.size(),
        chunks,
    }
}

fn allowed_write_len(decoded_bytes: u64, hard_cap: u64, scratch_len: usize) -> usize {
    let remaining_cap = hard_cap.saturating_sub(decoded_bytes).max(1);
    usize::try_from(remaining_cap)
        .unwrap_or(scratch_len)
        .min(scratch_len)
}

fn map_zstd_decode_err(err: io::Error) -> PortError {
    PortError::rejected(Rejection::DigestMismatch, err)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use futures::StreamExt as _;
    use futures::stream;
    use gang_spi::{PortError, Rejection};
    use gang_types::Digest;

    use super::{
        DEFAULT_ZSTD_LEVEL, compress_zstd, compress_zstd_stream, decompress_zstd_bounded,
        decompress_zstd_stream, verify_identity_stream,
    };

    fn assert_digest_mismatch<T: std::fmt::Debug>(result: &Result<T, PortError>) {
        assert!(
            matches!(
                result,
                Err(PortError::Rejected {
                    reason: Rejection::DigestMismatch,
                    ..
                })
            ),
            "expected Rejection::DigestMismatch, got {result:?}"
        );
    }

    #[tokio::test]
    async fn roundtrips_empty_small_and_streaming_payloads() {
        for payload in [
            Bytes::new(),
            Bytes::from_static(b"hello gang zstd"),
            Bytes::from(vec![0x5a_u8; 256 * 1024 + 13]),
        ] {
            let digest = Digest::from_bytes(&payload);
            let compressed = compress_zstd(&payload, DEFAULT_ZSTD_LEVEL).expect("compress");
            let decoded = decompress_zstd_bounded(digest, &compressed).expect("decompress bounded");
            assert_eq!(decoded, payload);

            // Split compressed bytes into small chunks to exercise streaming decoder state machine.
            let chunks: Vec<Result<Bytes, PortError>> = compressed
                .chunks(17)
                .map(|slice| Ok(Bytes::copy_from_slice(slice)))
                .collect();
            let stream = decompress_zstd_stream(digest, Box::pin(stream::iter(chunks)));
            assert_eq!(stream.size, digest.size());
            let streamed = stream.collect().await.expect("decompress stream");
            assert_eq!(streamed, payload);

            // Also verify streaming compressor round-trips into streaming decompressor.
            let raw_chunks: Vec<Result<Bytes, PortError>> = payload
                .chunks(1024)
                .map(|slice| Ok(Bytes::copy_from_slice(slice)))
                .collect();
            let comp_stream =
                compress_zstd_stream(Box::pin(stream::iter(raw_chunks)), DEFAULT_ZSTD_LEVEL);
            let roundtrip = decompress_zstd_stream(digest, comp_stream)
                .collect()
                .await
                .expect("stream-to-stream roundtrip");
            assert_eq!(roundtrip, payload);
        }
    }

    #[tokio::test]
    async fn rejects_decompression_bomb_at_size_plus_one_without_buffering() {
        // 4 MiB of zeros compresses into a tiny zstd payload (~200 bytes).
        let actual = vec![0_u8; 4 * 1024 * 1024];
        let compressed = compress_zstd(&actual, DEFAULT_ZSTD_LEVEL).expect("compress bomb");

        // Claim the digest size is only 100 bytes (even with a bogus hash).
        let claimed = Digest::new([0x11_u8; 32], 100);
        assert_digest_mismatch(&decompress_zstd_bounded(claimed, &compressed));

        // Streaming decoder must fail immediately upon hitting byte 101 and never pull later
        // trailing chunks or buffer the 4 MiB payload.
        let chunks_pulled = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&chunks_pulled);
        let compressed_clone = compressed.clone();
        let bomb_stream = stream::iter([
            Ok(compressed_clone),
            Ok(Bytes::from_static(b"never-pulled")),
        ])
        .inspect(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
        });

        let mut decoded = decompress_zstd_stream(claimed, Box::pin(bomb_stream));
        let mut total_emitted = 0_usize;
        let mut saw_err = None;
        while let Some(item) = decoded.chunks.next().await {
            match item {
                Ok(chunk) => total_emitted += chunk.len(),
                Err(err) => {
                    saw_err = Some(err);
                    break;
                }
            }
        }
        assert_digest_mismatch(&saw_err.map_or(Ok(()), Err));
        assert!(
            total_emitted <= 100,
            "stream emitted {total_emitted} bytes before aborting"
        );
        assert_eq!(chunks_pulled.load(Ordering::SeqCst), 1);

        // Zero-byte claimed digest against non-empty compressed bomb must also fail at byte 1.
        let empty_claimed = Digest::EMPTY;
        assert_digest_mismatch(&decompress_zstd_bounded(empty_claimed, &compressed));
        let zero_stream = decompress_zstd_stream(
            empty_claimed,
            Box::pin(stream::once(std::future::ready(Ok(compressed)))),
        );
        assert_digest_mismatch(&zero_stream.collect().await);
    }

    #[tokio::test]
    async fn rejects_truncated_corrupt_and_wrong_hash_streams() {
        let payload = b"0123456789abcdef0123456789abcdef";
        let digest = Digest::from_bytes(payload);
        let compressed = compress_zstd(payload, DEFAULT_ZSTD_LEVEL).expect("compress");

        // Truncated zstd frame.
        let truncated = compressed.slice(..compressed.len() / 2);
        assert_digest_mismatch(&decompress_zstd_bounded(digest, &truncated));
        let truncated_stream = decompress_zstd_stream(
            digest,
            Box::pin(stream::once(std::future::ready(Ok(truncated)))),
        );
        assert_digest_mismatch(&truncated_stream.collect().await);

        // Valid zstd frame of shorter length than declared digest.
        let shorter = compress_zstd(b"short", DEFAULT_ZSTD_LEVEL).expect("compress short");
        assert_digest_mismatch(&decompress_zstd_bounded(digest, &shorter));
        let shorter_stream = decompress_zstd_stream(
            digest,
            Box::pin(stream::once(std::future::ready(Ok(shorter)))),
        );
        assert_digest_mismatch(&shorter_stream.collect().await);

        // Same size, wrong SHA-256 hash.
        let wrong_digest = Digest::new([0xff_u8; 32], digest.size());
        assert_digest_mismatch(&decompress_zstd_bounded(wrong_digest, &compressed));
        let wrong_stream = decompress_zstd_stream(
            wrong_digest,
            Box::pin(stream::once(std::future::ready(Ok(compressed)))),
        );
        assert_digest_mismatch(&wrong_stream.collect().await);

        // Corrupt bytes.
        let corrupt = Bytes::from_static(b"not valid zstd data at all");
        assert_digest_mismatch(&decompress_zstd_bounded(digest, &corrupt));

        // Identity verifier mismatch.
        let id_bad = verify_identity_stream(
            wrong_digest,
            Box::pin(stream::once(std::future::ready(Ok(Bytes::from_static(
                payload,
            ))))),
        );
        assert_digest_mismatch(&id_bad.collect().await);
    }
}
