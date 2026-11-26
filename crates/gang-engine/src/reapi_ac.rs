//! `REAPI v2` `ActionCache` gRPC service over [`gang_spi::ActionStore`].
//!
//! Implements `GetActionResult` and `UpdateActionResult` (client updates enabled, matching Gang's
//! `read_only: false` policy without completeness checking).

use std::sync::Arc;

use bytes::Bytes;
use gang_reapi::build::bazel::remote::execution::v2::action_cache_server::{
    ActionCache, ActionCacheServer,
};
use gang_reapi::build::bazel::remote::execution::v2::{
    ActionResult, GetActionResultRequest, UpdateActionResultRequest,
};
use gang_spi::ActionStore;
use prost::Message as _;
use tonic::{Request, Response, Status};

use crate::reapi_cas::{map_port_error, parse_proto_digest, validate_sha256_function};

/// `REAPI v2` `ActionCache` gRPC server backed by an [`ActionStore`].
#[derive(Debug, Clone)]
pub struct ActionCacheService<A> {
    actions: Arc<A>,
}

impl<A: ActionStore> ActionCacheService<A> {
    /// Creates an [`ActionCacheService`] backed by `actions`.
    #[must_use]
    pub const fn new(actions: Arc<A>) -> Self {
        Self { actions }
    }

    /// Wraps this service in a tonic [`ActionCacheServer`].
    #[must_use]
    pub fn into_server(self) -> ActionCacheServer<Self> {
        ActionCacheServer::new(self)
    }
}

fn apply_inline_hints(result: &mut ActionResult, req: &GetActionResultRequest) {
    if !req.inline_stdout && result.stdout_digest.is_some() {
        result.stdout_raw.clear();
    }
    if !req.inline_stderr && result.stderr_digest.is_some() {
        result.stderr_raw.clear();
    }
    for file in &mut result.output_files {
        if file.digest.is_some()
            && !req
                .inline_output_files
                .iter()
                .any(|path| path == &file.path)
        {
            file.contents.clear();
        }
    }
}

#[tonic::async_trait]
impl<A: ActionStore> ActionCache for ActionCacheService<A> {
    async fn get_action_result(
        &self,
        request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let req = request.into_inner();
        validate_sha256_function(req.digest_function)?;
        let digest = parse_proto_digest(req.action_digest.as_ref(), "action_digest")?;

        let Some(raw) = self
            .actions
            .get(digest)
            .await
            .map_err(|err| map_port_error(&err))?
        else {
            return Err(Status::not_found(format!(
                "action result not found for {digest}"
            )));
        };

        let mut result = ActionResult::decode(raw)
            .map_err(|err| Status::internal(format!("corrupt cached ActionResult: {err}")))?;
        apply_inline_hints(&mut result, &req);
        Ok(Response::new(result))
    }

    async fn update_action_result(
        &self,
        request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let req = request.into_inner();
        validate_sha256_function(req.digest_function)?;
        let digest = parse_proto_digest(req.action_digest.as_ref(), "action_digest")?;
        let action_result = req
            .action_result
            .ok_or_else(|| Status::invalid_argument("missing action_result"))?;

        let encoded = Bytes::from(action_result.encode_to_vec());
        self.actions
            .put(digest, encoded)
            .await
            .map_err(|err| map_port_error(&err))?;

        Ok(Response::new(action_result))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use gang_reapi::build::bazel::remote::execution::v2::action_cache_server::ActionCache;
    use gang_reapi::build::bazel::remote::execution::v2::{
        ActionResult, Digest as ProtoDigest, GetActionResultRequest, OutputFile,
        UpdateActionResultRequest, digest_function,
    };
    use gang_reapi::to_proto;
    use gang_spi::PortError;
    use gang_testkit::MemoryActionStore;
    use gang_types::Digest;
    use tonic::{Code, Request};

    use super::ActionCacheService;

    #[tokio::test]
    async fn get_and_update_roundtrip_and_handle_misses_and_invalid_digests() {
        let store = Arc::new(MemoryActionStore::new());
        let svc = ActionCacheService::new(Arc::clone(&store));
        let action_digest = Digest::from_bytes(b"action-1");
        let proto_digest = to_proto(&action_digest).expect("digest");

        // Miss -> NOT_FOUND.
        let miss = svc
            .get_action_result(Request::new(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(proto_digest.clone()),
                inline_stdout: false,
                inline_stderr: false,
                inline_output_files: Vec::new(),
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect_err("cache miss");
        assert_eq!(miss.code(), Code::NotFound);

        // UpdateActionResult stores encoded ActionResult.
        let stdout_digest = Digest::from_bytes(b"out");
        let stderr_digest = Digest::from_bytes(b"err");
        let file_digest = Digest::from_bytes(b"file-body");
        let result = ActionResult {
            exit_code: 0,
            stdout_raw: Bytes::from_static(b"out"),
            stdout_digest: Some(to_proto(&stdout_digest).expect("stdout")),
            stderr_raw: Bytes::from_static(b"err"),
            stderr_digest: Some(to_proto(&stderr_digest).expect("stderr")),
            output_files: vec![OutputFile {
                path: "bin/app".to_owned(),
                digest: Some(to_proto(&file_digest).expect("file")),
                is_executable: true,
                contents: Bytes::from_static(b"file-body"),
                node_properties: None,
            }],
            ..Default::default()
        };
        let updated = svc
            .update_action_result(Request::new(UpdateActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(proto_digest.clone()),
                action_result: Some(result.clone()),
                results_cache_policy: None,
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect("update")
            .into_inner();
        assert_eq!(updated, result);
        assert!(store.contains(action_digest));

        // Get without inline hints strips digest-backed raw fields; with hints preserves them.
        let non_inlined = svc
            .get_action_result(Request::new(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(proto_digest.clone()),
                inline_stdout: false,
                inline_stderr: false,
                inline_output_files: Vec::new(),
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect("get hit")
            .into_inner();
        assert!(non_inlined.stdout_raw.is_empty());
        assert!(non_inlined.stderr_raw.is_empty());
        assert!(non_inlined.output_files[0].contents.is_empty());

        let inlined = svc
            .get_action_result(Request::new(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(proto_digest),
                inline_stdout: true,
                inline_stderr: true,
                inline_output_files: vec!["bin/app".to_owned()],
                digest_function: digest_function::Value::Unknown as i32,
            }))
            .await
            .expect("get hit inlined")
            .into_inner();
        assert_eq!(inlined, result);
    }

    #[tokio::test]
    async fn get_rejects_invalid_digest_and_propagates_store_error() {
        let store = Arc::new(MemoryActionStore::new());
        let svc = ActionCacheService::new(Arc::clone(&store));
        let action_digest = Digest::from_bytes(b"action-2");

        // Malformed digest -> INVALID_ARGUMENT.
        let bad_digest = svc
            .get_action_result(Request::new(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(ProtoDigest {
                    hash: "short".to_owned(),
                    size_bytes: 1,
                }),
                inline_stdout: false,
                inline_stderr: false,
                inline_output_files: Vec::new(),
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect_err("bad digest");
        assert_eq!(bad_digest.code(), Code::InvalidArgument);

        // Store backend error propagates as UNAVAILABLE/INTERNAL.
        store.fail_next_get(PortError::unavailable("transient"));
        let err = svc
            .get_action_result(Request::new(GetActionResultRequest {
                instance_name: String::new(),
                action_digest: Some(to_proto(&action_digest).expect("digest")),
                inline_stdout: false,
                inline_stderr: false,
                inline_output_files: Vec::new(),
                digest_function: digest_function::Value::Sha256 as i32,
            }))
            .await
            .expect_err("unavailable");
        assert_eq!(err.code(), Code::Unavailable);
    }
}
