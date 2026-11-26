//! Generated `build.*` protobuf and gRPC modules.

#![allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    rustdoc::broken_intra_doc_links,
    rustdoc::invalid_html_tags
)]

pub mod bazel {
    pub mod remote {
        pub mod execution {
            pub mod v2 {
                include!("build.bazel.remote.execution.v2.rs");
            }
        }
    }

    pub mod semver {
        include!("build.bazel.semver.rs");
    }
}
