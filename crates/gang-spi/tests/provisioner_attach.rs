//! The default provisioner attachment needs no sidecar.

use gang_spi::{ProvisionerPort, ServerEndpoints};
use gang_testkit::FakeProvisioner;
use gang_types::Url;

#[tokio::test]
async fn default_attach_has_nothing_to_attach() -> gang_testkit::TestResult {
    let endpoints = ServerEndpoints {
        public: Url::parse("https://public.example/")?,
        worker: Url::parse("https://worker.example/")?,
    };
    assert!(
        FakeProvisioner::default()
            .attach(&endpoints)
            .await?
            .is_none()
    );
    Ok(())
}
