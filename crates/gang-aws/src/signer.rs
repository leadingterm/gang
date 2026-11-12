//! The certificate authority's key in AWS KMS. The key is pinned by ARN on first use, so an
//! alias that is repointed while the process runs cannot make signatures disagree with the
//! public key already read.

use aws_sdk_kms::types::{KeySpec, KeyUsageType, MessageType, SigningAlgorithmSpec};
use aws_smithy_types::Blob;
use gang_spi::{CertificateSignerPort, Plugin, PluginContext, PortError, SignatureAlgorithm};
use tokio::sync::OnceCell;

use crate::config::KmsSignerConfig;
use crate::errors::{AwsError, sdk_error};
use crate::sdk::load;

const GET_PUBLIC_KEY: &str = "GetPublicKey";
const SIGN: &str = "Sign";
const KEY_NOT_P256: &str = "the key is not an ecc nist p-256 key";
const KEY_NOT_SIGNING: &str = "the key is not a signing key";
const KEY_NOT_ECDSA_SHA256: &str = "the key does not support ecdsa sha-256";

/// The signing key of the certificate authority, held in AWS KMS. The key never leaves KMS.
#[derive(Debug)]
pub struct KmsSigner {
    config: KmsSignerConfig,
    client: OnceCell<aws_sdk_kms::Client>,
    key: OnceCell<PinnedKey>,
}

/// The key as it was first resolved: its ARN and its public key.
#[derive(Debug)]
struct PinnedKey {
    arn: String,
    public_key_der: Vec<u8>,
}

impl Plugin for KmsSigner {
    type Config = KmsSignerConfig;

    fn build(config: KmsSignerConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self {
            config,
            client: OnceCell::new(),
            key: OnceCell::new(),
        })
    }
}

impl KmsSigner {
    /// A signer that uses `client` instead of loading one from the configuration.
    #[cfg(test)]
    fn with_client(config: KmsSignerConfig, client: aws_sdk_kms::Client) -> Self {
        Self {
            config,
            client: OnceCell::new_with(Some(client)),
            key: OnceCell::new(),
        }
    }

    async fn client(&self) -> &aws_sdk_kms::Client {
        self.client
            .get_or_init(|| async {
                aws_sdk_kms::Client::new(&load(&self.config.region, &self.config.client).await)
            })
            .await
    }

    async fn pinned(&self) -> Result<&PinnedKey, AwsError> {
        self.key.get_or_try_init(|| self.fetch_key()).await
    }

    async fn fetch_key(&self) -> Result<PinnedKey, AwsError> {
        let key_id = self.config.key_id.as_str();
        let response = self
            .client()
            .await
            .get_public_key()
            .key_id(key_id)
            .send()
            .await
            .map_err(|e| sdk_error(GET_PUBLIC_KEY, e))?;

        if response.key_spec() != Some(&KeySpec::EccNistP256) {
            return Err(unsuitable(key_id, KEY_NOT_P256));
        }
        if response.key_usage() != Some(&KeyUsageType::SignVerify) {
            return Err(unsuitable(key_id, KEY_NOT_SIGNING));
        }
        if !response
            .signing_algorithms()
            .contains(&SigningAlgorithmSpec::EcdsaSha256)
        {
            return Err(unsuitable(key_id, KEY_NOT_ECDSA_SHA256));
        }

        let arn = response
            .key_id()
            .ok_or_else(|| AwsError::malformed(GET_PUBLIC_KEY, "key arn"))?;
        let public_key = response
            .public_key()
            .ok_or_else(|| AwsError::malformed(GET_PUBLIC_KEY, "public key"))?;

        Ok(PinnedKey {
            arn: arn.to_owned(),
            public_key_der: public_key.as_ref().to_vec(),
        })
    }
}

fn unsuitable(key_id: &str, reason: &'static str) -> AwsError {
    AwsError::UnsuitableKey {
        key_id: key_id.to_owned(),
        reason,
    }
}

impl CertificateSignerPort for KmsSigner {
    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::EcdsaP256Sha256
    }

    async fn public_key(&self) -> Result<Vec<u8>, PortError> {
        Ok(self.pinned().await?.public_key_der.clone())
    }

    async fn sign(&self, message: &[u8]) -> Result<Vec<u8>, PortError> {
        let key = self.pinned().await?;
        let response = self
            .client()
            .await
            .sign()
            .key_id(key.arn.as_str())
            .message(Blob::new(message))
            .message_type(MessageType::Raw)
            .signing_algorithm(SigningAlgorithmSpec::EcdsaSha256)
            .send()
            .await
            .map_err(|e| sdk_error(SIGN, e))?;

        let signature = response
            .signature()
            .ok_or_else(|| AwsError::malformed(SIGN, "signature"))?;
        Ok(signature.as_ref().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use base64::Engine as _;
    use gang_testkit::{AUTHORITY_SEED, signer_contract::signer_contract, signing_key};
    use p256::ecdsa::Signature;
    use p256::ecdsa::signature::hazmat::PrehashVerifier as _;
    use p256::pkcs8::EncodePublicKey as _;
    use serde_json::json;

    use super::*;
    use crate::fake_aws::{FakeAws, FakeFailure, FakeKey};

    const ARN: &str = "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000";
    const ALIAS: &str = "alias/gang-ca";
    const OTHER_ALIAS: &str = "alias/elsewhere";
    const UNKNOWN_ALIAS: &str = "alias/missing";

    fn kms_config(key_id: &str) -> Result<KmsSignerConfig, Box<dyn std::error::Error>> {
        let config = serde_json::from_value(json!({
            "region": "us-east-1",
            "key_id": key_id,
        }))?;
        Ok(config)
    }

    fn ecdsa_key() -> Result<FakeKey, Box<dyn std::error::Error>> {
        Ok(FakeKey {
            arn: ARN.to_owned(),
            alias: ALIAS.to_owned(),
            key_spec: "ECC_NIST_P256".to_owned(),
            key_usage: "SIGN_VERIFY".to_owned(),
            signing_algorithms: vec!["ECDSA_SHA_256".to_owned()],
            signing_key: signing_key(AUTHORITY_SEED)?,
        })
    }

    async fn fake_with(key: FakeKey) -> Result<FakeAws, std::io::Error> {
        let fake = FakeAws::start().await?;
        fake.state().kms = Some(key);
        Ok(fake)
    }

    async fn signer_for(
        fake: &FakeAws,
        key_id: &str,
    ) -> Result<KmsSigner, Box<dyn std::error::Error>> {
        Ok(KmsSigner::with_client(
            kms_config(key_id)?,
            aws_sdk_kms::Client::new(&fake.sdk_config().await),
        ))
    }

    fn count_requests(fake: &FakeAws, operation: &str) -> usize {
        fake.state()
            .requests
            .iter()
            .filter(|request| request.operation == operation)
            .count()
    }

    fn last_parameters(fake: &FakeAws, operation: &str) -> Option<BTreeMap<String, String>> {
        fake.state()
            .requests
            .iter()
            .rev()
            .find(|request| request.operation == operation)
            .map(|request| request.parameters.clone())
    }

    #[tokio::test]
    async fn public_key_is_fetched_once() -> Result<(), Box<dyn std::error::Error>> {
        let key = ecdsa_key()?;
        let expected = key.signing_key.verifying_key().to_public_key_der()?;
        let fake = fake_with(key).await?;
        let signer = signer_for(&fake, ALIAS).await?;

        assert_eq!(signer.public_key().await?, expected.as_ref());
        assert_eq!(signer.public_key().await?, expected.as_ref());
        assert_eq!(count_requests(&fake, GET_PUBLIC_KEY), 1);
        Ok(())
    }

    #[tokio::test]
    async fn sign_sends_the_raw_message_and_algorithm() -> Result<(), Box<dyn std::error::Error>> {
        let fake = fake_with(ecdsa_key()?).await?;
        let signer = signer_for(&fake, ALIAS).await?;

        signer.sign(b"hello").await?;

        let parameters = last_parameters(&fake, SIGN).ok_or("no Sign request was recorded")?;
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"hello");
        assert_eq!(parameters.get("KeyId").map(String::as_str), Some(ARN));
        assert_eq!(
            parameters.get("MessageType").map(String::as_str),
            Some("RAW")
        );
        assert_eq!(
            parameters.get("SigningAlgorithm").map(String::as_str),
            Some("ECDSA_SHA_256")
        );
        assert_eq!(
            parameters.get("Message").map(String::as_str),
            Some(encoded.as_str())
        );
        Ok(())
    }

    #[tokio::test]
    async fn signer_satisfies_contract() -> Result<(), Box<dyn std::error::Error>> {
        let fake = fake_with(ecdsa_key()?).await?;
        let signer = signer_for(&fake, ALIAS).await?;
        signer_contract(&signer).await
    }

    #[tokio::test]
    async fn fake_signs_a_digest_as_prehash() -> Result<(), Box<dyn std::error::Error>> {
        let key = ecdsa_key()?;
        let verifying_key = *key.signing_key.verifying_key();
        let fake = fake_with(key).await?;
        let client = aws_sdk_kms::Client::new(&fake.sdk_config().await);
        let digest = [42_u8; 32];
        let response = client
            .sign()
            .key_id(ARN)
            .message(Blob::new(digest))
            .message_type(MessageType::Digest)
            .signing_algorithm(SigningAlgorithmSpec::EcdsaSha256)
            .send()
            .await?;
        let signature =
            Signature::from_der(response.signature().ok_or("missing signature")?.as_ref())?;
        verifying_key.verify_prehash(&digest, &signature)?;
        Ok(())
    }

    #[tokio::test]
    async fn signing_uses_the_pinned_arn_after_the_alias_is_repointed()
    -> Result<(), Box<dyn std::error::Error>> {
        let fake = fake_with(ecdsa_key()?).await?;
        let signer = signer_for(&fake, ALIAS).await?;
        signer.sign(b"first").await?;

        fake.state()
            .kms
            .as_mut()
            .ok_or("kms key missing from fake")?
            .alias = OTHER_ALIAS.to_owned();
        signer.sign(b"second").await?;

        let parameters = last_parameters(&fake, SIGN).ok_or("no Sign request was recorded")?;
        assert_eq!(parameters.get("KeyId").map(String::as_str), Some(ARN));
        assert_eq!(count_requests(&fake, GET_PUBLIC_KEY), 1);
        Ok(())
    }

    #[tokio::test]
    async fn rsa_key_is_rejected_as_configuration() -> Result<(), Box<dyn std::error::Error>> {
        let fake = fake_with(FakeKey {
            key_spec: "RSA_2048".to_owned(),
            ..ecdsa_key()?
        })
        .await?;
        let signer = signer_for(&fake, ALIAS).await?;

        assert!(matches!(
            signer.public_key().await,
            Err(PortError::Config(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn encrypt_decrypt_key_is_unsuitable() -> Result<(), Box<dyn std::error::Error>> {
        let fake = fake_with(FakeKey {
            key_usage: "ENCRYPT_DECRYPT".to_owned(),
            ..ecdsa_key()?
        })
        .await?;
        let signer = signer_for(&fake, ALIAS).await?;

        assert!(matches!(
            signer.pinned().await,
            Err(AwsError::UnsuitableKey { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn unknown_key_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let fake = fake_with(ecdsa_key()?).await?;
        let signer = signer_for(&fake, UNKNOWN_ALIAS).await?;

        assert!(matches!(
            signer.pinned().await,
            Err(AwsError::Refused { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn throttled_sign_is_unavailable() -> Result<(), Box<dyn std::error::Error>> {
        let fake = fake_with(ecdsa_key()?).await?;
        fake.state().failures.insert(
            SIGN,
            FakeFailure {
                status: 400,
                code: "ThrottlingException",
                message: "rate exceeded",
            },
        );
        let signer = signer_for(&fake, ALIAS).await?;

        assert!(matches!(
            signer.sign(b"hello").await,
            Err(PortError::Unavailable(_))
        ));
        Ok(())
    }

    #[test]
    fn empty_key_id_is_rejected_at_deserialization() {
        let config = json!({ "region": "us-east-1", "key_id": "" });
        assert!(serde_json::from_value::<KmsSignerConfig>(config).is_err());
    }

    #[tokio::test]
    async fn p256_key_without_ecdsa_sha_256_is_unsuitable() -> Result<(), Box<dyn std::error::Error>>
    {
        let fake = fake_with(FakeKey {
            signing_algorithms: vec!["ECDSA_SHA_384".to_owned()],
            ..ecdsa_key()?
        })
        .await?;
        let signer = signer_for(&fake, ALIAS).await?;

        assert!(matches!(
            signer.pinned().await,
            Err(AwsError::UnsuitableKey { .. })
        ));
        assert!(matches!(
            signer.public_key().await,
            Err(PortError::Config(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn key_offering_ecdsa_sha_256_and_384_is_accepted()
    -> Result<(), Box<dyn std::error::Error>> {
        let fake = fake_with(FakeKey {
            signing_algorithms: vec!["ECDSA_SHA_256".to_owned(), "ECDSA_SHA_384".to_owned()],
            ..ecdsa_key()?
        })
        .await?;
        let signer = signer_for(&fake, ALIAS).await?;

        signer.public_key().await?;
        Ok(())
    }
}
