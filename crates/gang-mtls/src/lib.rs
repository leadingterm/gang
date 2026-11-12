//! Certificate authority and CSR verification for Gang's mutual TLS identities.
//!
//! Every identity is a SPIFFE-style URI in the certificate's subject alternative name. Principals
//! are percent-encoded as a single path segment, so no principal can add a path segment:
//!
//! ```
//! use gang_mtls::spiffe_id;
//! use gang_types::{Principal, ProviderName, TrustDomain};
//!
//! let trust_domain = TrustDomain::try_new("gang.example")?;
//! let provider = ProviderName::try_new("alpha")?;
//! let principal = Principal::try_new("dev@example.test")?;
//! assert_eq!(
//!     spiffe_id(&trust_domain, &provider, &principal),
//!     "spiffe://gang.example/alpha/dev%40example.test"
//! );
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod authority;
mod certificate;
mod csr;
mod errors;
mod key_pair;
mod profile;
mod serial;
mod signing;
mod spiffe;
mod subject_name;
mod time;

pub use authority::{
    CertificateAuthority, ClientCertificateRequest, IssuedCertificate, ServerCertificateRequest,
};
pub use csr::{VerifiedCsr, verify_csr};
pub use errors::{CsrError, MtlsError};
pub use key_pair::KeyPair;
pub use spiffe::spiffe_id;
pub use subject_name::SubjectName;
