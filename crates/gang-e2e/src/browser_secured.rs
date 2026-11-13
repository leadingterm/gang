//! [`Browser`] over a real [`Secured`] deployment.

use gang_types::Url;

use crate::access::Access;
use crate::browser::Browser;
use crate::errors::Result;
use crate::identity::Identity;
use crate::issuer::{AuthorizationRecord, RefreshRecord, SubjectPolicy};
use crate::secured::Secured;
use crate::server::{Answer, Request, ServerApi};
use crate::session::SessionCookie;
use crate::state_api::StateStream;
use crate::template::PROVIDER;

impl Browser for Secured<'_> {
    type Stream = StateStream;

    fn endpoint(&self) -> Result<Url> {
        self.stack().endpoint()
    }

    fn request(&self, request: &Request) -> Result<Answer> {
        ServerApi::send(self.stack(), request)
    }

    fn visit(&self, url: &Url) -> Result<Answer> {
        <Self as Access>::visit(self, url)
    }

    fn open_state_stream(&self, cookie: &SessionCookie) -> Result<Self::Stream> {
        <Self as Identity>::open_state_stream(self, cookie)
    }

    fn revoke(&self, subject: &str) -> Result<()> {
        self.stack()
            .administrator()?
            .revoke(&self.stack().ca_pin()?, &format!("{PROVIDER}:{subject}"))
    }

    fn restore(&self, subject: &str) -> Result<()> {
        self.stack()
            .administrator()?
            .restore(&self.stack().ca_pin()?, &format!("{PROVIDER}:{subject}"))
    }

    fn authorizations(&self, subject: &str) -> Vec<AuthorizationRecord> {
        self.issuer().authorizations(subject)
    }

    fn refreshes(&self, subject: &str) -> Vec<RefreshRecord> {
        self.issuer().refreshes(subject)
    }

    fn set_policy(&self, subject: &str, policy: SubjectPolicy) {
        self.issuer().set_policy(subject, policy);
    }
}
