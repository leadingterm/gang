//! `OpenID` Connect login: authorization-code flow with PKCE, and renewal.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_spi::{
    LoginChallenge, LoginClient, LoginGrant, LoginPort, LoginRequest, LoginSupport, PortError,
};
use gang_types::{ClientSecret, LoginSession, Proof, RefreshToken, Url};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::LoginConfig;
use crate::errors::OidcError;
use crate::provider::Provider;

/// The plugin's secret state of a login in progress, serialized into the [`LoginSession`].
#[derive(Serialize, Deserialize)]
struct Session {
    state: String,
    nonce: String,
    verifier: String,
    redirect_uri: Url,
    /// The client id the login runs under, sent to the authorization and token endpoints.
    client_id: String,
    /// The client secret. It lives only in the returned `LoginSession`, which the caller
    /// keeps in memory.
    client_secret: Option<ClientSecret>,
}

/// The token endpoint's successful response; only the fields this flow uses.
#[derive(Deserialize)]
struct TokenResponse {
    #[serde(rename = "id_token")]
    identity: Option<String>,
    #[serde(rename = "refresh_token")]
    refresh: Option<String>,
}

impl Provider {
    fn login_config(&self) -> Result<&LoginConfig, OidcError> {
        self.config()
            .login
            .as_ref()
            .ok_or(OidcError::LoginNotConfigured)
    }

    /// The client a login or renewal runs under: the override when given, otherwise the
    /// provider's configured client.
    fn client_credentials(&self, client: Option<&LoginClient>) -> (String, Option<ClientSecret>) {
        match client {
            Some(client) => (client.id.clone(), Some(client.secret.clone())),
            None => (
                self.config().audience.as_str().to_owned(),
                self.config()
                    .login
                    .as_ref()
                    .and_then(|login| login.client_secret.clone()),
            ),
        }
    }

    async fn begin(
        &self,
        redirect_uri: &Url,
        client: Option<&LoginClient>,
    ) -> Result<LoginChallenge, OidcError> {
        let login = self.login_config()?;
        let (client_id, client_secret) = self.client_credentials(client);
        let discovery = self.discovery().await?;
        let endpoint = discovery.authorization_endpoint()?;
        let state = random_token()?;
        let nonce = random_token()?;
        let verifier = random_token()?;
        let code_challenge = challenge(&verifier);

        let mut authorization_url = endpoint.clone();
        {
            let mut query = authorization_url.query_pairs_mut();
            query
                .append_pair("response_type", "code")
                .append_pair("client_id", &client_id)
                .append_pair("redirect_uri", redirect_uri.as_str())
                .append_pair("scope", &login.scopes.join(" "))
                .append_pair("state", &state)
                .append_pair("nonce", &nonce)
                .append_pair("code_challenge", &code_challenge)
                .append_pair("code_challenge_method", "S256");
            for (name, value) in &login.authorization_params {
                query.append_pair(name, value);
            }
        }

        let session = Session {
            state: state.clone(),
            nonce,
            verifier,
            redirect_uri: redirect_uri.clone(),
            client_id,
            client_secret,
        };
        let session = serde_json::to_string(&session).map_err(OidcError::MalformedSession)?;
        Ok(LoginChallenge {
            authorization_url,
            state,
            session: LoginSession::new(session),
        })
    }

    async fn complete(
        &self,
        session: LoginSession,
        callback: &Url,
    ) -> Result<LoginGrant, OidcError> {
        self.login_config()?;
        let session: Session =
            serde_json::from_str(session.expose()).map_err(OidcError::MalformedSession)?;
        if !same_endpoint(&session.redirect_uri, callback) {
            return Err(OidcError::RedirectMismatch);
        }
        if let Some(error) = query_values(callback, "error").into_iter().next() {
            return Err(if error == "access_denied" {
                OidcError::AccessDenied
            } else {
                OidcError::CallbackError(error)
            });
        }
        let states = query_values(callback, "state");
        if !matches!(states.as_slice(), [only] if *only == session.state) {
            return Err(OidcError::StateMismatch);
        }
        let [code]: [String; 1] = query_values(callback, "code")
            .try_into()
            .map_err(|_| OidcError::MissingCode)?;

        let discovery = self.discovery().await?;
        let token_endpoint = discovery.token_endpoint()?;
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", "authorization_code"),
            ("code", code.as_str()),
            ("redirect_uri", session.redirect_uri.as_str()),
            ("code_verifier", session.verifier.as_str()),
        ];
        push_client(
            &mut form,
            &session.client_id,
            session.client_secret.as_ref(),
        );
        let response: TokenResponse = self.http().post_form_json(token_endpoint, &form).await?;
        let id_token = response
            .identity
            .ok_or(OidcError::MissingClaim("id_token"))?;
        self.verify_id_token(&id_token, Some(session.nonce.as_str()))
            .await?;
        Ok(LoginGrant {
            proof: Proof::new(id_token),
            refresh_token: response.refresh.map(RefreshToken::new),
        })
    }

    async fn renew_grant(
        &self,
        refresh_token: &RefreshToken,
        client: Option<&LoginClient>,
    ) -> Result<LoginGrant, OidcError> {
        self.login_config()?;
        let (client_id, client_secret) = self.client_credentials(client);
        let discovery = self.discovery().await?;
        let token_endpoint = discovery.token_endpoint()?;
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token.expose()),
        ];
        push_client(&mut form, &client_id, client_secret.as_ref());
        let response: TokenResponse = self.http().post_form_json(token_endpoint, &form).await?;
        let id_token = response
            .identity
            .ok_or(OidcError::MissingClaim("id_token"))?;
        self.verify_id_token(&id_token, None).await?;
        Ok(LoginGrant {
            proof: Proof::new(id_token),
            refresh_token: response.refresh.map(RefreshToken::new),
        })
    }
}

impl LoginPort for Provider {
    fn login(&self) -> LoginSupport {
        if self.config().login.is_some() {
            LoginSupport::Interactive
        } else {
            LoginSupport::Unsupported
        }
    }

    async fn begin_login(&self, request: &LoginRequest<'_>) -> Result<LoginChallenge, PortError> {
        self.begin(request.redirect_uri, request.client)
            .await
            .map_err(PortError::from)
    }

    async fn complete_login(
        &self,
        session: LoginSession,
        callback_url: &Url,
    ) -> Result<LoginGrant, PortError> {
        self.complete(session, callback_url)
            .await
            .map_err(PortError::from)
    }

    async fn renew(
        &self,
        refresh_token: &RefreshToken,
        client: Option<&LoginClient>,
    ) -> Result<LoginGrant, PortError> {
        self.renew_grant(refresh_token, client)
            .await
            .map_err(PortError::from)
    }
}

/// Appends the client id, and the client secret when there is one, to a token request.
fn push_client<'a>(
    form: &mut Vec<(&'a str, &'a str)>,
    client_id: &'a str,
    secret: Option<&'a ClientSecret>,
) {
    form.push(("client_id", client_id));
    if let Some(secret) = secret {
        form.push(("client_secret", secret.expose()));
    }
}

/// A fresh 32-byte random value, base64url without padding.
fn random_token() -> Result<String, OidcError> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| OidcError::Entropy)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

/// The PKCE S256 code challenge for a verifier (RFC 7636 section 4.2).
fn challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier))
}

/// Scheme, host, port and path agree; the query and fragment are not compared.
fn same_endpoint(expected: &Url, actual: &Url) -> bool {
    expected.scheme() == actual.scheme()
        && expected.host_str() == actual.host_str()
        && expected.port_or_known_default() == actual.port_or_known_default()
        && expected.path() == actual.path()
}

/// Every value of the query parameter `key`, in order of appearance.
fn query_values(url: &Url, key: &str) -> Vec<String> {
    url.query_pairs()
        .filter(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::challenge;

    #[test]
    fn challenge_matches_rfc_7636_appendix_b() {
        assert_eq!(
            challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }
}
