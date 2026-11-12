# ADR-03: Gang terminates TLS in one fixed adapter; transport is independent of authentication

Status: Accepted

## Context
- NativeLink reads a CRL only at listener start and never closes established connections (ADR-02), so TLS and admission cannot live in it.
- Operators also run Gang behind a tunnel or proxy that terminates TLS, or on a trusted network.
- Clients must trust the server without shipping a CA file.

## Decision
- **Single TLS authority.** `gang-tls` is the only place Gang does TLS. It is a fixed adapter (Ring 3, tier 28) imported by `gang-server`, `gang-client`, `gang-oidc` and `gang-main`, not a plugin; no scheduler, provisioner or sandbox plugin sees a certificate. It also owns the client HTTP stack used by `gang-oidc` and `gang-client` (`gang_tls::HttpClient`).
- **Transport is its own switch.** `[server] transport = "tls" | "plaintext"` says whether Gang terminates TLS. No authentication plugin implies it: a tunnel terminates TLS and Gang runs plaintext behind a `grpcs://` name, `token` works over either, and `none` over TLS is server authentication only. The one cross-rule: `mtls` requires `tls`.
- **Gang's CA is transport, and fixed.** `gang-mtls` is wired unconditionally. It exists whenever Gang terminates TLS, signs listener certificates, defines `ca_pin`, and signs only through the signer's `CertificateSignerPort`. Key custody is the signer plugin's concern (KMS in production, a file in Local Mode).
- **Acceptor.** TLS 1.3 only, session resumption disabled so every connection runs the verifier, ALPN per listener (ADR-09). The client-certificate mode of each listener comes from its surface's authentication plugin.
- **Client trust.** Clients use the operating system roots, or pin the CA's public-key hash (`ca_pin`) so first contact needs no CA file. Gang alone derives the pin (`CaPin::of_spki` of the signer's public key); a worker may be configured `ca_pin = "signer"` to derive it at start (with KMS, `kms:GetPublicKey` under its own role), so no deploy tool computes it.
- **Local Mode** defaults to `tls` with workers on `mtls`; `gang local up --insecure` runs plaintext with `none`.

## Consequences
- Under plaintext, or behind a public-certificate proxy, clients use system roots and Gang's CA is not in their path.
- Plaintext is safe only behind something that authenticates, or on a trusted network.
- The CA key never exists on disk in production.

## Rejected alternatives
- **Making the CA a plugin.** It is the one root of the worker credential; only the key's location varies, through the signer domain.
- **Transport implied by the authentication plugin.** Wrong for tunnels, for bearer tokens over Gang-terminated TLS, and for `none` over TLS.
