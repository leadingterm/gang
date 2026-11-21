use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod, Transaction};
use gang_clock::{Clock, SystemClock};
use gang_spi::{Plugin, PluginContext, PortError};
use gang_tls::ServerTrust;
use gang_types::CertificatePem;
use tokio::sync::OnceCell;
use tokio_postgres::NoTls;
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::config::{PostgresConfig, Tls};
use crate::errors::PostgresError;
use crate::schema::initialize;

/// `PostgreSQL`-backed state repository, leader lease and expiring records, shared by every
/// controller of a deployment.
///
/// Building opens no connection: the pool connects on the first port call, which also brings the
/// schema up to date once per process. Nothing is kept per session (no advisory locks held across
/// statements, no `LISTEN`). Transaction-mode poolers have not been runtime-tested.
pub struct Postgres {
    config: PostgresConfig,
    pool: Pool,
    clock: Arc<dyn Clock>,
    schema: OnceCell<()>,
}

/// How a transaction ends once its work succeeded.
pub(crate) enum Finish<T> {
    /// Keep every write.
    Commit(T),
    /// Undo every write: the work found a condition that refuses the whole request.
    Rollback(T),
}

impl fmt::Debug for Postgres {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Postgres")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Plugin for Postgres {
    type Config = PostgresConfig;

    fn build(config: PostgresConfig, context: &PluginContext<'_>) -> Result<Self, PortError> {
        Ok(Self::connect(
            config,
            context,
            Arc::new(SystemClock::new()),
        )?)
    }
}

impl Postgres {
    /// Builds the pool for `config`, reading the password and CA files named relative to the
    /// configuration file. Every timeout is measured on `clock`.
    pub(crate) fn connect(
        config: PostgresConfig,
        context: &PluginContext<'_>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, PostgresError> {
        config.validate()?;
        let mut server = tokio_postgres::Config::new();
        server
            .host(config.host.as_str())
            .port(config.port.get())
            .dbname(config.database.as_str())
            .user(config.user.as_str())
            .application_name("gang")
            .connect_timeout(config.connect_timeout.get());
        if let Some(file) = &config.password_file {
            server.password(read_password(&beside(context, file))?);
        }
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = match config.tls {
            Tls::Disable => {
                server.ssl_mode(SslMode::Disable);
                Manager::from_config(server, NoTls, manager_config)
            }
            Tls::VerifyFull => {
                // rustls always checks the chain and the host name, so `require` is verify-full.
                server.ssl_mode(SslMode::Require);
                let ca = config
                    .ca_file
                    .as_ref()
                    .map(|file| read_ca(&beside(context, file)))
                    .transpose()?;
                let trust = ca.as_ref().map_or(ServerTrust::System, ServerTrust::Ca);
                let tls = gang_tls::client_config(trust, None, &[], Arc::clone(&clock))
                    .map_err(PostgresError::Tls)?;
                Manager::from_config(server, MakeRustlsConnect::new(tls), manager_config)
            }
        };
        let pool = Pool::builder(manager)
            .max_size(config.pool_size.get())
            .build()
            // Only pool timeouts need a runtime; none are set, so building cannot fail in practice.
            .map_err(|_| PostgresError::Config {
                detail: "the connection pool cannot be built",
            })?;
        Ok(Self {
            config,
            pool,
            clock,
            schema: OnceCell::new(),
        })
    }

    /// Runs `work` on a pooled connection once the schema is up to date. The connection must open
    /// within `connect_timeout` and `work` must finish within `statement_timeout`.
    pub(crate) async fn run<T>(
        &self,
        work: impl AsyncFnOnce(&mut Object) -> Result<T, PostgresError>,
    ) -> Result<T, PostgresError> {
        self.schema
            .get_or_try_init(|| {
                self.bounded(async |client| {
                    let transaction = self.begin(client).await?;
                    initialize(&transaction).await?;
                    Ok(transaction.commit().await?)
                })
            })
            .await?;
        self.bounded(work).await
    }

    /// Runs `work` in one transaction on a pooled connection, as [`Self::run`] does.
    pub(crate) async fn transaction<T>(
        &self,
        work: impl AsyncFnOnce(&Transaction<'_>) -> Result<Finish<T>, PostgresError>,
    ) -> Result<T, PostgresError> {
        self.run(async move |client| {
            let transaction = self.begin(client).await?;
            match work(&transaction).await? {
                Finish::Commit(value) => {
                    transaction.commit().await?;
                    Ok(value)
                }
                Finish::Rollback(value) => {
                    transaction.rollback().await?;
                    Ok(value)
                }
            }
        })
        .await
    }

    /// Runs a write in a transaction with server-side timeouts and a cancellation boundary.
    pub(crate) async fn mutate<T>(
        &self,
        work: impl AsyncFnOnce(&Transaction<'_>) -> Result<T, PostgresError>,
    ) -> Result<T, PostgresError> {
        self.transaction(async move |tx| Ok(Finish::Commit(work(tx).await?)))
            .await
    }

    /// Opens a transaction that carries the statement timeout to the server and ends itself when
    /// the client goes quiet, so a controller that loses its link mid-transaction cannot keep row
    /// or advisory locks that other controllers wait on.
    async fn begin<'c>(&self, client: &'c mut Object) -> Result<Transaction<'c>, PostgresError> {
        let bound = self.config.statement_timeout.get().as_millis();
        let transaction = client.transaction().await?;
        transaction
            .batch_execute(&format!(
                "SET LOCAL statement_timeout = {bound}; \
                 SET LOCAL idle_in_transaction_session_timeout = {bound}"
            ))
            .await?;
        Ok(transaction)
    }

    /// Checks a connection out for `work`. The connection goes back to the pool only when `work`
    /// finished without a transient failure; one whose call timed out, failed in transit, or was
    /// cancelled by its caller is discarded, since a cut link or a stopped server may still owe
    /// it a reply.
    async fn bounded<T>(
        &self,
        work: impl AsyncFnOnce(&mut Object) -> Result<T, PostgresError>,
    ) -> Result<T, PostgresError> {
        let pooled = self
            .clock
            .timeout(self.config.connect_timeout.get(), self.pool.get())
            .await
            .map_err(|_| PostgresError::Timeout {
                bound: "connect_timeout",
            })??;
        let mut checkout = Checkout(None);
        let client = checkout.0.insert(pooled);
        let outcome = self
            .clock
            .timeout(self.config.statement_timeout.get(), work(client))
            .await
            .unwrap_or(Err(PostgresError::Timeout {
                bound: "statement_timeout",
            }));
        if !outcome.as_ref().is_err_and(PostgresError::is_transient) {
            checkout.release();
        }
        outcome
    }
}

/// A pooled connection that is discarded unless released back to the pool.
struct Checkout(Option<Object>);

impl Checkout {
    /// Returns the connection to the pool.
    fn release(mut self) {
        drop(self.0.take());
    }
}

impl Drop for Checkout {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            drop(Object::take(client));
        }
    }
}

/// `file` as named in the configuration: absolute, or relative to the configuration file's
/// directory.
fn beside(context: &PluginContext<'_>, file: &Path) -> PathBuf {
    context
        .config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .join(file)
}

/// The password stored in `path`, without the line ending that ends the file.
fn read_password(path: &Path) -> Result<String, PostgresError> {
    let text = std::fs::read_to_string(path).map_err(|source| PostgresError::PasswordFile {
        path: path.to_owned(),
        source,
    })?;
    let password = text.strip_suffix('\n').unwrap_or(&text);
    let password = password.strip_suffix('\r').unwrap_or(password);
    if password.is_empty() {
        return Err(PostgresError::Config {
            detail: "password_file must not be empty",
        });
    }
    Ok(password.to_owned())
}

/// The CA certificates stored in `path`, PEM encoded.
fn read_ca(path: &Path) -> Result<CertificatePem, PostgresError> {
    let text = std::fs::read_to_string(path).map_err(|source| PostgresError::CaFile {
        path: path.to_owned(),
        source,
    })?;
    CertificatePem::try_new(text).map_err(|source| PostgresError::CaInvalid {
        path: path.to_owned(),
        source,
    })
}
