//! Drain bridge for `NativeLink` v0.3.x workers during rolling upgrades.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::{self, BoxStream};
use gang_clock::Clock;
use gang_reapi::build::bazel::remote::execution::v2::platform::Property;
use gang_scheduler::{WorkerEvent, WorkerRegistration};
use gang_types::{ActionResources, Bytes, FleetName, InstanceId, property};
use prost::Message;
use tonic::codegen::{Body, BoxFuture, Service, StdError};
use tonic::{Request, Response, Status, Streaming};

use crate::actor::WorkerOutbound;
use crate::controller::ControllerHandle;
use crate::worker_service::{
    InboundDropGuard, SessionDropGuard, WorkerLossGuard, WorkerServiceConfig, map_register_error,
};

#[derive(Clone, PartialEq, Message)]
struct UpdateForScheduler {
    #[prost(oneof = "scheduler_update::Update", tags = "1, 2, 3")]
    update: Option<scheduler_update::Update>,
}
mod scheduler_update {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Update {
        #[prost(message, tag = "1")]
        Connect(super::ConnectWorkerRequest),
        #[prost(message, tag = "2")]
        KeepAlive(()),
        #[prost(message, tag = "3")]
        GoingAway(super::GoingAwayRequest),
    }
}
#[derive(Clone, PartialEq, Message)]
struct ConnectWorkerRequest {
    #[prost(message, repeated, tag = "1")]
    properties: Vec<Property>,
    #[prost(string, tag = "2")]
    worker_id_prefix: String,
    #[prost(uint64, tag = "3")]
    max_inflight_tasks: u64,
}
#[derive(Clone, PartialEq, Message)]
struct GoingAwayRequest {
    #[prost(bool, tag = "2")]
    drain: bool,
}
#[derive(Clone, PartialEq, Message)]
struct UpdateForWorker {
    #[prost(oneof = "worker_update::Update", tags = "1, 2, 4")]
    update: Option<worker_update::Update>,
}
mod worker_update {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Update {
        #[prost(message, tag = "1")]
        ConnectionResult(super::ConnectionResult),
        #[prost(message, tag = "2")]
        KeepAlive(()),
        #[prost(message, tag = "4")]
        Disconnect(()),
    }
}
#[derive(Clone, PartialEq, Message)]
struct ConnectionResult {
    #[prost(string, tag = "1")]
    worker_id: String,
    #[prost(bool, tag = "2")]
    dispatch_ack: bool,
    #[prost(string, tag = "3")]
    memory_property: String,
}

#[derive(Clone)]
pub(crate) struct LegacyWorkerService {
    controller: ControllerHandle,
    config: WorkerServiceConfig,
    clock: Arc<dyn Clock>,
    limit: usize,
}
impl LegacyWorkerService {
    pub(crate) const fn new(
        controller: ControllerHandle,
        config: WorkerServiceConfig,
        clock: Arc<dyn Clock>,
        limit: usize,
    ) -> Self {
        Self {
            controller,
            config,
            clock,
            limit,
        }
    }

    async fn connect(
        &self,
        request: Request<Streaming<UpdateForScheduler>>,
    ) -> Result<Response<BoxStream<'static, Result<UpdateForWorker, Status>>>, Status> {
        let mut inbound = request.into_inner();
        let first = self
            .clock
            .timeout(self.config.registration_timeout.get(), inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("timed out waiting for ConnectWorker"))??;
        let Some(UpdateForScheduler {
            update: Some(scheduler_update::Update::Connect(connect)),
        }) = first
        else {
            return Err(Status::invalid_argument(
                "first worker frame must be ConnectWorker",
            ));
        };
        let registered = self
            .controller
            .register_worker(parse_registration(connect)?)
            .await
            .map_err(map_register_error)?;
        let loss = Arc::new(WorkerLossGuard::new(
            self.controller.clone(),
            registered.key,
            registered.worker_id.clone(),
            tokio::runtime::Handle::current(),
        ));
        let inbound_task = tokio::spawn(read_inbound(
            inbound,
            self.controller.clone(),
            registered.key,
            Arc::clone(&loss),
        ));
        let initial = UpdateForWorker {
            update: Some(worker_update::Update::ConnectionResult(ConnectionResult {
                worker_id: registered.worker_id.as_str().to_owned(),
                dispatch_ack: false,
                memory_property: String::new(),
            })),
        };
        let state = (
            Some(initial),
            registered.outbound,
            SessionDropGuard { loss, inbound_task },
            false,
        );
        let stream = stream::unfold(
            state,
            |(mut initial, mut outbound, guard, finished)| async move {
                if finished {
                    return None;
                }
                if let Some(frame) = initial.take() {
                    return Some((Ok(frame), (initial, outbound, guard, false)));
                }
                match outbound.recv().await {
                    Some(WorkerOutbound::SetDraining(_)) => Some((
                        Ok(UpdateForWorker {
                            update: Some(worker_update::Update::KeepAlive(())),
                        }),
                        (initial, outbound, guard, false),
                    )),
                    Some(WorkerOutbound::Rehome(_)) => {
                        guard.inbound_task.abort();
                        guard.loss.notify_lost().await;
                        Some((
                            Ok(UpdateForWorker {
                                update: Some(worker_update::Update::Disconnect(())),
                            }),
                            (initial, outbound, guard, true),
                        ))
                    }
                    Some(WorkerOutbound::Assign(_) | WorkerOutbound::Kill(_)) => {
                        tracing::error!("unexpected directive to stale NativeLink worker");
                        guard.inbound_task.abort();
                        guard.loss.notify_lost().await;
                        None
                    }
                    None => {
                        guard.inbound_task.abort();
                        guard.loss.notify_lost().await;
                        None
                    }
                }
            },
        );
        Ok(Response::new(Box::pin(stream)))
    }
}

async fn read_inbound(
    mut inbound: Streaming<UpdateForScheduler>,
    controller: ControllerHandle,
    key: gang_scheduler::WorkerKey,
    loss: Arc<WorkerLossGuard>,
) {
    let _guard = InboundDropGuard { loss };
    while let Ok(Some(frame)) = inbound.message().await {
        let event = match frame.update {
            Some(scheduler_update::Update::KeepAlive(())) => WorkerEvent::KeepAlive {
                cache: None,
                memory_capacity: None,
                load: None,
                inflight: Vec::new(),
            },
            Some(scheduler_update::Update::GoingAway(going)) => {
                if controller
                    .worker_event(key, WorkerEvent::GoingAway)
                    .await
                    .is_err()
                    || !going.drain
                {
                    break;
                }
                continue;
            }
            _ => continue,
        };
        if controller.worker_event(key, event).await.is_err() {
            break;
        }
    }
}

fn parse_registration(request: ConnectWorkerRequest) -> Result<WorkerRegistration, Status> {
    let properties: BTreeMap<String, String> = request
        .properties
        .into_iter()
        .map(|p| (p.name, p.value))
        .collect();
    let fleet = FleetName::try_new(properties.get(property::FLEET).cloned().unwrap_or_default())
        .map_err(|e| Status::invalid_argument(format!("invalid fleet: {e}")))?;
    let instance = request
        .worker_id_prefix
        .strip_suffix('-')
        .ok_or_else(|| Status::invalid_argument("invalid worker_id_prefix"))?;
    let instance = InstanceId::try_new(instance.to_owned())
        .map_err(|e| Status::invalid_argument(format!("invalid instance: {e}")))?;
    let version = properties
        .get(property::GANG_VERSION)
        .filter(|v| !v.is_empty() && v.as_str() != env!("CARGO_PKG_VERSION"))
        .cloned()
        .unwrap_or_else(|| "legacy-nativelink".to_owned());
    let number = |key: &str| {
        properties
            .get(key)
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&v| v > 0)
    };
    let bytes = |key: &str| {
        number(key)
            .and_then(|n| n.checked_mul(1024))
            .map_or(Bytes::mib(1), Bytes::new)
    };
    let capacity = ActionResources {
        cpu_millicores: number(property::CPU_MILLICORES).unwrap_or(1000),
        memory: bytes(property::MEMORY_KB),
        disk: bytes(property::DISK_KB),
    };
    let mut uuid_bytes = [0; 16];
    getrandom::fill(&mut uuid_bytes)
        .map_err(|e| Status::internal(format!("failed to read OS entropy: {e}")))?;
    Ok(WorkerRegistration {
        instance: Arc::new(instance),
        fleet,
        gang_version: version,
        properties,
        capacity,
        ceilings: capacity,
        max_inflight: u32::try_from(request.max_inflight_tasks)
            .ok()
            .and_then(NonZeroU32::new),
        single_use: false,
        environment: None,
        uuid_bytes,
    })
}

impl tonic::server::NamedService for LegacyWorkerService {
    const NAME: &'static str = "com.github.trace_machina.nativelink.remote_execution.WorkerApi";
}
struct Connect(LegacyWorkerService);
impl tonic::server::StreamingService<UpdateForScheduler> for Connect {
    type Response = UpdateForWorker;
    type ResponseStream = BoxStream<'static, Result<UpdateForWorker, Status>>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;
    fn call(&mut self, request: Request<Streaming<UpdateForScheduler>>) -> Self::Future {
        let service = self.0.clone();
        Box::pin(async move { service.connect(request).await })
    }
}

impl<B> Service<http::Request<B>> for LegacyWorkerService
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path()
            != "/com.github.trace_machina.nativelink.remote_execution.WorkerApi/ConnectWorker"
        {
            return Box::pin(async {
                let mut response = http::Response::new(tonic::body::Body::default());
                response.headers_mut().insert(
                    tonic::Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(response)
            });
        }
        let service = self.clone();
        Box::pin(async move {
            let limit = service.limit;
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                .apply_max_message_size_config(Some(limit), Some(limit));
            Ok(grpc.streaming(Connect(service), request).await)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_legacy_registration_with_unknown_action_ceilings() {
        let request = ConnectWorkerRequest {
            properties: vec![Property {
                name: property::FLEET.to_owned(),
                value: "alpha".to_owned(),
            }],
            worker_id_prefix: "i-123-".to_owned(),
            max_inflight_tasks: 2,
        };
        let mut encoded = request.encode_to_vec();
        encoded.extend_from_slice(&[0x2a, 0x02, 0x08, 0x01]);
        let decoded = ConnectWorkerRequest::decode(encoded.as_slice()).expect("legacy frame");
        let registration = parse_registration(decoded).expect("registration");
        assert_eq!(registration.instance.as_str(), "i-123");
        assert_eq!(registration.fleet.as_str(), "alpha");
        assert_ne!(registration.gang_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(registration.capacity.memory, Bytes::mib(1));
    }
    async fn connect_legacy(
        addr: std::net::SocketAddr,
    ) -> (
        tokio::sync::mpsc::Sender<UpdateForScheduler>,
        Streaming<UpdateForWorker>,
    ) {
        use tokio::sync::mpsc;
        use tonic::transport::Endpoint;
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.expect("ready");
        let (tx, rx) = mpsc::channel(8);
        tx.send(UpdateForScheduler {
            update: Some(scheduler_update::Update::Connect(ConnectWorkerRequest {
                properties: vec![Property {
                    name: property::FLEET.to_owned(),
                    value: "alpha".to_owned(),
                }],
                worker_id_prefix: "i-123-".to_owned(),
                max_inflight_tasks: 1,
            })),
        })
        .await
        .expect("send");
        let outbound = stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|frame| (frame, rx))
        });
        let path = http::uri::PathAndQuery::from_static(
            "/com.github.trace_machina.nativelink.remote_execution.WorkerApi/ConnectWorker",
        );
        let response: Response<Streaming<UpdateForWorker>> = client
            .streaming(
                Request::new(outbound),
                path,
                tonic_prost::ProstCodec::default(),
            )
            .await
            .expect("handshake");
        (tx, response.into_inner())
    }

    async fn assert_registration(inbound: &mut Streaming<UpdateForWorker>) {
        let first = inbound
            .message()
            .await
            .expect("message")
            .expect("registration");
        let Some(worker_update::Update::ConnectionResult(result)) = first.update else {
            panic!("expected connection result")
        };
        assert!(result.worker_id.starts_with("i-123-"));
    }

    #[tokio::test]
    async fn legacy_stream_registers_drains_and_rehomes() {
        use crate::actor::{ActorConfig, ControllerRuntime};
        use crate::controller::{LearnedSlot, spawn_controller};
        use gang_clock::SystemClock;
        use gang_scheduler::{Learned, Scheduler, SchedulerPolicy};
        use gang_spi::{ControllerDrain, SchedulerSnapshot};
        use gang_types::{FleetActionPolicy, FleetCatalog, FleetProperties, Isa, Resources};
        use std::time::Duration;
        use tokio::net::TcpListener;
        use tonic::transport::Server;

        tokio::time::timeout(Duration::from_secs(20), async {
            let fleet = FleetProperties::new(
                &FleetName::try_new("alpha").expect("fleet"),
                Isa::X86_64,
                &"bwrap",
                &"host:test".parse().expect("environment"),
                Resources {
                    vcpus: 8,
                    memory: Bytes::gib(8),
                    disk: Bytes::gib(100),
                },
                &FleetActionPolicy::default(),
                std::iter::empty(),
            );
            let catalog = FleetCatalog::try_new(vec![fleet]).expect("catalog");
            let policy = SchedulerPolicy {
                expected_gang_version: env!("CARGO_PKG_VERSION").to_owned(),
                ..SchedulerPolicy::default()
            };
            let learned =
                Learned::new(&policy.actions, &policy.scaling, catalog.len()).expect("learned");
            let scheduler = Scheduler::new(policy, &catalog, learned).expect("scheduler");
            let (controller, actor) = spawn_controller(
                scheduler,
                SystemClock::new(),
                ActorConfig::default(),
                LearnedSlot::empty(),
                ControllerRuntime::new(),
            );
            let mut before = SchedulerSnapshot::default();
            controller.snapshot(&mut before).await.expect("snapshot");
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
            let addr = listener.local_addr().expect("address");
            let incoming = stream::unfold(listener, |listener| async move {
                let (socket, _) = listener.accept().await.ok()?;
                Some((Ok::<_, std::io::Error>(socket), listener))
            });
            let service = LegacyWorkerService::new(
                controller.clone(),
                WorkerServiceConfig::default(),
                Arc::new(SystemClock::new()),
                4 * 1024 * 1024,
            );
            let server = tokio::spawn(async move {
                Server::builder()
                    .add_service(service)
                    .serve_with_incoming(incoming)
                    .await
            });
            let (tx, mut inbound) = connect_legacy(addr).await;
            assert_registration(&mut inbound).await;
            let mut snapshot = SchedulerSnapshot::default();
            controller.snapshot(&mut snapshot).await.expect("snapshot");
            assert_eq!(snapshot.fleet_capacity, before.fleet_capacity);
            assert_eq!(snapshot.workers.len(), 1);
            assert!(!snapshot.workers[0].current_build);
            assert!(snapshot.workers[0].draining);
            assert_eq!(snapshot.workers[0].running_actions, 0);
            assert!(snapshot.cordons.iter().any(|cordon| cordon.replacing));
            tx.send(UpdateForScheduler {
                update: Some(scheduler_update::Update::KeepAlive(())),
            })
            .await
            .expect("keepalive");
            controller
                .hand_off(true, &mut ControllerDrain::default())
                .await
                .expect("handoff");
            loop {
                let frame = inbound
                    .message()
                    .await
                    .expect("message")
                    .expect("disconnect");
                if matches!(frame.update, Some(worker_update::Update::Disconnect(()))) {
                    break;
                }
            }
            drop(inbound);
            drop(tx);
            server.abort();
            actor.abort();
        })
        .await
        .expect("test deadline");
    }
}
