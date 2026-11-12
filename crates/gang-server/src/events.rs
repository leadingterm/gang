//! The wire form of the scaling events the ledger records.

use gang_spi::{RecordedScalingEvent, ScalingEventKind};
use gang_wire::name::{self, Events, Fleets, Instances, Workers};
use gang_wire::{Cause, Event, EventChange};

/// The wire form of one recorded scaling event.
pub(crate) fn wire_event(recorded: RecordedScalingEvent) -> Event {
    let RecordedScalingEvent { seq, event } = recorded;
    let change = match event.kind {
        ScalingEventKind::ScaledOut { from, to } => EventChange::ScaledOut { from, to },
        ScalingEventKind::DrainStarted { worker } => EventChange::DrainStarted {
            worker: name::of::<Workers>(&worker),
        },
        ScalingEventKind::DrainCancelled { worker } => EventChange::DrainCancelled {
            worker: name::of::<Workers>(&worker),
        },
        ScalingEventKind::Terminated { instance } => EventChange::Terminated {
            instance: name::of::<Instances>(&instance),
        },
        ScalingEventKind::ExitedEarly { instance, failure } => EventChange::ExitedEarly {
            instance: name::of::<Instances>(&instance),
            failure,
        },
        ScalingEventKind::WorkerRegistered { worker } => EventChange::WorkerRegistered {
            worker: name::of::<Workers>(&worker),
        },
        ScalingEventKind::WorkerDeregistered { worker } => EventChange::WorkerDeregistered {
            worker: name::of::<Workers>(&worker),
        },
    };
    Event {
        name: name::of::<Events>(&seq.get()),
        create_time: event.at,
        fleet: name::of::<Fleets>(&event.fleet),
        cause: event.cause.map(Cause::from),
        change,
    }
}
