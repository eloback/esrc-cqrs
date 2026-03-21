//! NATS-backed CQRS dispatcher and projector runner.
//!
//! # Command Dispatcher
//!
//! Commands use NATS **core request/reply**: the dispatcher creates a service
//! group on the JetStream context and listens on subjects of the form
//! `<prefix>.cmd.<handler_name>`. Each incoming request is dispatched to the
//! matching [`CommandHandler`], and the reply is sent back to the caller.
//!
//! This is the correct transport choice for commands because:
//! * Commands are point-in-time requests that expect an immediate acknowledgment.
//! * Core NATS request/reply is low-latency and naturally load-balances across
//!   multiple service instances via queue groups.
//! * There is no need to persist commands; only the resulting events are durable.
//!
//! # Projector Runner
//!
//! Projectors use NATS **JetStream durable pull consumers** (the same mechanism
//! as the existing `Subscribe` / `durable_observe` in `NatsStore`). Each
//! projector runs as an independent task and resumes from its last position
//! across restarts using its durable consumer name.
//!
//! This is the correct transport choice for projectors because:
//! * Event projections must be durable and survive process restarts.
//! * Pull consumers allow back-pressure and fine-grained acknowledgment.
//! * Each projector gets its own consumer position so they progress independently.
//!
//! # Query Dispatcher
//!
//! Queries use NATS **core request/reply**, the same transport as commands, but
//! with a shared (non-exclusive) store reference because queries are read-only.
//! The dispatcher creates a service group and listens on subjects of the form
//! `<service_name>.<handler_name>`. Each incoming request is dispatched to the
//! matching [`QueryHandler`], and the reply is sent back to the caller.
//!
//! This is the correct transport choice for queries because:
//! * Queries are point-in-time reads that expect an immediate response.
//! * Sharing the store across handlers avoids unnecessary cloning of connections.
//! * Core NATS request/reply naturally load-balances across service instances.

/// NATS command dispatcher backed by core NATS request/reply service groups.
pub mod command_dispatcher;
/// NATS projector runner backed by JetStream durable consumers.
pub mod projector_runner;
/// NATS query dispatcher backed by core NATS request/reply service groups.
pub mod query_dispatcher;

pub use aggregate_command_handler::{AggregateCommandHandler, CommandEnvelope, CommandReply};
pub use command_dispatcher::NatsCommandDispatcher;
pub use durable_projector_handler::DurableProjectorHandler;
pub use live_view_query::LiveViewQuery;
pub use memory_view_query::{MemoryView, MemoryViewQuery};
pub use projector_runner::NatsProjectorRunner;
pub use query_dispatcher::{NatsQueryDispatcher, QueryEnvelope, QueryReply};

/// Aggregate command handler wiring: maps a typed command to an aggregate and writes events.
mod aggregate_command_handler;
/// Durable projector handler wiring: maps a projector to a durable JetStream consumer.
mod durable_projector_handler;
/// Live view query handler: replays events on each request to build a View.
mod live_view_query;
/// Memory view projector and query handler: keeps a View per aggregate ID in memory.
mod memory_view_query;
