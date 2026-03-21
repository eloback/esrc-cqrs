#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! CQRS extension for `esrc`.
//!
//! Provides a registry for command handlers and event projectors, allowing
//! structured dispatch of commands and event projections over an event store.
//!
//! # Command Handlers
//!
//! A command handler receives a typed command, loads the target aggregate,
//! processes the command, and writes the resulting event back to the store.
//! Handlers are registered by aggregate type and dispatched by subject.
//!
//! # Event Handlers (Projectors)
//!
//! Event handlers are [`esrc::project::Project`] implementors. They are
//! registered and driven by the registry, which subscribes them to the
//! relevant event streams.
//!
//! # Query Handlers
//!
//! A query handler receives a typed query request, loads the required data
//! (e.g., replaying aggregate state or reading a read model), and returns a
//! serialized response. Queries are read-only: the store reference is shared
//! rather than exclusive, so no events are written during query processing.
//! Handlers are registered by name and dispatched by subject.
//!
//! # NATS Backend
//!
//! When the `nats` feature is enabled, concrete implementations are provided
//! for all three handler kinds:
//!
//! - [`nats::NatsCommandDispatcher`] drives command handlers over core NATS
//!   request/reply service groups.
//! - [`nats::NatsQueryDispatcher`] drives query handlers over the same
//!   mechanism, using a shared store reference.
//! - [`nats::NatsProjectorRunner`] drives projectors via JetStream durable
//!   pull consumers so they resume across restarts.

/// Command handler trait and registry entry.
pub mod command;
/// Serializable CQRS error type transmitted in command replies.
pub mod error;
/// Event projector handler registry entry.
pub mod projector;
/// The top-level CQRS registry that holds and drives all handlers.
pub mod registry;
/// Query handler trait and registry entry.
pub mod query;

/// NATS-backed implementations of the command dispatcher and projector runner.
#[cfg(feature = "nats")]
pub mod nats;

pub use command::CommandHandler;
pub use error::Error;
pub use projector::ProjectorHandler;
pub use registry::CqrsRegistry;
pub use query::QueryHandler;
