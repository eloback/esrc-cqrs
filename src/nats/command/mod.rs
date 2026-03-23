/// Aggregate command handler wiring: maps a typed command to an aggregate and writes events.
pub mod aggregate_command_handler;

pub use aggregate_command_handler::{AggregateCommandHandler, CommandEnvelope, CommandReply};
