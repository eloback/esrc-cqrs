/// Aggregate command handler wiring: maps a typed command to an aggregate and writes events.
pub mod aggregate_command_handler;
/// Service command handler adapter: wraps a NatsServiceCommandHandler into CommandHandler.
pub mod service_command_handler;

pub use aggregate_command_handler::{AggregateCommandHandler, CommandEnvelope, CommandReply};
pub use service_command_handler::ServiceCommandHandler;
