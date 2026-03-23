/// Live view query handler: replays events on each request to build a View.
pub mod live_view_query;
/// Memory view projector and query handler: keeps a View per aggregate ID in memory.
pub mod memory_view_query;

pub use live_view_query::LiveViewQuery;
pub use memory_view_query::{MemoryView, MemoryViewQuery};
