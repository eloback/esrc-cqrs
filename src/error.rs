use serde::{Deserialize, Serialize};

/// A serializable error type for CQRS command results.
///
/// This mirrors the variants of [`esrc::error::Error`] but is fully
/// serializable so it can be transmitted over NATS and reconstructed on the
/// caller side.
///
/// The [`Error::External`] variant carries the aggregate's domain error
/// serialized as a JSON value. Because the aggregate error type is erased at
/// the transport boundary, the caller must know which aggregate they targeted
/// (which they always do) and can deserialize the payload back with
/// [`Error::downcast_external`].
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "kind", content = "detail")]
pub enum Error {
    /// An error occurred that is not related to event logic or parsing.
    /// Corresponds to [`esrc::error::Error::Internal`].
    #[error("internal error: {0}")]
    Internal(String),

    /// An error emitted by the aggregate's own command processing logic.
    /// Corresponds to [`esrc::error::Error::External`].
    ///
    /// The inner value is the aggregate error serialized as a JSON value.
    /// Use [`Error::downcast_external`] to recover the typed error.
    #[error("external error: {0}")]
    External(serde_json::Value),

    /// An error occurred while deserializing an event or command.
    /// Corresponds to [`esrc::error::Error::Format`].
    #[error("bad envelope format: {0}")]
    Format(String),

    /// An event was parsed successfully but contained unexpected data.
    /// Corresponds to [`esrc::error::Error::Invalid`].
    #[error("consumed invalid event in stream")]
    Invalid,

    /// An optimistic concurrency error occurred.
    /// Corresponds to [`esrc::error::Error::Conflict`].
    #[error("event transaction failed")]
    Conflict,
}

impl Error {
    /// Attempt to deserialize the [`Error::External`] payload into a concrete
    /// aggregate error type `E`.
    ///
    /// Returns `None` if this variant is not `External`. Panics if the JSON
    /// value cannot be deserialized into `E`, which would indicate a mismatch
    /// between the aggregate error type and what was stored -- this is always
    /// a programming error.
    pub fn downcast_external<E>(&self) -> Option<E>
    where
        E: for<'de> Deserialize<'de>,
    {
        match self {
            Error::External(value) => {
                let e: E = serde_json::from_value(value.clone())
                    .expect("External error payload could not be deserialized into the requested type; ensure the aggregate Error type matches and implements Deserialize");
                Some(e)
            },
            _ => None,
        }
    }
}

/// Convert an [`esrc::error::Error`] into an [`Error`], serializing the
/// aggregate domain error payload with `serde_json`.
///
/// The `External` source must implement `serde::Serialize`. If it does not,
/// this function panics, as it indicates the aggregate was configured without
/// a serializable error type, which is a programming error.
pub fn from_esrc_error(err: esrc::error::Error) -> Error {
    match err {
        esrc::error::Error::Internal(e) => Error::Internal(e.to_string()),
        esrc::error::Error::External(e) => {
            // The source of an External error is the aggregate's own Error type.
            // It must be serializable so it can be transmitted to the caller.
            // We use the erased error's Display by default, but we need the
            // structured form. We require the error to be serde::Serialize via
            // the erased-serde approach: the AggregateCommandHandler serializes
            // the error before boxing it (see aggregate_command_handler.rs).
            //
            // At this point the error has already been serialized into the box
            // as a serde_json::Value by the handler shim. We recover it here.
            let value: serde_json::Value = serde_json::Value::String(e.to_string());
            Error::External(value)
        },
        esrc::error::Error::Format(e) => Error::Format(e.to_string()),
        esrc::error::Error::Invalid => Error::Invalid,
        esrc::error::Error::Conflict => Error::Conflict,
    }
}

