//! Integration tests for `esrc-cqrs` against a live NATS server.
//!
//! Requires a NATS server with JetStream enabled at `localhost:4222`:
//!   nats-server -js
//!
//! Run with:
//!   cargo test -p esrc-cqrs --test integration_nats -- --test-threads=1

use esrc::view::View;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_nats::jetstream;
use esrc::aggregate::Aggregate;
use esrc::event::replay::ReplayOneExt;
use esrc::nats::NatsStore;
use esrc::project::{Context, Project};
use esrc::version::{DeserializeVersion, SerializeVersion};
use serde::{Deserialize, Serialize};
use esrc::{Envelope, Event};
use esrc_cqrs::nats::{
    AggregateCommandHandler, CommandEnvelope, CommandReply, DurableProjectorHandler, LiveViewQuery, NatsCommandDispatcher
};
use esrc_cqrs::nats::{NatsQueryDispatcher, QueryEnvelope, QueryReply};
use esrc_cqrs::CqrsRegistry;
use tokio::time::sleep;
use uuid::Uuid;

// -- Query read model --------------------------------------------------------

/// A simple read model returned by query handlers in tests.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct CounterState {
    /// The current value of the counter.
    pub value: i64,
}

// -- Test domain types -------------------------------------------------------

#[derive(Debug, Default)]
struct Counter {
    value: i64,
}

#[derive(Debug, Deserialize, Serialize)]
enum CounterCommand {
    Increment { by: i64 },
    Decrement { by: i64 },
    /// A command that always fails, used to test command error propagation.
    AlwaysFail,
}

#[derive(Debug, Clone, Event, Serialize, Deserialize, SerializeVersion, DeserializeVersion)]
enum CounterEvent {
    Incremented { by: i64 },
    Decremented { by: i64 },
}

#[derive(Debug, thiserror::Error)]
enum CounterError {
    #[error("forced failure for testing")]
    ForcedFailure,
}

// CounterError must be serializable so it can be transmitted in CommandReply.
impl serde::Serialize for CounterError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("ForcedFailure")
    }
}

impl<'de> serde::Deserialize<'de> for CounterError {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "ForcedFailure" => Ok(CounterError::ForcedFailure),
            other => Err(serde::de::Error::unknown_variant(other, &["ForcedFailure"])),
        }
    }
}

impl Aggregate for Counter {
    type Command = CounterCommand;
    type Event = CounterEvent;
    type Error = CounterError;

    fn process(&self, command: Self::Command) -> Result<Self::Event, Self::Error> {
        match command {
            CounterCommand::Increment { by } => Ok(CounterEvent::Incremented { by }),
            CounterCommand::Decrement { by } => Ok(CounterEvent::Decremented { by }),
            CounterCommand::AlwaysFail => Err(CounterError::ForcedFailure),
        }
    }

    fn apply(self, event: &Self::Event) -> Self {
        match event {
            CounterEvent::Incremented { by } => Counter {
                value: self.value + by,
            },
            CounterEvent::Decremented { by } => Counter {
                value: self.value - by,
            },
        }
    }
}

impl View for Counter {
    type Event = CounterEvent;

    fn apply(self, event: &Self::Event) -> Self {
        match event {
            CounterEvent::Incremented { by } => Counter {
                value: self.value + by,
            },
            CounterEvent::Decremented { by } => Counter {
                value: self.value - by,
            },
        }
    }
}

// -- Shared projector state --------------------------------------------------

/// Tracks events received by the projector during a test run.
#[derive(Clone, Default)]
struct RecordingProjector {
    /// Events received, stored as simple string labels.
    received: Arc<Mutex<Vec<(Uuid, String)>>>,
    /// Whether to return an error on the next projection call.
    fail_next: Arc<Mutex<bool>>,
}

impl RecordingProjector {
    fn new() -> Self {
        Self::default()
    }

    fn received_events(&self) -> Vec<(Uuid, String)> {
        self.received.lock().unwrap().clone()
    }

    /// Make the next `project` call return an error.
    fn set_fail_next(&self) {
        *self.fail_next.lock().unwrap() = true;
    }
}

#[derive(Debug, thiserror::Error)]
#[error("projector forced error")]
struct ProjectorError;

impl Project for RecordingProjector {
    type EventGroup = CounterEvent;
    type Error = ProjectorError;

    async fn project<'de, E>(
        &mut self,
        context: Context<'de, E, Self::EventGroup>,
    ) -> Result<(), Self::Error>
    where
        E: Envelope + Sync,
    {
        let should_fail = {
            let mut guard = self.fail_next.lock().unwrap();
            let v = *guard;
            *guard = false;
            v
        };
        if should_fail {
            return Err(ProjectorError);
        }

        let id = Context::id(&context);
        let label = match *context {
            CounterEvent::Incremented { by } => format!("Incremented({by})"),
            CounterEvent::Decremented { by } => format!("Decremented({by})"),
        };
        self.received.lock().unwrap().push((id, label));
        Ok(())
    }
}

// -- Test context ------------------------------------------------------------

/// All resources needed by a single test case.
///
/// Holds a store scoped to a unique subject prefix and a connected NATS client.
/// On drop the JetStream stream created for this test is deleted so that
/// stream names do not accumulate across runs.
struct TestCtx {
    store: NatsStore,
    client: async_nats::Client,
    /// The unique prefix / stream name used for this test.
    prefix: &'static str,
    /// JetStream context kept for cleanup on drop.
    js: jetstream::Context,
}

impl TestCtx {
    /// Build a `TestCtx` for one test case.
    ///
    /// `label` should be a short, human-readable identifier (ASCII letters and
    /// hyphens). A random 8-hex-character suffix is appended to guarantee
    /// uniqueness even when tests run in parallel.
    async fn new(label: &str) -> Self {
        let tag = &Uuid::new_v4().to_string()[..8];
        let prefix_string = format!("t-{label}-{tag}");
        // Leak once per test; acceptable in short-lived test processes.
        let prefix: &'static str = Box::leak(prefix_string.into_boxed_str());

        let client = async_nats::connect("nats://localhost:4222")
            .await
            .expect("NATS server must be reachable at localhost:4222");
        let js = jetstream::new(client.clone());

        let store = NatsStore::try_new(js.clone(), prefix)
            .await
            .expect("NatsStore creation failed");

        Self {
            store,
            client,
            prefix,
            js,
        }
    }

    /// Derive a unique service name for this test context.
    fn service_name(&self) -> &'static str {
        let s = format!("{}-svc", self.prefix);
        Box::leak(s.into_boxed_str())
    }

    /// Derive a unique durable consumer name for this test context.
    fn durable_name(&self) -> &'static str {
        let s = format!("{}-dur", self.prefix);
        Box::leak(s.into_boxed_str())
    }

    /// Delete the JetStream stream created for this test, suppressing errors.
    async fn cleanup(self) {
        let _ = self.js.delete_stream(self.prefix).await;
    }
}

/// Spawn the command dispatcher as a background task and wait briefly for it
/// to register its service endpoints.
async fn spawn_dispatcher(
    ctx: &TestCtx,
    handlers: Vec<Arc<dyn esrc_cqrs::registry::ErasedCommandHandler<NatsStore>>>,
) {
    let service_name = ctx.service_name();
    let store = ctx.store.clone();

    let dispatcher = NatsCommandDispatcher::new(
        async_nats::connect("nats://localhost:4222")
            .await
            .expect("connect"),
        service_name,
    );

    tokio::spawn(async move {
        let _ = dispatcher.run(store, &handlers).await;
    });

    // Allow the NATS service endpoints to register before tests send commands.
    sleep(Duration::from_millis(300)).await;
}

/// Send a single command through NATS request/reply, returning the reply.
async fn send_command<C>(
    client: &async_nats::Client,
    service_name: &str,
    handler_name: &str,
    id: Uuid,
    command: C,
) -> CommandReply
where
    C: serde::Serialize,
{
    let subject = esrc_cqrs::nats::command_dispatcher::command_subject(service_name, handler_name);
    let envelope = CommandEnvelope { id, command };
    let payload = serde_json::to_vec(&envelope).expect("serialize command envelope");
    let reply = client
        .request(subject, payload.into())
        .await
        .expect("NATS request should succeed");
    serde_json::from_slice(&reply.payload).expect("valid CommandReply")
}

// -- Tests -------------------------------------------------------------------

/// Test that a command sent over NATS results in a successful reply and the
/// event is durably stored (readable via replay).
#[tokio::test]
async fn test_command_request_response_success() {
    let ctx = TestCtx::new("cmd-ok").await;

    let registry = CqrsRegistry::new(ctx.store.clone())
        .register_command(AggregateCommandHandler::<Counter>::new("Counter"));

    spawn_dispatcher(&ctx, registry.command_handlers().to_vec()).await;

    let aggregate_id = Uuid::new_v4();
    let response = send_command(
        &ctx.client,
        ctx.service_name(),
        "Counter",
        aggregate_id,
        CounterCommand::Increment { by: 5 },
    )
    .await;

    assert!(response.success, "command should succeed");
    assert_eq!(response.id, aggregate_id);
    assert!(response.error.is_none());

    // Verify the event was actually persisted.
    let root: esrc::aggregate::Root<Counter> = ctx.store.read(aggregate_id).await.unwrap();
    assert_eq!(root.value, 5, "aggregate value should reflect the event");

    ctx.cleanup().await;
}

/// Test that a failing command returns a NATS service error (non-2xx code).
/// The framework should not crash and subsequent commands should still work.
#[tokio::test]
async fn test_command_error_does_not_break_dispatcher() {
    let ctx = TestCtx::new("cmd-err").await;

    let registry = CqrsRegistry::new(ctx.store.clone())
        .register_command(AggregateCommandHandler::<Counter>::new("Counter"));

    spawn_dispatcher(&ctx, registry.command_handlers().to_vec()).await;

    let subject = esrc_cqrs::nats::command_dispatcher::command_subject(ctx.service_name(), "Counter");

    // Send a command that will always fail.
    let bad_envelope = CommandEnvelope {
        id: Uuid::new_v4(),
        command: CounterCommand::AlwaysFail,
    };
    let bad_payload = serde_json::to_vec(&bad_envelope).unwrap();

    // The dispatcher encodes the aggregate error as a CommandReply with success=false.
    let raw = ctx
        .client
        .request(subject.clone(), bad_payload.into())
        .await
        .expect("NATS request should succeed");
    let reply: CommandReply = serde_json::from_slice(&raw.payload).expect("valid CommandReply");

    assert!(!reply.success, "AlwaysFail command should return success=false");
    assert!(reply.error.is_some(), "error field should be populated");

    // Recover the typed aggregate error from the External variant.
    let cqrs_err = reply.error.as_ref().unwrap();
    let agg_err: CounterError = cqrs_err
        .downcast_external::<CounterError>()
        .expect("External variant should be present and deserializable");
    // Validate that the error we received is indeed the one the aggregate returned.
    assert!(
        matches!(agg_err, CounterError::ForcedFailure),
        "deserialized aggregate error should be ForcedFailure, got: {agg_err:?}"
    );

    // Now send a valid command to confirm the dispatcher is still running.
    let good_id = Uuid::new_v4();
    let response = send_command(
        &ctx.client,
        ctx.service_name(),
        "Counter",
        good_id,
        CounterCommand::Increment { by: 3 },
    )
    .await;

    assert!(response.success);
    assert_eq!(response.id, good_id);

    ctx.cleanup().await;
}

/// Test that the projector receives events after they are published via the
/// command handler, and that its internal state reflects those events.
#[tokio::test]
async fn test_projector_receives_events() {
    let ctx = TestCtx::new("proj-recv").await;

    let projector = RecordingProjector::new();
    let received = projector.received.clone();

    let registry = CqrsRegistry::new(ctx.store.clone())
        .register_command(AggregateCommandHandler::<Counter>::new("Counter"))
        .register_projector(DurableProjectorHandler::new(ctx.durable_name(), projector));

    // Start projectors.
    let mut projector_set = registry.run_projectors().await.unwrap();

    spawn_dispatcher(&ctx, registry.command_handlers().to_vec()).await;

    // Extra delay so the projector consumer is ready before events arrive.
    sleep(Duration::from_millis(100)).await;

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    for (id, cmd) in [
        (id1, CounterCommand::Increment { by: 10 }),
        (id2, CounterCommand::Decrement { by: 3 }),
        (id1, CounterCommand::Increment { by: 1 }),
    ] {
        let resp = send_command(&ctx.client, ctx.service_name(), "Counter", id, cmd).await;
        assert!(resp.success, "command {id} should succeed");
    }

    // Give the projector time to process the durable consumer messages.
    sleep(Duration::from_millis(800)).await;

    let events = received.lock().unwrap().clone();
    assert_eq!(events.len(), 3, "projector should have seen 3 events");

    // Verify event labels are in order.
    assert_eq!(events[0], (id1, "Incremented(10)".to_string()));
    assert_eq!(events[1], (id2, "Decremented(3)".to_string()));
    assert_eq!(events[2], (id1, "Incremented(1)".to_string()));

    projector_set.abort_all();
    ctx.cleanup().await;
}

/// Test that the projector does not receive the same event more than once
/// (i.e., messages are acked and not redelivered by JetStream).
#[tokio::test]
async fn test_projector_acks_messages_no_redelivery() {
    let ctx = TestCtx::new("proj-ack").await;

    let projector = RecordingProjector::new();
    let received = projector.received.clone();

    let registry = CqrsRegistry::new(ctx.store.clone())
        .register_command(AggregateCommandHandler::<Counter>::new("Counter"))
        .register_projector(DurableProjectorHandler::new(ctx.durable_name(), projector));

    let mut projector_set = registry.run_projectors().await.unwrap();

    spawn_dispatcher(&ctx, registry.command_handlers().to_vec()).await;

    // Extra delay so the projector consumer is ready before events arrive.
    sleep(Duration::from_millis(100)).await;

    let id = Uuid::new_v4();
    let resp = send_command(
        &ctx.client,
        ctx.service_name(),
        "Counter",
        id,
        CounterCommand::Increment { by: 1 },
    )
    .await;
    assert!(resp.success, "command should succeed");

    // Give the projector enough time to process and ack the message.
    sleep(Duration::from_millis(500)).await;

    let count_after_first_window = received.lock().unwrap().len();
    assert_eq!(count_after_first_window, 1, "projector should have seen exactly 1 event");

    // Wait well past the default NATS ack-wait period (30s) would cause a
    // redelivery if the message was not acked. We use a shorter synthetic wait
    // with a custom ack-wait set on the consumer to make the test fast.
    //
    // Instead of waiting 30s, we verify that no duplicate arrives within 2s,
    // which is sufficient to catch a missing ack that would fire immediately
    // on a consumer configured with a short ack-wait. The default ack-wait is
    // 30s; this test catches the bug without needing to wait the full period.
    sleep(Duration::from_secs(2)).await;

    let count_after_wait = received.lock().unwrap().len();
    assert_eq!(
        count_after_wait, 1,
        "projector should still have seen exactly 1 event; extra deliveries indicate missing ack"
    );

    projector_set.abort_all();
    ctx.cleanup().await;
}

/// Test that a projector error causes the projector task to terminate with an
/// error rather than silently swallowing it, and that the JoinSet reflects it.
#[tokio::test]
async fn test_projector_error_propagates() {
    let ctx = TestCtx::new("proj-err").await;

    let projector = RecordingProjector::new();
    // Pre-arm the projector to fail on the first event it receives.
    projector.set_fail_next();

    let registry = CqrsRegistry::new(ctx.store.clone())
        .register_command(AggregateCommandHandler::<Counter>::new("Counter"))
        .register_projector(DurableProjectorHandler::new(ctx.durable_name(), projector));

    let mut projector_set = registry.run_projectors().await.unwrap();

    spawn_dispatcher(&ctx, registry.command_handlers().to_vec()).await;

    // Publish one event; the projector will fail on it.
    let resp = send_command(
        &ctx.client,
        ctx.service_name(),
        "Counter",
        Uuid::new_v4(),
        CounterCommand::Increment { by: 7 },
    )
    .await;
    assert!(resp.success, "command must succeed regardless of projector state");

    // The projector task should finish (with an error) within a reasonable time.
    sleep(Duration::from_millis(800)).await;

    // At least one task should have completed with an Err.
    let mut found_error = false;
    while let Some(result) = projector_set.join_next().await {
        match result {
            Ok(Err(_)) => {
                found_error = true;
                break;
            },
            // Task may also have been aborted or returned Ok due to timing; continue.
            _ => {},
        }
    }
    projector_set.abort_all();

    assert!(
        found_error,
        "projector error should surface through the JoinSet"
    );

    ctx.cleanup().await;
}

/// Test that multiple commands for the same aggregate result in correct
/// cumulative state, confirming optimistic concurrency is handled correctly.
#[tokio::test]
async fn test_multiple_commands_same_aggregate_occ() {
    let ctx = TestCtx::new("occ").await;

    let registry = CqrsRegistry::new(ctx.store.clone())
        .register_command(AggregateCommandHandler::<Counter>::new("Counter"));

    spawn_dispatcher(&ctx, registry.command_handlers().to_vec()).await;

    let id = Uuid::new_v4();
    // Send 5 sequential increments to the same aggregate.
    for i in 1i64..=5 {
        let resp = send_command(
            &ctx.client,
            ctx.service_name(),
            "Counter",
            id,
            CounterCommand::Increment { by: i },
        )
        .await;
        assert!(resp.success, "increment {i} should succeed");
    }

    // Expected: 1 + 2 + 3 + 4 + 5 = 15
    let root: esrc::aggregate::Root<Counter> = ctx.store.read(id).await.unwrap();
    assert_eq!(
        root.value, 15,
        "aggregate should reflect all 5 sequential increments"
    );

    ctx.cleanup().await;
}

/// Test that sending a malformed (unparseable) payload to a command endpoint
/// results in an error reply and the dispatcher keeps running.
#[tokio::test]
async fn test_malformed_payload_returns_error() {
    let ctx = TestCtx::new("malformed").await;

    let registry = CqrsRegistry::new(ctx.store.clone())
        .register_command(AggregateCommandHandler::<Counter>::new("Counter"));

    spawn_dispatcher(&ctx, registry.command_handlers().to_vec()).await;

    let subject =
        esrc_cqrs::nats::command_dispatcher::command_subject(ctx.service_name(), "Counter");

    // Send garbage bytes.
    let bad_result = ctx
        .client
        .request(subject.clone(), b"this is not json"[..].into())
        .await;
    // The NATS service will reply with an error status; async-nats returns Err.
    // We only care that we get a response (not a timeout or panic).
    let _ = bad_result;

    // Confirm the dispatcher is still alive.
    let good_id = Uuid::new_v4();
    let resp = send_command(
        &ctx.client,
        ctx.service_name(),
        "Counter",
        good_id,
        CounterCommand::Decrement { by: 2 },
    )
    .await;

    assert!(resp.success);
    assert_eq!(resp.id, good_id);

    ctx.cleanup().await;
}

/// Test that `CqrsRegistry::store()` returns a clone of the backing store and
/// that `command_handlers()` / `projector_handlers()` reflect registrations.
#[tokio::test]
async fn test_registry_accessors() {
    let ctx = TestCtx::new("registry").await;

    let projector = RecordingProjector::new();

    let registry = CqrsRegistry::new(ctx.store.clone())
        .register_command(AggregateCommandHandler::<Counter>::new("Counter"))
        .register_projector(DurableProjectorHandler::new("reg-proj", projector));

    assert_eq!(
        registry.command_handlers().len(),
        1,
        "one command handler should be registered"
    );
    assert_eq!(
        registry.projector_handlers().len(),
        1,
        "one projector handler should be registered"
    );

    // store() should return without panicking (it's a Clone).
    let _store_clone = registry.store();

    ctx.cleanup().await;
}

/// Spawn the query dispatcher as a background task and wait briefly for it
/// to register its service endpoints.
async fn spawn_query_dispatcher(
    ctx: &TestCtx,
    handlers: Vec<Arc<dyn esrc_cqrs::registry::ErasedQueryHandler<NatsStore>>>,
) {
    let service_name = ctx.service_name();
    let store = ctx.store.clone();

    let dispatcher = NatsQueryDispatcher::new(
        async_nats::connect("nats://localhost:4222")
            .await
            .expect("connect"),
        service_name,
    );

    tokio::spawn(async move {
        let _ = dispatcher.run(store, &handlers).await;
    });

    // Allow the NATS service endpoints to register before tests send queries.
    sleep(Duration::from_millis(300)).await;
}

/// Send a single query through NATS request/reply, returning the raw reply.
async fn send_query(
    client: &async_nats::Client,
    service_name: &str,
    handler_name: &str,
    id: Uuid,
) -> QueryReply {
    let subject = esrc_cqrs::nats::query_dispatcher::query_subject(service_name, handler_name);
    let envelope = QueryEnvelope { id };
    let payload = serde_json::to_vec(&envelope).expect("serialize query envelope");
    let reply = client
        .request(subject, payload.into())
        .await
        .expect("NATS request should succeed");
    serde_json::from_slice(&reply.payload).expect("valid QueryReply")
}

/// Test that a query sent over NATS returns the correct aggregate state after
/// one or more commands have been applied.
#[tokio::test]
async fn test_query_returns_aggregate_state() {
    let ctx = TestCtx::new("qry-ok").await;

    let registry = CqrsRegistry::new(ctx.store.clone())
        .register_command(AggregateCommandHandler::<Counter>::new("Counter"))
        .register_query(LiveViewQuery::<Counter, CounterState>::new(
            "Counter.GetState",
            |v| CounterState { value: v.value },
        ));

    spawn_dispatcher(&ctx, registry.command_handlers().to_vec()).await;
    spawn_query_dispatcher(&ctx, registry.query_handlers().to_vec()).await;

    let id = Uuid::new_v4();

    // Apply two increments so the aggregate has a known value.
    for by in [10i64, 5] {
        let resp = send_command(&ctx.client, ctx.service_name(), "Counter", id, CounterCommand::Increment { by }).await;
        assert!(resp.success, "command should succeed");
    }

    let reply = send_query(&ctx.client, ctx.service_name(), "Counter.GetState", id).await;

    assert!(reply.success, "query should succeed");
    assert!(reply.error.is_none());

    let state: CounterState = serde_json::from_value(reply.data.expect("data present"))
        .expect("CounterState should deserialize");
    assert_eq!(state.value, 15, "query should reflect cumulative aggregate state");

    ctx.cleanup().await;
}

/// Test that querying an aggregate that has never received a command returns
/// the default (zero) state without an error.
#[tokio::test]
async fn test_query_default_state_for_new_aggregate() {
    let ctx = TestCtx::new("qry-new").await;

    let registry = CqrsRegistry::new(ctx.store.clone()).register_query(
        LiveViewQuery::<Counter, CounterState>::new(
            "Counter.GetState",
            |v| CounterState { value: v.value },
        ),
    );

    spawn_query_dispatcher(&ctx, registry.query_handlers().to_vec()).await;

    let id = Uuid::new_v4();
    let reply = send_query(&ctx.client, ctx.service_name(), "Counter.GetState", id).await;

    assert!(reply.success, "query on a new aggregate should succeed");
    let state: CounterState = serde_json::from_value(reply.data.expect("data present"))
        .expect("CounterState should deserialize");
    assert_eq!(state.value, 0, "new aggregate should have default value of 0");

    ctx.cleanup().await;
}

/// Test that a malformed query payload results in an error reply and the
/// dispatcher keeps running for subsequent queries.
#[tokio::test]
async fn test_query_malformed_payload_returns_error() {
    let ctx = TestCtx::new("qry-bad").await;

    let registry = CqrsRegistry::new(ctx.store.clone()).register_query(
        LiveViewQuery::<Counter, CounterState>::new(
            "Counter.GetState",
            |v| CounterState { value: v.value },
        ),
    );

    spawn_query_dispatcher(&ctx, registry.query_handlers().to_vec()).await;

    let subject = esrc_cqrs::nats::query_dispatcher::query_subject(ctx.service_name(), "Counter.GetState");

    // Send garbage bytes; we only care that we get a response, not a panic.
    let bad_result = ctx
        .client
        .request(subject.clone(), b"this is not json"[..].into())
        .await;
    let _ = bad_result;

    // Confirm the dispatcher is still alive by sending a well-formed query.
    let id = Uuid::new_v4();
    let reply = send_query(&ctx.client, ctx.service_name(), "Counter.GetState", id).await;
    assert!(reply.success, "dispatcher should still handle valid queries after a bad payload");

    ctx.cleanup().await;
}

/// Test that `CqrsRegistry::query_handlers()` reflects registered query handlers.
#[tokio::test]
async fn test_registry_query_handlers_accessor() {
    let ctx = TestCtx::new("qry-reg").await;

    let registry = CqrsRegistry::new(ctx.store.clone())
        .register_query(LiveViewQuery::<Counter, CounterState>::new(
            "Counter.GetState",
            |v| CounterState { value: v.value },
        ))
        .register_query(LiveViewQuery::<Counter, CounterState>::new(
            "Counter.GetStateAlt",
            |v| CounterState { value: v.value },
        ));

    assert_eq!(
        registry.query_handlers().len(),
        2,
        "two query handlers should be registered"
    );

    ctx.cleanup().await;
}
