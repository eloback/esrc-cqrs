//! Cafe example demonstrating `esrc-cqrs` usage with NATS.
//!
//! Run with:
//!   cargo run --example cafe --features nats
//!
//! Requires a local NATS server with JetStream enabled:
//!   nats-server -js

mod domain;
mod projector;
mod service;

use std::time::Duration;

use esrc::nats::NatsStore;
use esrc_cqrs::nats::client::CqrsClient;
use esrc_cqrs::nats::command::ServiceCommandHandler;
use esrc_cqrs::nats::command::{AggregateCommandHandler, CommandEnvelope, CommandReply};
use esrc_cqrs::nats::query::{LiveViewQuery, MemoryView, MemoryViewQuery};
use esrc_cqrs::nats::{
    DurableProjectorHandler, NatsCommandDispatcher, NatsQueryDispatcher, QueryEnvelope, QueryReply,
};
use esrc_cqrs::CqrsRegistry;
use tokio::time::sleep;
use uuid::Uuid;

use crate::domain::{Order, OrderCommand, OrderState};
use crate::projector::OrderProjector;
use crate::service::{CafeCommands, CafeServiceHandler};

const NATS_URL: &str = "nats://localhost:4222";
const STORE_PREFIX: &str = "cafe";
const COMMAND_SERVICE_NAME: &str = "cafe-command";
const PROJECTOR_DURABLE: &str = "cafe-orders";
/// Query service name, kept separate from the command service to avoid subject collisions.
const QUERY_SERVICE_NAME: &str = "cafe-query";

/// Service command handler name, used as its NATS endpoint name.
const SERVICE_HANDLER_NAME: &str = "CafeService";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect(NATS_URL).await?;
    let jetstream = async_nats::jetstream::new(client.clone());
    let store = NatsStore::try_new(jetstream, STORE_PREFIX).await?;

    let order_state_memory_view = MemoryView::<OrderState>::new();

    let registry = CqrsRegistry::new(store.clone())
        .register_command(AggregateCommandHandler::<Order>::new("Order"))
        .register_command(ServiceCommandHandler::new(
            CafeServiceHandler,
            SERVICE_HANDLER_NAME,
        ))
        .register_query(
            LiveViewQuery::<OrderState, OrderState>::new_for_serializable_view("Order.GetState"),
        )
        .register_query(
            MemoryViewQuery::<OrderState, OrderState>::new_for_serializable_view(
                "Order.GetState.MemoryView",
                order_state_memory_view.clone(),
            ),
        )
        .register_projector(DurableProjectorHandler::new(
            "order_state_memory_view",
            order_state_memory_view,
        ))
        .register_projector(DurableProjectorHandler::new(
            PROJECTOR_DURABLE,
            OrderProjector::default(),
        ));

    // Spawn all projectors as background tasks.
    let mut projector_set = registry.run_projectors().await?;

    // Spawn a client driver task that sends commands after a brief delay.
    let driver_client = client.clone();
    tokio::spawn(async move {
        // Give the dispatcher a moment to start listening.
        sleep(Duration::from_millis(500)).await;

        let order_id = Uuid::new_v4();

        // --- AggregateCommandHandler path (existing) ---

        // Place an order using CqrsClient::dispatch_command, which converts
        // a failed reply into Err automatically.
        let cqrs = CqrsClient::new(driver_client.clone());
        let placed_id = cqrs
            .dispatch_command(
                COMMAND_SERVICE_NAME,
                "Order",
                order_id,
                OrderCommand::PlaceOrder {
                    item: "Espresso".to_string(),
                    quantity: 2,
                },
            )
            .await
            .expect("PlaceOrder command failed");
        println!("[client] PlaceOrder dispatch_command id: {:?}", placed_id);
        assert_eq!(placed_id, order_id);

        sleep(Duration::from_millis(200)).await;

        // Query the order state using CqrsClient::dispatch_query for a typed result.
        let order_state: OrderState = cqrs
            .dispatch_query(QUERY_SERVICE_NAME, "Order.GetState", order_id)
            .await
            .expect("Order.GetState query failed");
        println!("[client] Order.GetState dispatch_query: {:?}", order_state);
        assert_eq!(order_state.item.as_deref(), Some("Espresso"));

        // Query the order state using CqrsClient::dispatch_query for a typed result.
        let order_state: OrderState = cqrs
            .dispatch_query(QUERY_SERVICE_NAME, "Order.GetState.MemoryView", order_id)
            .await
            .expect("Order.GetState.MemoryView query failed");
        println!(
            "[client] Order.GetState.MemoryView dispatch_query: {:?}",
            order_state
        );
        assert_eq!(order_state.item.as_deref(), Some("Espresso"));

        sleep(Duration::from_millis(200)).await;

        // Using manual request/reply to demonstrate access to raw CommandReply and QueryReply
        // fields.

        // Complete the order using CqrsClient::send_command to inspect the raw reply.
        let complete_cmd = CommandEnvelope {
            id: order_id,
            command: OrderCommand::CompleteOrder,
        };
        let payload = serde_json::to_vec(&complete_cmd).expect("serialize complete command");
        // Construct the subject manually, can be reused for other commands in the same aggregate.
        let subject =
            esrc_cqrs::nats::command_dispatcher::command_subject(COMMAND_SERVICE_NAME, "Order");
        let reply = driver_client
            .request(subject.clone(), payload.into())
            .await
            .unwrap();
        let reply: CommandReply = serde_json::from_slice(&reply.payload).unwrap();
        println!("[client] CompleteOrder reply: {:?}", reply);
        assert!(reply.success);

        sleep(Duration::from_millis(200)).await;

        // Query again using send_query to access the raw QueryReply fields.
        let query_subject =
            esrc_cqrs::nats::query_dispatcher::query_subject(QUERY_SERVICE_NAME, "Order.GetState");
        let query_payload =
            serde_json::to_vec(&QueryEnvelope { id: order_id }).expect("serialize query");
        let reply = driver_client
            .request(query_subject.clone(), query_payload.into())
            .await
            .unwrap();
        let reply: QueryReply = serde_json::from_slice(&reply.payload).unwrap();
        println!("[client] Order.GetState reply: {:?}", reply);
        assert!(reply.success);

        // Let the projector process the events before shutdown.
        sleep(Duration::from_millis(200)).await;

        // --- ServiceCommandHandler path (new) ---

        let service_order_id = Uuid::new_v4();

        // Place an order via the service command handler.
        let placed_id: Uuid = cqrs
            .dispatch_service_command(
                COMMAND_SERVICE_NAME,
                SERVICE_HANDLER_NAME,
                CafeCommands::PlaceOrder {
                    id: service_order_id,
                    item: "Latte".to_string(),
                    quantity: 1,
                },
            )
            .await
            .expect("CafeService PlaceOrder should succeed");
        println!("[client] CafeService PlaceOrder id: {:?}", placed_id);
        assert_eq!(placed_id, service_order_id);

        sleep(Duration::from_millis(200)).await;

        // Complete the order via the service command handler.
        let completed_id: Uuid = cqrs
            .dispatch_service_command(
                COMMAND_SERVICE_NAME,
                SERVICE_HANDLER_NAME,
                CafeCommands::CompleteOrder {
                    id: service_order_id,
                },
            )
            .await
            .expect("CafeService CompleteOrder should succeed");
        println!("[client] CafeService CompleteOrder id: {:?}", completed_id);
        assert_eq!(completed_id, service_order_id);

        sleep(Duration::from_secs(1)).await;
    });

    // Build and run the command dispatcher (blocks until NATS closes or an error occurs).
    let dispatcher = NatsCommandDispatcher::new(client.clone(), COMMAND_SERVICE_NAME);
    // Spawn the query dispatcher alongside the command dispatcher.
    let query_dispatcher = NatsQueryDispatcher::new(client.clone(), QUERY_SERVICE_NAME);
    let query_store = store.clone();
    let query_handlers: Vec<_> = registry.query_handlers().to_vec();
    tokio::spawn(async move {
        if let Err(e) = query_dispatcher.run(query_store, &query_handlers).await {
            eprintln!("[query dispatcher] error: {e}");
        }
    });
    tokio::spawn(async move {
        if let Err(e) = dispatcher
            .run(store.clone(), registry.command_handlers())
            .await
        {
            eprintln!("[command dispatcher] error: {e}");
        }
    });

    // Wait for projectors to finish (they run indefinitely in normal operation).
    while let Some(result) = projector_set.join_next().await {
        result??;
    }

    Ok(())
}
