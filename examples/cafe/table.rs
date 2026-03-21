use std::collections::HashMap;
use std::sync::Arc;

use esrc::project::{Context, Project};
use esrc::Envelope;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::error::TabError;
use crate::tab::TabEvent;

#[derive(Clone)]
pub struct ActiveTables {
    table_numbers: Arc<RwLock<HashMap<Uuid, u64>>>,
}

impl ActiveTables {
    pub fn new() -> Self {
        Self {
            table_numbers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn is_active(&self, table_number: u64) -> bool {
        self.table_numbers
            .read()
            .await
            .values()
            .any(|n| *n == table_number)
    }

    pub async fn get_table_numbers(&self) -> HashMap<Uuid, u64> {
        self.table_numbers.read().await.clone()
    }
}

impl Project for ActiveTables {
    type EventGroup = TabEvent;
    type Error = TabError;

    async fn project<'a, E>(
        &mut self,
        context: Context<'a, E, Self::EventGroup>,
    ) -> Result<(), Self::Error>
    where
        E: Envelope + Sync,
    {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let id = Context::id(&context);
        let mut map = self.table_numbers.write().await;

        if let TabEvent::Opened { table_number, .. } = *context {
            map.insert(id, table_number);
        } else if let TabEvent::Closed { .. } = *context {
            map.remove(&id);
        }

        Ok(())
    }
}
