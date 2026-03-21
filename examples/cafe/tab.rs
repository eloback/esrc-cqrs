use esrc::version::{DeserializeVersion, SerializeVersion};
use esrc::{Aggregate, Event};
use serde::{Deserialize, Serialize};

use crate::error::TabError;

mod tests;

#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
pub struct Item {
    pub menu_number: u64,
    pub description: String,
    pub price: f64,
}

pub enum TabCommand {
    Open { table_number: u64, waiter: String },
    PlaceOrder { items: Vec<Item> },
    MarkServed { menu_numbers: Vec<u64> },
    Close { amount_paid: f64 },
}

#[derive(Event, Deserialize, DeserializeVersion, Serialize, SerializeVersion, PartialEq, Debug)]
pub enum TabEvent {
    Opened {
        table_number: u64,
        waiter: String,
    },
    Ordered {
        items: Vec<Item>,
    },
    Served {
        menu_numbers: Vec<u64>,
    },
    Closed {
        amount_paid: f64,
        order_value: f64,
        tip_value: f64,
    },
}

#[derive(Default)]
pub struct Tab {
    open: bool,
    outstanding_items: Vec<Item>,
    served_value: f64,
}

impl Tab {
    fn are_outstanding(&self, menu_numbers: &Vec<u64>) -> bool {
        let mut curr = self.outstanding_items.clone();
        for n in menu_numbers {
            let index = curr.iter().position(|i| i.menu_number == *n);
            if let Some(index) = index {
                curr.remove(index);
            } else {
                return false;
            }
        }

        true
    }

    fn remove_outstanding(&mut self, menu_numbers: &Vec<u64>) {
        for n in menu_numbers {
            let index = self
                .outstanding_items
                .iter()
                .position(|i| i.menu_number == *n)
                .unwrap();

            self.served_value += self.outstanding_items.get(index).unwrap().price;
            // The items should have been validated to exist already, so unwrap.
            self.outstanding_items.remove(index);
        }
    }
}

impl Aggregate for Tab {
    type Command = TabCommand;
    type Event = TabEvent;
    type Error = TabError;

    fn process(&self, command: Self::Command) -> Result<TabEvent, Self::Error> {
        match command {
            TabCommand::Open {
                table_number,
                waiter,
            } => Ok(TabEvent::Opened {
                table_number,
                waiter,
            }),
            TabCommand::PlaceOrder { items } => {
                if !self.open {
                    Err(TabError::NotOpen)
                } else {
                    Ok(TabEvent::Ordered { items })
                }
            },
            TabCommand::MarkServed { menu_numbers } => {
                if !self.are_outstanding(&menu_numbers) {
                    Err(TabError::AlreadyServed)
                } else {
                    Ok(TabEvent::Served { menu_numbers })
                }
            },
            TabCommand::Close { amount_paid } => {
                if !self.open {
                    Err(TabError::NotOpen)
                } else if !self.outstanding_items.is_empty() {
                    Err(TabError::NotFinished)
                } else if amount_paid < self.served_value {
                    Err(TabError::Unpaid)
                } else {
                    Ok(TabEvent::Closed {
                        amount_paid,
                        order_value: self.served_value,
                        tip_value: amount_paid - self.served_value,
                    })
                }
            },
        }
    }

    fn apply(mut self, event: &Self::Event) -> Self {
        match event {
            TabEvent::Opened { .. } => self.open = true,
            TabEvent::Ordered { ref items } => self.outstanding_items.extend(items.clone()),
            TabEvent::Served { ref menu_numbers } => self.remove_outstanding(menu_numbers),
            TabEvent::Closed { .. } => self.open = false,
        }

        self
    }
}
