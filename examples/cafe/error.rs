#[derive(Debug, PartialEq, thiserror::Error)]
pub enum TabError {
    #[error("the tab is not open")]
    NotOpen,
    #[error("requested items already served")]
    AlreadyServed,
    #[error("still waiting on ordered items")]
    NotFinished,
    #[error("the tab has not been fully paid")]
    Unpaid,
}
