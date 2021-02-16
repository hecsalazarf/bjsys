mod raft;
mod service;
pub use service::tasks_core_server::*;

pub mod raft2 {
  pub use crate::stub::raft::*;
  use async_raft::{AppData, AppDataResponse};

  impl AppData for ClientRequest {}
  impl AppDataResponse for ClientResponse {}
}
