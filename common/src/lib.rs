pub mod errors;
mod request;
mod stub;
mod validators;

pub use request::RequestExt;
pub use stub::msg::*;
pub use validators::MessageValidator;

#[cfg(feature = "client")]
pub mod client {
  pub use super::stub::service::tasks_core_client::*;
}

#[cfg(feature = "server")]
pub mod server {
  pub use super::stub::service::tasks_core_server::*;
}

#[cfg(feature = "server")]
pub mod cluster {
  pub mod msg {
    pub use crate::stub::raft::*;
  }

  pub use async_raft::*;

  impl async_raft::AppData for msg::ClientRequest {}
  impl async_raft::AppDataResponse for msg::ClientResponse {}
}
