/// Create Request
#[derive(Clone, PartialEq, ::prost::Message, ::serde::Serialize, ::serde::Deserialize)]
pub struct CreateRequest {
  #[prost(string, tag = "1")]
  pub queue: std::string::String,
  #[prost(string, tag = "2")]
  pub data: std::string::String,
  #[prost(uint32, tag = "3")]
  pub retry: u32,
  #[prost(uint64, tag = "4")]
  pub delay: u64,
}
/// Create Response
#[derive(Clone, PartialEq, ::prost::Message, ::serde::Serialize, ::serde::Deserialize)]
pub struct CreateResponse {
  #[prost(string, tag = "1")]
  pub task_id: std::string::String,
}
/// Acknowledge Request
#[derive(Clone, PartialEq, ::prost::Message, ::serde::Serialize, ::serde::Deserialize)]
pub struct AckRequest {
  #[prost(string, tag = "1")]
  pub task_id: std::string::String,
  #[prost(string, tag = "2")]
  pub queue: std::string::String,
  #[prost(enumeration = "TaskStatus", tag = "3")]
  pub status: i32,
  #[prost(string, tag = "4")]
  pub message: std::string::String,
}
/// Fetch Request
#[derive(Clone, PartialEq, ::prost::Message, ::serde::Serialize, ::serde::Deserialize)]
pub struct FetchRequest {
  #[prost(string, tag = "1")]
  pub hostname: std::string::String,
  #[prost(string, tag = "2")]
  pub queue: std::string::String,
  #[prost(string, repeated, tag = "3")]
  pub label: ::std::vec::Vec<std::string::String>,
}
/// FetchResponse
#[derive(Clone, PartialEq, ::prost::Message, ::serde::Serialize, ::serde::Deserialize)]
pub struct FetchResponse {
  #[prost(string, tag = "1")]
  pub id: std::string::String,
  #[prost(string, tag = "2")]
  pub queue: std::string::String,
  #[prost(string, tag = "3")]
  pub data: std::string::String,
}
/// Empty response
#[derive(Clone, PartialEq, ::prost::Message, ::serde::Serialize, ::serde::Deserialize)]
pub struct Empty {}
/// Task status
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
#[derive(::serde::Serialize, ::serde::Deserialize)]
pub enum TaskStatus {
  Done = 0,
  Failed = 1,
  Canceled = 2,
}
