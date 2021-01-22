/// Create Request
#[derive(::serde::Serialize, ::serde::Deserialize, Clone, PartialEq, ::prost::Message)]
pub struct CreateRequest {
  #[prost(string, tag = "1")]
  pub queue: ::prost::alloc::string::String,
  #[prost(string, tag = "2")]
  pub data: ::prost::alloc::string::String,
  #[prost(uint32, tag = "3")]
  pub retry: u32,
  #[prost(uint64, tag = "4")]
  pub delay: u64,
}
/// Create Response
#[derive(::serde::Serialize, ::serde::Deserialize, Clone, PartialEq, ::prost::Message)]
pub struct CreateResponse {
  #[prost(string, tag = "1")]
  pub task_id: ::prost::alloc::string::String,
}
/// Acknowledge Request
#[derive(::serde::Serialize, ::serde::Deserialize, Clone, PartialEq, ::prost::Message)]
pub struct AckRequest {
  #[prost(string, tag = "1")]
  pub task_id: ::prost::alloc::string::String,
  #[prost(string, tag = "2")]
  pub queue: ::prost::alloc::string::String,
  #[prost(enumeration = "TaskStatus", tag = "3")]
  pub status: i32,
  #[prost(string, tag = "4")]
  pub message: ::prost::alloc::string::String,
}
/// Fetch Request
#[derive(::serde::Serialize, ::serde::Deserialize, Clone, PartialEq, ::prost::Message)]
pub struct FetchRequest {
  #[prost(string, tag = "1")]
  pub hostname: ::prost::alloc::string::String,
  #[prost(string, tag = "2")]
  pub queue: ::prost::alloc::string::String,
  #[prost(string, repeated, tag = "3")]
  pub label: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// FetchResponse
#[derive(::serde::Serialize, ::serde::Deserialize, Clone, PartialEq, ::prost::Message)]
pub struct FetchResponse {
  #[prost(string, tag = "1")]
  pub id: ::prost::alloc::string::String,
  #[prost(string, tag = "2")]
  pub queue: ::prost::alloc::string::String,
  #[prost(string, tag = "3")]
  pub data: ::prost::alloc::string::String,
}
/// Empty response
#[derive(::serde::Serialize, ::serde::Deserialize, Clone, PartialEq, ::prost::Message)]
pub struct Empty {}
/// Task status
#[derive(
  ::serde::Serialize,
  ::serde::Deserialize,
  Clone,
  Copy,
  Debug,
  PartialEq,
  Eq,
  Hash,
  PartialOrd,
  Ord,
  ::prost::Enumeration,
)]
#[repr(i32)]
pub enum TaskStatus {
  Done = 0,
  Failed = 1,
  Canceled = 2,
}
