/// Describes violations in a client request. This error type focuses on the
/// syntactic aspects of the request.
#[derive(::serde::Serialize, ::serde::Deserialize, Clone, PartialEq, ::prost::Message)]
pub struct BadRequest {
  /// Describes all violations in a client request.
  #[prost(message, repeated, tag = "1")]
  pub field_violations: ::prost::alloc::vec::Vec<bad_request::FieldViolation>,
}
/// Nested message and enum types in `BadRequest`.
pub mod bad_request {
  /// A message type used to describe a single bad request field.
  #[derive(::serde::Serialize, ::serde::Deserialize, Clone, PartialEq, ::prost::Message)]
  pub struct FieldViolation {
    /// A path leading to a field in the request body. The value will be a
    /// sequence of dot-separated identifiers that identify a protocol buffer
    /// field. E.g., "field_violations.field" would identify this field.
    #[prost(string, tag = "1")]
    pub field: ::prost::alloc::string::String,
    /// A description of why the request element is bad.
    #[prost(string, tag = "2")]
    pub description: ::prost::alloc::string::String,
  }
}
