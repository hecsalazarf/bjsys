/// Describes violations in a client request. This error type focuses on the
/// syntactic aspects of the request.
#[derive(Clone, PartialEq, ::prost::Message, ::serde::Serialize, ::serde::Deserialize)]
pub struct BadRequest {
  /// Describes all violations in a client request.
  #[prost(message, repeated, tag = "1")]
  pub field_violations: ::std::vec::Vec<bad_request::FieldViolation>,
}
pub mod bad_request {
  /// A message type used to describe a single bad request field.
  #[derive(Clone, PartialEq, ::prost::Message, ::serde::Serialize, ::serde::Deserialize)]
  pub struct FieldViolation {
    /// A path leading to a field in the request body. The value will be a
    /// sequence of dot-separated identifiers that identify a protocol buffer
    /// field. E.g., "field_violations.field" would identify this field.
    #[prost(string, tag = "1")]
    pub field: std::string::String,
    /// A description of why the request element is bad.
    #[prost(string, tag = "2")]
    pub description: std::string::String,
  }
}
