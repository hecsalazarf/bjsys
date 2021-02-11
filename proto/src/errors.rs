pub use crate::stub::error_details::*;
use prost::Message;
use validator::{ValidationErrors, ValidationErrorsKind};

impl From<ValidationErrors> for BadRequest {
  fn from(value: ValidationErrors) -> Self {
    let errors = value.into_errors();

    let mut field_violations = Vec::new();
    for (field, kind) in errors {
      if let ValidationErrorsKind::Field(errs) = kind {
        let violations = errs.into_iter().map(|e| bad_request::FieldViolation {
          field: field.to_owned(),
          description: format!("{}, {:?}", e.code, e.params),
        });
        field_violations.append(&mut violations.collect());
      }
    }

    BadRequest { field_violations }
  }
}

impl BadRequest {
  /// Encodes `self` into a `prost` message bytes.
  pub fn into_bytes(self) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(self.encoded_len());
    // Never fails as the buffer has sufficient capacity
    self.encode(&mut buffer).unwrap();
    buffer
  }
}
