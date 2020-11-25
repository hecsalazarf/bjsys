pub use crate::stub::error_details::*;
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
