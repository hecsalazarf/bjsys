use crate::{AckRequest, CreateRequest, FetchRequest};
use validator::{HasLen, ValidationError, ValidationErrors};

#[derive(Clone)]
enum Validator {
  Range {
    min: Option<f64>,
    max: Option<f64>,
  },
  Length {
    min: Option<u64>,
    max: Option<u64>,
    equal: Option<u64>,
  },
}

impl Validator {
  pub fn code(&self) -> &'static str {
    match *self {
      Validator::Range { .. } => "range",
      Validator::Length { .. } => "length",
    }
  }
}

struct Defaults;

impl Defaults {
  const STRING_LEN: Validator = Validator::Length {
    min: Some(1),
    max: Some(255),
    equal: None,
  };

  const DATA_SIZE: Validator = Validator::Length {
    min: None,
    max: Some(1024),
    equal: None,
  };

  const RETRY: Validator = Validator::Range {
    min: Some(1_f64),
    max: Some(25_f64),
  };

  const LABEL: Validator = Validator::Length {
    min: None,
    max: Some(5),
    equal: None,
  };
}

struct Params;

impl Params {
  const MIN: &'static str = "min";
  const MAX: &'static str = "max";
  const EQUAL: &'static str = "required";
  const FOUND: &'static str = "found";
}

pub trait MessageValidator {
  fn validate(&self) -> ValidationErrors {
    ValidationErrors::new()
  }
}

impl MessageValidator for CreateRequest {
  fn validate(&self) -> ValidationErrors {
    let mut errors = ValidationErrors::new();
    // Queue length
    if let Err(e) = validate_length(Defaults::STRING_LEN, &self.queue) {
      errors.add(FieldName::QUEUE, e);
    }
    // Data size
    if let Err(e) = validate_length(Defaults::DATA_SIZE, &self.data) {
      errors.add(FieldName::DATA, e);
    }
    // Retries range
    if let Err(e) = validate_range(Defaults::RETRY, self.retry as f64) {
      errors.add(FieldName::RETRY, e);
    }
    errors
  }
}

impl MessageValidator for AckRequest {
  fn validate(&self) -> ValidationErrors {
    let mut errors = ValidationErrors::new();
    // Task ID length
    if let Err(e) = validate_length(Defaults::STRING_LEN, &self.task_id) {
      errors.add(FieldName::ID, e);
    }
    // Queue length
    if let Err(e) = validate_length(Defaults::STRING_LEN, &self.queue) {
      errors.add(FieldName::QUEUE, e);
    }

    // Message is optional, so there is no minimum length
    let validator = Validator::Length {
      min: None,
      max: Some(1024),
      equal: None,
    };
    if let Err(e) = validate_length(validator, &self.message) {
      errors.add(FieldName::MESSAGE, e);
    }
    // Status is always valid. Value is converted to 0 if it exceeds the range

    errors
  }
}

impl MessageValidator for FetchRequest {
  fn validate(&self) -> ValidationErrors {
    let mut errors = ValidationErrors::new();
    // Hostname length
    if let Err(e) = validate_length(Defaults::STRING_LEN, &self.hostname) {
      errors.add("hostname", e);
    }
    // Queue length
    if let Err(e) = validate_length(Defaults::STRING_LEN, &self.queue) {
      errors.add(FieldName::QUEUE, e);
    }
    // Label vec length
    if let Err(e) = validate_length(Defaults::LABEL, &self.label) {
      errors.add("label", e);
    }
    // Label length
    for l in self.label.iter() {
      if let Err(e) = validate_length(Defaults::STRING_LEN, l) {
        errors.add("label", e);
        break;
      }
    }
    errors
  }
}

fn validate_length<T: HasLen>(validator: Validator, value: T) -> Result<(), ValidationError> {
  match validator {
    Validator::Length { min, max, equal } => {
      let length = value.length();
      if validator::validate_length(value, min, max, equal) {
        return Ok(());
      }

      let mut error = ValidationError::new(validator.code());
      error.message = Some("Length is not valid".into());
      if let Some(eq) = equal {
        error.add_param(Params::EQUAL.into(), &eq);
      }
      if let Some(m) = min {
        error.add_param(Params::MIN.into(), &m);
      }
      if let Some(m) = max {
        error.add_param(Params::MAX.into(), &m);
      }
      error.add_param(Params::FOUND.into(), &length);
      Err(error)
    }
    _ => unreachable!(),
  }
}

fn validate_range(validator: Validator, value: f64) -> Result<(), ValidationError> {
  match validator {
    Validator::Range { min, max } => {
      if validator::validate_range(value, min, max) {
        return Ok(());
      }

      let mut error = ValidationError::new(validator.code());
      error.message = Some("Range is not valid".into());
      if let Some(m) = min {
        error.add_param(Params::MIN.into(), &m);
      }
      if let Some(m) = max {
        error.add_param(Params::MAX.into(), &m);
      }
      error.add_param(Params::FOUND.into(), &value);
      Err(error)
    }
    _ => unreachable!(),
  }
}

pub struct FieldName;

impl FieldName {
  pub const ID: &'static str = "id";
  pub const DATA: &'static str = "data";
  pub const QUEUE: &'static str = "queue";
  pub const RETRY: &'static str = "retry";
  pub const MESSAGE: &'static str = "message";
}
