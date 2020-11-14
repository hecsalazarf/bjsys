use crate::store::TaskHash;
use crate::stub::tasks::{AckRequest, CreateRequest, FetchRequest};
use std::borrow::Cow;
use tonic::{Code, Request, Status};
use validator::{HasLen, ValidationError, ValidationErrors, Validator};

pub trait RequestInterceptor {
  fn intercept(&self) -> Result<(), Status>;
  fn validate_payload(&self) -> Result<(), Status>;
}

impl<T: ValidatePayload> RequestInterceptor for Request<T> {
  fn intercept(&self) -> Result<(), Status> {
    // Validate payload data
    self.validate_payload()?;
    Ok(())
  }

  fn validate_payload(&self) -> Result<(), Status> {
    let errors = self.get_ref().validate();
    if errors.is_empty() {
      Ok(())
    } else {
      // TODO: Implement richer error model to send error details
      // https://grpc.io/docs/guides/error/
      Err(Status::new(Code::InvalidArgument, "Payload is not valid"))
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
  const EQUAL: &'static str = "equal";
  const FOUND: &'static str = "found";
}

pub trait ValidatePayload {
  fn validate(&self) -> ValidationErrors {
    ValidationErrors::new()
  }
}

impl ValidatePayload for CreateRequest {
  fn validate(&self) -> ValidationErrors {
    let mut errors = ValidationErrors::new();
    // Queue length
    if let Err(e) = validate_length(Defaults::STRING_LEN, &self.queue) {
      errors.add(TaskHash::QUEUE, e);
    }
    // Data size
    if let Err(e) = validate_length(Defaults::DATA_SIZE, &self.data) {
      errors.add(TaskHash::DATA, e);
    }
    // Retries range
    if let Err(e) = validate_range(Defaults::RETRY, self.retry as f64) {
      errors.add(TaskHash::RETRY, e);
    }
    errors
  }
}

impl ValidatePayload for AckRequest {
  fn validate(&self) -> ValidationErrors {
    let mut errors = ValidationErrors::new();
    // Task ID length
    if let Err(e) = validate_length(Defaults::STRING_LEN, &self.task_id) {
      errors.add(TaskHash::ID, e);
    }
    // Queue length
    if let Err(e) = validate_length(Defaults::STRING_LEN, &self.queue) {
      errors.add(TaskHash::QUEUE, e);
    }
    // Message length
    if let Err(e) = validate_length(Defaults::STRING_LEN, &self.message) {
      errors.add(TaskHash::MESSAGE, e);
    }
    // Status is always valid. Value is converted to 0 if it exceeds the range

    errors
  }
}

impl ValidatePayload for FetchRequest {
  fn validate(&self) -> ValidationErrors {
    let mut errors = ValidationErrors::new();
    // Hostname length
    if let Err(e) = validate_length(Defaults::STRING_LEN, &self.hostname) {
      errors.add("hostname", e);
    }
    // Queue length
    if let Err(e) = validate_length(Defaults::STRING_LEN, &self.queue) {
      errors.add(TaskHash::QUEUE, e);
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
  let length = value.length();
  if validator::validate_length(validator.clone(), value) {
    return Ok(());
  }

  let mut error = ValidationError::new(validator.code());
  error.message = Some(Cow::from("Length is not valid"));

  match validator {
    Validator::Length { min, max, equal } => {
      if let Some(eq) = equal {
        error.add_param(Cow::from(Params::EQUAL), &eq);
      }
      if let Some(m) = min {
        error.add_param(Cow::from(Params::MIN), &m);
      }
      if let Some(m) = max {
        error.add_param(Cow::from(Params::MAX), &m);
      }
      error.add_param(Cow::from(Params::FOUND), &length);
    }
    _ => unreachable!(),
  }

  Err(error)
}

fn validate_range(validator: Validator, value: f64) -> Result<(), ValidationError> {
  if validator::validate_range(validator.clone(), value) {
    return Ok(());
  }

  let mut error = ValidationError::new(validator.code());
  error.message = Some(Cow::from("Range is not valid"));

  match validator {
    Validator::Range { min, max } => {
      if let Some(m) = min {
        error.add_param(Cow::from(Params::MIN), &m);
      }
      if let Some(m) = max {
        error.add_param(Cow::from(Params::MAX), &m);
      }
      error.add_param(Cow::from(Params::FOUND), &value);
    }
    _ => unreachable!(),
  }

  Err(error)
}
