use common::{errors::BadRequest, MessageValidator};
use tonic::{Code, Request, Status};

pub trait RequestInterceptor {
  fn intercept(&self) -> Result<(), Status>;
  fn validate_payload(&self) -> Result<(), Status>;
}

impl<T: MessageValidator> RequestInterceptor for Request<T> {
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
      let br_bytes = BadRequest::from(errors).into_bytes();

      Err(Status::with_details(
        Code::InvalidArgument,
        "Payload is not valid",
        br_bytes.into(),
      ))
    }
  }
}
