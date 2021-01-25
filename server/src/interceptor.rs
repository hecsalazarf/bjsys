use proto::MessageValidator;
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
      use prost::Message;
      use proto::errors::BadRequest;

      let br = BadRequest::from(errors);
      let mut buffer = Vec::with_capacity(br.encoded_len());
      // Never fails as the buffer has sufficient capacity
      br.encode(&mut buffer).unwrap();

      Err(Status::with_details(
        Code::InvalidArgument,
        "Payload is not valid",
        buffer.into(),
      ))
    }
  }
}
