use tonic::metadata::MetadataValue;
use tonic::Request;
use uuid::Uuid;

type RequestId = Uuid;

/// Metadata key values
struct MetadataKey;
impl MetadataKey {
  /// Request ID key. Note that binary keys must be suffixed with "-bin", otherwise
  /// `tonic::metadata::MetadataMap` panics at insertion.
  const REQUEST_ID: &'static str = "request-id";
}

/// Convenient methods for `tonic::Request`.
pub trait RequestExt {
  /// Makes the request idempotent by assigning a unique ID.
  fn make_idempotent(&mut self) -> RequestId;

  /// Returns the request ID if it is idempotent, otherwise returns `None`.
  fn id(&self) -> Option<RequestId>;
}

impl<T> RequestExt for Request<T> {
  fn make_idempotent(&mut self) -> RequestId {
    let id = RequestId::new_v4();
    let mut buffer = RequestId::encode_buffer();
    let id_str = id.to_simple_ref().encode_lower(&mut buffer);
    let val = MetadataValue::from_str(id_str).unwrap();
    self.metadata_mut().insert(MetadataKey::REQUEST_ID, val);
    id
  }

  fn id(&self) -> Option<RequestId> {
    let id_opt = self.metadata().get(MetadataKey::REQUEST_ID);
    id_opt.and_then(|val| {
      if let Ok(bytes) = val.to_str() {
        RequestId::parse_str(bytes).ok()
      } else {
        None
      }
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn idempotentency() {
    let mut request = Request::new(true);
    assert_eq!(None, request.id());

    let id = request.make_idempotent();
    assert_eq!(Some(id), request.id());
  }
}
