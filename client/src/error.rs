use serde_json::Error as SerdeError;
use std::{error::Error as StdError, fmt};
use tonic::{transport::Error as TransportError, Status};

pub struct Error {
  kind: ErrorKind,
  source: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl Error {
  fn description(&self) -> &str {
    match self.kind {
      ErrorKind::Transport => "Transport failed",
      ErrorKind::Server => "Server responded with error",
      ErrorKind::Data => "Task data is not valid",
    }
  }

  pub fn is_transport(&self) -> bool {
    matches!(self.kind, ErrorKind::Transport)
  }

  pub fn is_server(&self) -> bool {
    matches!(self.kind, ErrorKind::Server)
  }

  pub fn is_data(&self) -> bool {
    matches!(self.kind, ErrorKind::Data)
  }
}

#[derive(Debug)]
pub(crate) enum ErrorKind {
  Transport,
  Server,
  Data,
}

impl From<SerdeError> for Error {
  fn from(error: SerdeError) -> Self {
    Self {
      kind: ErrorKind::Data,
      source: Some(Box::new(error)),
    }
  }
}

impl From<Status> for Error {
  fn from(error: Status) -> Self {
    Self {
      kind: ErrorKind::Server,
      source: Some(Box::new(error)),
    }
  }
}

impl From<TransportError> for Error {
  fn from(error: TransportError) -> Self {
    Self {
      kind: ErrorKind::Transport,
      source: Some(Box::new(error)),
    }
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if let Some(source) = &self.source {
      write!(f, "{}: {}", self.description(), source)
    } else {
      f.write_str(self.description())
    }
  }
}

impl fmt::Debug for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut f = f.debug_tuple("bjsys::Error");

    f.field(&self.kind);

    if let Some(source) = &self.source {
      f.field(source);
    }

    f.finish()
  }
}

impl StdError for Error {
  fn source(&self) -> Option<&(dyn StdError + 'static)> {
    self
      .source
      .as_ref()
      .map(|source| &**source as &(dyn StdError + 'static))
  }
}
