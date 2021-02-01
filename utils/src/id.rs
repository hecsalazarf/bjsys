use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use std::fmt;

static GENERATOR: IdGenerator = IdGenerator {
  sequence: AtomicU64::new(0),
  last: AtomicU64::new(0),
};

pub struct IdGenerator {
  sequence: AtomicU64,
  last: AtomicU64,
}

impl IdGenerator {
  fn generate(&self) -> IncrId {
    let now = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;

    let seq = if now > self.last.load(Ordering::Relaxed) {
      self.last.store(now, Ordering::Relaxed);
      self.sequence.store(0, Ordering::Relaxed);
      self.sequence.fetch_add(1, Ordering::Relaxed)
    } else {
      self.sequence.fetch_add(1, Ordering::Relaxed)
    };
    IncrId(now, seq)
  }

  pub const fn encode_buffer() -> [u8; IncrId::LENGHT] {
    [0; IncrId::LENGHT]
  }
}

pub struct IncrId(u64, u64);

impl IncrId {
  pub const LENGHT: usize = 41;

  pub fn encode<'a>(&self, buffer: &'a mut [u8]) -> &'a str {
    use std::io::Write;
    write!(&mut buffer[..], "{}-{}", self.0, self.1).unwrap();
    let encoded = unsafe {
      // Safe because we encode only valid UTF-8: {u64}-{u64}
      std::str::from_utf8_unchecked(buffer)
    };
    encoded.trim_matches(char::from(0))
  }
}

pub fn generate_id() -> IncrId {
  GENERATOR.generate()
}

impl fmt::Display for IncrId {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut buffer = IdGenerator::encode_buffer();
    f.write_str(self.encode(&mut buffer))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn id_buffer() {
    let buffer = IdGenerator::encode_buffer();
    // Buffer length
    assert_eq!(buffer.len(), IncrId::LENGHT);
    for value in buffer.iter() {
      // Buffer initialization
      assert_eq!(value, &0);
    }
  }

  #[test]
  fn id() {
    let mut buffer = IdGenerator::encode_buffer();
    let id = GENERATOR.generate();
    println!("ID: {}",id.encode(&mut buffer));
  }

  #[test]
  fn id_spawn() {
    for i in 0..10 {
      std::thread::spawn(move || {
        let mut buffer = IdGenerator::encode_buffer();
        let id = generate_id();
        println!("ID[{}]: {}", i, id.encode(&mut buffer));
      });
    }
  }

  #[test]
  fn id_to_string() {
    let id = GENERATOR.generate();
    let id_string = id.to_string();
    println!("ID: {}", id_string);
    assert!(id_string.len() > 0);
  }
}
