//! A hash in a B+ tree.

use sled::{Batch, Db, IVec, Result, Tree};
use std::ops::Deref;

/// A hash in a B+ tree. Practically a wrapper around sled `Tree` with added
/// functionality.
/// `Hash` implements Deref<Target = Tree> such that a Hash acts like a Tree.
#[derive(Debug, Clone)]
pub struct Hash {
  tree: Tree,
}

impl Hash {
  /// Open a hash with the provided key.
  pub fn open(db: &Db, key: &str) -> Result<Self> {
    let tree = db.open_tree(key)?;
    Ok(Self { tree })
  }

  /// A sugar function to insert many values using a sled `Batch`.
  /// Values are a slice of `(key, value)` pairs.
  pub fn insert_many<K, V>(&self, values: &[(K, V)]) -> Result<()>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    let mut batch = Batch::default();
    for (key, value) in values {
      batch.insert(key.as_ref(), value.as_ref());
    }
    self.apply_batch(batch)
  }

  /// Increments the number stored at key by `incr`.
  /// If key does not exist the value is set to `incr` in a new one.
  /// The increment argument is signed, so both increment and decrement operations
  /// can be performed.
  ///
  /// Returns the value after increment, or `Error::Unsupported` if the key holds
  /// a non `i64` value.
  ///
  /// This function never panics since wrapping (modular) additions and substractions are computed.
  pub fn incr_by<K: AsRef<[u8]>>(&self, key: K, incr: i64) -> Result<IVec> {
    use std::convert::TryInto;

    let mut error = false;
    let ivec = self
      .tree
      .update_and_fetch(key, |old| {
        if let Some(val) = old {
          if let Ok(arr) = val.as_ref().try_into() {
            // A valid integer is converted an incremented
            let incremented = i64::from_be_bytes(arr).wrapping_add(incr);
            Some(incremented.to_be_bytes().to_vec())
          } else {
            // If it is not a valid integer, return the same old value
            error = true;
            Some(val.to_vec())
          }
        } else {
          // If key is empty, initialize with incr
          Some(incr.to_be_bytes().to_vec())
        }
      })?
      .unwrap();

    if error {
      Err(sled::Error::Unsupported(
        "Hold value is not an integer".to_string(),
      ))
    } else {
      Ok(ivec)
    }
  }
}

impl Deref for Hash {
  type Target = Tree;

  fn deref(&self) -> &Tree {
    &self.tree
  }
}

#[cfg(test)]
mod test {
  use super::*;

  #[test]
  fn hash_deref() {
    let db = sled::Config::new().temporary(true).open().unwrap();
    let hash = Hash::open(&db, "my_hash").unwrap();

    hash.insert("A", "aa").unwrap();
  }

  #[test]
  fn hash_insert_many() {
    let db = sled::Config::new().temporary(true).open().unwrap();
    let hash = Hash::open(&db, "my_hash").unwrap();
    hash.insert_many(&[("A", "aa"), ("B", "bb"), ("C", "cc")]).unwrap();

    assert_eq!(Some(IVec::from("aa")), hash.get("A").unwrap());
    assert_eq!(Some(IVec::from("bb")), hash.get("B").unwrap());
    assert_eq!(Some(IVec::from("cc")), hash.get("C").unwrap());
  }

  #[test]
  fn hash_incr_by() {
    let db = sled::Config::new().temporary(true).open().unwrap();
    let hash = Hash::open(&db, "my_hash").unwrap();

    hash.insert("A", "aa").unwrap();
    // Failed to increment str
    assert!(hash.incr_by("A", 1).is_err());
    // Old value is preserved
    assert_eq!(IVec::from("aa"), hash.get("A").unwrap().unwrap());

    // Valid increments
    let incr = hash.incr_by("incr", 4).unwrap();
    assert_eq!(IVec::from(&4_i64.to_be_bytes()), incr);
    let incr = hash.incr_by("incr", -2).unwrap();
    assert_eq!(IVec::from(&2_i64.to_be_bytes()), incr);
  }
}
