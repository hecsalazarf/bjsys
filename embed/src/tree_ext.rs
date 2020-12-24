//! An extension trait which adds utility methods to Trees.
use sled::transaction::TransactionalTree;
use sled::{Batch, Error, IVec, Tree};

/// An extension trait which adds utility methods to Trees.
pub trait TreeExt {
  /// Error returned by the implementing type of TreeExt
  type Err: std::error::Error;
  /// A sugar function to insert many values using a sled `Batch`.
  /// Values are a slice of `(key, value)` pairs.
  fn insert_many<K, V>(&self, values: &[(K, V)]) -> Result<(), Self::Err>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>;

  /// Increments the number stored at key by `incr`.
  /// If key does not exist the value is set to `incr` in a new one.
  /// The increment argument is signed, so both increment and decrement operations
  /// can be performed.
  ///
  /// Returns the value after increment, or `Error::Unsupported` if the key holds
  /// a non `i64` value.
  ///
  /// This function never panics since wrapping (modular) additions and substractions are computed.
  fn incr_by<K: AsRef<[u8]>>(&self, key: K, incr: i64) -> Result<IVec, Self::Err>;
}

impl TreeExt for Tree {
  type Err = Error;

  fn insert_many<K, V>(&self, values: &[(K, V)]) -> Result<(), Self::Err>
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

  fn incr_by<K: AsRef<[u8]>>(&self, key: K, incr: i64) -> Result<IVec, Self::Err> {
    let mut error = None;
    let ivec = self
      .update_and_fetch(key, |old| match incr_slice(old, incr) {
        Ok(val) => Some(val.to_vec()),
        Err(e) => {
          error = Some(e);
          old.map(|o| o.to_vec())
        }
      })?
      // Never panics as this update_and_fetch() always returns a value
      .unwrap();

    if let Some(e) = error {
      Err(e)
    } else {
      Ok(ivec)
    }
  }
}

impl TreeExt for TransactionalTree {
  type Err = sled::transaction::ConflictableTransactionError;

  fn insert_many<K, V>(&self, values: &[(K, V)]) -> Result<(), Self::Err>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    let mut batch = Batch::default();
    for (key, value) in values {
      batch.insert(key.as_ref(), value.as_ref());
    }
    self.apply_batch(&batch)?;
    Ok(())
  }

  fn incr_by<K: AsRef<[u8]>>(&self, key: K, incr: i64) -> Result<IVec, Self::Err> {
    let opt_val = self.get(&key)?;
    let opt_val = opt_val.as_ref().map(|i| i.as_ref());
    let incremented = incr_slice(opt_val, incr)?;
    self.insert(key.as_ref(), &incremented)?;
    Ok(self.get(&key)?.unwrap())
  }
}

/// Auxiliar function that increments a &[u8] by `incr`. Returns error if `slice`
/// is not a valid `i64`.
fn incr_slice(slice: Option<&[u8]>, incr: i64) -> Result<[u8; 8], Error> {
  use std::convert::TryInto;

  if let Some(val) = slice {
    let arr = val
      .try_into()
      .map_err(|_| Error::Unsupported("Failed to incr IVec".to_string()))?;
    let new_incr = i64::from_be_bytes(arr).wrapping_add(incr);
    Ok(new_incr.to_be_bytes())
  } else {
    // If key is empty, initialize with incr
    Ok(incr.to_be_bytes())
  }
}

#[cfg(test)]
mod test {
  use super::*;

  #[test]
  fn tree_ext_insert_many() {
    let db = sled::Config::new().temporary(true).open().unwrap();
    let tree = db.open_tree("my_hash").unwrap();
    tree
      .insert_many(&[("A", "aa"), ("B", "bb"), ("C", "cc")])
      .unwrap();

    assert_eq!(Some(IVec::from("aa")), tree.get("A").unwrap());
    assert_eq!(Some(IVec::from("bb")), tree.get("B").unwrap());
    assert_eq!(Some(IVec::from("cc")), tree.get("C").unwrap());
  }

  #[test]
  fn tree_ext_incr_by() {
    let db = sled::Config::new().temporary(true).open().unwrap();
    let tree = db.open_tree("my_hash").unwrap();

    tree.insert("A", "aa").unwrap();
    // Failed to increment str
    assert!(tree.incr_by("A", 1).is_err());
    // Old value is preserved
    assert_eq!(IVec::from("aa"), tree.get("A").unwrap().unwrap());

    // Valid increments
    let incr = tree.incr_by("incr", 4).unwrap();
    assert_eq!(IVec::from(&4_i64.to_be_bytes()), incr);
    let incr = tree.incr_by("incr", -2).unwrap();
    assert_eq!(IVec::from(&2_i64.to_be_bytes()), incr);
  }
}
