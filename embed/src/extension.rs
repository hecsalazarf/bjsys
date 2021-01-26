//! Automatically-implemented extension traits on `Transaction` and `Cursor`.
use lmdb::{Cursor, Database, Error, Result, RwTransaction, Transaction, WriteFlags};

/// An automatically-implemented extension trait on `Transaction` providing
/// convenience methods.
pub trait TransactionExt: Transaction {
  /// Gets an item from a database, but instead of returning `Error::NotFound`, returns
  /// `None` if the item is not in the database.
  fn get_opt<K>(&self, db: Database, key: K) -> Result<Option<&[u8]>>
  where
    K: AsRef<[u8]>,
  {
    self.get(db, &key.as_ref()).map_or_else(
      |e| {
        if e == Error::NotFound {
          Ok(None)
        } else {
          Err(e)
        }
      },
      |v| Ok(Some(v)),
    )
  }
}

/// Extension trait for `RwTransaction`, providing convenience methods.
pub trait TransactionRwExt: TransactionExt {
  /// Increments the `u64` number stored at `key` by `incr`. If key does not exist, a new key holding
  /// the value is set to `incr`.
  /// Returns a tuple of the value at key after the increment with a boolean indicating whether an
  /// arithmetic overflow occured. If an overflow have occurred then the wrapped value is returned.
  /// Fails with `Error::Incompatible` if the value before incrementing is not a valid `u64` number.
  fn incr_by<K>(&mut self, db: Database, key: K, incr: u64, wf: WriteFlags) -> Result<(u64, bool)>
  where
    K: AsRef<[u8]>;
}

/// An automatically-implemented extension trait on `Cursor` providing
/// convenience methods.
pub trait CursorExt<'txn>: Cursor<'txn> {
  /// Retrieves a key/data pair from the cursor just as `Cursor::get`, but instead of returning
  /// `Error::NotFound`, returns `(None, None)` if the item is not in the database.
  fn get_opt(&'txn self, key: Option<&[u8]>, data: Option<&[u8]>, op: u32) -> Result<PairCursor> {
    self.get(key, data, op).map_or_else(
      |e| {
        if e == Error::NotFound {
          Ok((None, None))
        } else {
          Err(e)
        }
      },
      |(k, v)| Ok((k, Some(v))),
    )
  }
}

type PairCursor<'txn> = (Option<&'txn [u8]>, Option<&'txn [u8]>);

impl<'txn, C: Cursor<'txn> + ?Sized> CursorExt<'txn> for C {}

impl<T: Transaction + ?Sized> TransactionExt for T {}

impl TransactionRwExt for RwTransaction<'_> {
  fn incr_by<K>(&mut self, db: Database, key: K, incr: u64, wf: WriteFlags) -> Result<(u64, bool)>
  where
    K: AsRef<[u8]>,
  {
    use std::convert::TryInto;
    if let Some(val) = self.get_opt(db, key.as_ref())? {
      let val_arr = val.try_into().map_err(|_| Error::Incompatible)?;
      let res = u64::from_be_bytes(val_arr).overflowing_add(incr);
      self.put(db, &key.as_ref(), &res.0.to_be_bytes(), wf)?;
      return Ok(res)
    }

    self.put(db, &key.as_ref(), &incr.to_be_bytes(), wf)?;
    Ok((incr, false))
  }
}
