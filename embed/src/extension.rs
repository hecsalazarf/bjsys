//! Automatically-implemented extension traits on `Transaction` and `Cursor`.
use lmdb::{Cursor, Database, Error, Result, Transaction};

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

impl<T: Transaction + ?Sized> TransactionExt for T {}

type PairCursor<'txn> = (Option<&'txn [u8]>, Option<&'txn [u8]>);

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

impl<'txn, C: Cursor<'txn> + ?Sized> CursorExt<'txn> for C {}
