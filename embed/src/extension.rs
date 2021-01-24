//! Extension traits for LMDB transactions and cursors.
use lmdb::{
  Cursor, Database, Error, Result, RoCursor, RoTransaction, RwCursor, RwTransaction, Transaction,
};

/// Extension trait to add functionality to LMDB transactions.
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

impl TransactionExt for RoTransaction<'_> {}
impl TransactionExt for RwTransaction<'_> {}

type PairCursor<'txn> = (Option<&'txn [u8]>, Option<&'txn [u8]>);

/// Extension trait to add functionality to LMDB cursors
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

impl<'txn> CursorExt<'txn> for RoCursor<'txn> {}
impl<'txn> CursorExt<'txn> for RwCursor<'txn> {}
