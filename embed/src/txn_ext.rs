use lmdb::{Transaction, Database, Result, Error, RwTransaction, RoTransaction};

/// Extension trait to add functionality to LMDB transactions
pub trait TransactionExt: Transaction {
  
  /// Gets an item from a database, but instead of returning `Error::NotFound`, returns
  /// `None` if the item is not in the database.
  fn get<K>(&self, db: Database, key: K) -> Result<Option<&[u8]>>
  where
    K: AsRef<[u8]>,
  {
    Transaction::get(self, db, &key.as_ref()).map_or_else(
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
