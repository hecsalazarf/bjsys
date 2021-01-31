use crate::extension::{TransactionExt, TransactionRwExt};
use lmdb::{
  Cursor, Database, Environment, Error, Iter, Result, RwTransaction, Transaction, WriteFlags,
};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// A typed key-value LMDB database.
#[derive(Copy, Clone, Debug)]
pub struct Store<D> {
  /// Inner database
  database: Database,
  _data: PhantomData<D>,
}

impl<D> Store<D> {
  /// Open a store with the provided database name.
  /// 
  /// # Note
  /// Stores are not meant to be opened more than once, since there is no way
  /// (yet) to know if a store was previously operated on a different data type.
  /// Instead, copy the opened store whenever it's neded.
  pub fn open<K>(env: &Environment, db: K) -> Result<Self>
  where
    K: AsRef<str>,
  {
    let db_flags = lmdb::DatabaseFlags::default();
    let database = env.create_db(Some(db.as_ref()), db_flags)?;

    Ok(Self {
      database,
      _data: PhantomData,
    })
  }

  /// Insert data with key. Data can be any type that implements `serde:Serialize`.
  pub fn put<K>(&self, txn: &mut RwTransaction, key: K, data: &D) -> Result<()>
  where
    K: AsRef<[u8]>,
    D: Serialize,
  {
    txn.put_data(self.database, &key.as_ref(), &data, WriteFlags::default())
  }

  /// Retrieve data from `Store` if keys exists.
  ///
  /// # Note
  /// This method returns `Error::Incompatible` if the stored value has a different type
  /// from the one that this method tried to deserialize.
  pub fn get<'txn, K, T>(&self, txn: &'txn T, key: K) -> Result<Option<D>>
  where
    K: AsRef<[u8]>,
    D: Deserialize<'txn>,
    T: Transaction,
  {
    txn.get_data(self.database, &key.as_ref())
  }

  /// Remove key from the database.
  pub fn delete<K>(&self, txn: &mut RwTransaction, key: K) -> Result<()>
  where
    K: AsRef<[u8]>,
  {
    txn.del(self.database, &key.as_ref(), None)
  }

  /// Return an iterator positioned at first key greater than or equal to the specified key.
  pub fn iter_from<'txn, K, T>(&self, txn: &'txn T, key: K) -> Result<StoreIter<'txn, D>>
  where
    K: AsRef<[u8]>,
    T: Transaction,
  {
    let mut cursor = txn.open_ro_cursor(self.database)?;
    let iter = cursor.iter_from(key);
    Ok(StoreIter::new(iter))
  }
}

/// Iterator over key/data pairs of a store.
pub struct StoreIter<'txn, D> {
  inner: Iter<'txn>,
  _data: PhantomData<D>,
}

impl<'txn, D> StoreIter<'txn, D> {
  fn new(inner: Iter<'txn>) -> Self {
    Self {
      inner,
      _data: PhantomData,
    }
  }

  fn next_inner(&mut self) -> Option<Result<(&'txn [u8], D)>>
  where
    D: Deserialize<'txn>,
  {
    self.inner.next().and_then(|res| match res {
      Ok((key, val)) => {
        let v = bincode::deserialize(val);
        Some(v.map(|val| (key, val)).map_err(|_| Error::Incompatible))
      }
      Err(e) => Some(Err(e)),
    })
  }
}

impl<'txn, D: Deserialize<'txn>> Iterator for StoreIter<'txn, D> {
  type Item = Result<(&'txn [u8], D)>;

  fn next(&mut self) -> Option<Self::Item> {
    self.next_inner()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::create_env;

  #[test]
  fn put_get() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let store = Store::open(&env, "mystore")?;
    let mut tx = env.begin_rw_txn()?;
    store.put(&mut tx, "hello", &"world")?;
    assert_eq!(Ok(Some("world")), store.get(&mut tx, "hello"));

    Ok(())
  }

  #[test]
  fn iter_from() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let store = Store::open(&env, "mystore")?;
    let mut tx = env.begin_rw_txn()?;
    store.put(&mut tx, [10], &10)?;
    store.put(&mut tx, [100], &100)?;
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    let mut iter = store.iter_from(&tx, [0])?;
    assert_eq!(Some(Ok((&[10][..], 10))), iter.next());
    assert_eq!(Some(Ok((&[100][..], 100))), iter.next());

    Ok(())
  }

  #[test]
  fn get_incompatible() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let store = Store::open(&env, "mystore")?;
    let mut tx = env.begin_rw_txn()?;
    store.put(&mut tx, "true", &true)?;
    tx.commit()?;

    let store = Store::<f64>::open(&env, "mystore")?;
    let tx = env.begin_ro_txn()?;
    // Err because we store a bool and we try to get a f64
    assert_eq!(Err(Error::Incompatible), store.get(&tx, "true"));
    Ok(())
  }
}
