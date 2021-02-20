use crate::collections::SortedSet;
use lmdb::{sys, Database, Error, Result, RwTransaction, Transaction, WriteFlags};
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};
use tokio::sync::SemaphorePermit;

/// An LMDB read-write transaction that is created asynchronously and can be
/// idempotent.
///
/// `RwTxn` implements `Deref<Target = RwTransaction>`.
#[derive(Debug)]
pub struct RwTxn<'env> {
  permit: SemaphorePermit<'env>,
  txn: RwTransaction<'env>,
  id: Option<u128>,
  store: Option<&'env TxnStorage>,
}

impl<'env> Transaction for RwTxn<'env> {
  fn txn(&self) -> *mut sys::MDB_txn {
    self.txn.txn()
  }

  fn commit(self) -> Result<()> {
    self.commit_with(&())
  }

  fn abort(self) {
    // Explicitly abort the transaction so that the permit destructor
    // is invoked and no deadlocks occur.
    self.txn.abort()
  }
}

impl<'env> Deref for RwTxn<'env> {
  type Target = RwTransaction<'env>;

  fn deref(&self) -> &Self::Target {
    &self.txn
  }
}

impl<'env> DerefMut for RwTxn<'env> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.txn
  }
}

impl<'env> RwTxn<'env> {
  pub(crate) const fn new(
    permit: SemaphorePermit<'env>,
    txn: RwTransaction<'env>,
    id: Option<u128>,
    store: Option<&'env TxnStorage>,
  ) -> Self {
    Self {
      txn,
      permit,
      id,
      store,
    }
  }
  /// Gets the result of an idempotent transaction. The result can be `None`
  /// if either the transaction is not idempotent or the transaction is
  /// idempotent but has not been commited earlier.
  pub fn recover<D>(&'env self) -> Result<Option<D>>
  where
    D: serde::Deserialize<'env>,
  {
    if let Some(id) = self.id {
      return self.store.unwrap().retrieve(&self.txn, id);
    }

    Ok(None)
  }

  /// Commits an idempotent transaction, storing `val` as the result of the
  /// operation. If the transaction was previously commited, the operation
  /// aborts.
  ///
  /// # Note
  /// If the trasaction is not idempotent, `val` is ignored and the operation
  /// is committed as usual.
  pub fn commit_with<D: ?Sized>(self, val: &D) -> Result<()>
  where
    D: serde::Serialize,
  {
    let mut txn = self.txn;
    if let Some(id) = self.id {
      let store = self.store.unwrap();
      if store.contains(&txn, id)? {
        return Ok(());
      }
      store.insert(&mut txn, id, val)?;
    }

    txn.commit()
  }
}

/// Storage for idempotent transactions. It provides functions to store, retrieve
/// and delete idempotent transactions.
#[derive(Debug)]
pub struct TxnStorage {
  ops: Database,
  ops_set: SortedSet,
}

impl TxnStorage {
  /// Opens the transaction storage attached to the environament.
  pub fn open(env: &lmdb::Environment) -> Result<Self> {
    use crate::collections::SortedSetDb;
    use lmdb::DatabaseFlags;

    let (skiplist, elements) = SortedSetDb::create_dbs(env, Some("ops"))?;
    let ops_set = SortedSet::new(skiplist, elements, None);
    let ops = env.create_db(Some("__ops"), DatabaseFlags::default())?;

    Ok(Self { ops, ops_set })
  }

  /// Inserts a new idempotent transaction with the provided data. If the transaction
  /// exists, then it is overwritten.
  pub fn insert<D: ?Sized>(&self, txn: &mut RwTransaction, id: u128, data: &D) -> Result<()>
  where
    D: Serialize,
  {
    let key = id.to_be_bytes();
    self.ops_set.add(txn, now_as_millis(), &key)?;
    txn.put_data(self.ops, &key, data, WriteFlags::default())
  }

  /// Retrieves the response of the transaction `id`.
  pub fn retrieve<'env, T, D>(&self, txn: &'env T, id: u128) -> Result<Option<D>>
  where
    D: Deserialize<'env>,
    T: Transaction,
  {
    txn.get_data(self.ops, id.to_be_bytes())
  }

  /// Returns `true` if the transacion exists for the specified `id`.
  pub fn contains<'env, T>(&self, txn: &'env T, id: u128) -> Result<bool>
  where
    T: Transaction,
  {
    Ok(txn.get_opt(self.ops, id.to_be_bytes())?.is_some())
  }
}

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

  /// Gets a typed-item from a database, returns `None` if the item is not in the database.
  fn get_data<'txn, K, D>(&'txn self, db: Database, key: K) -> Result<Option<D>>
  where
    K: AsRef<[u8]>,
    D: Deserialize<'txn>,
  {
    if let Some(d) = self.get_opt(db, key)? {
      let data = bincode::deserialize(d).map_err(|_| Error::Incompatible)?;
      return Ok(Some(data));
    }

    Ok(None)
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

  /// Stores an item  of type `T` into database
  fn put_data<K, D: ?Sized>(
    &mut self,
    db: Database,
    key: K,
    data: &D,
    wf: WriteFlags,
  ) -> Result<()>
  where
    K: AsRef<[u8]>,
    D: Serialize;
}

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
      return Ok(res);
    }

    self.put(db, &key.as_ref(), &incr.to_be_bytes(), wf)?;
    Ok((incr, false))
  }

  fn put_data<K, D: ?Sized>(&mut self, db: Database, key: K, data: &D, wf: WriteFlags) -> Result<()>
  where
    K: AsRef<[u8]>,
    D: Serialize,
  {
    let data = bincode::serialize(data).map_err(|_| Error::Incompatible)?;
    self.put(db, &key.as_ref(), &data, wf)
  }
}

/// Return the current time in milliseconds as u64
fn now_as_millis() -> u64 {
  use std::time::SystemTime;

  SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64
}
