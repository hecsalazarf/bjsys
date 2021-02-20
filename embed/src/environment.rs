use crate::collections::SortedSet;
use lmdb::{sys, Database, Environment, Result, RwTransaction, Transaction};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{Semaphore, SemaphorePermit};

/// A thread-safe LMDB environment protected by an `Arc`.
///
/// An environment supports multiple databases, all residing in the same
/// shared-memory map. It can be cloned and sent across threads; providing an async
/// version to begin a read-write transaction.
#[derive(Debug, Clone)]
pub struct Env {
  inner: Arc<EnvInner>,
}

impl Env {
  pub(crate) fn open(environment: Environment) -> Result<Self> {
    let txn_storage = TxnStorage::open(&environment)?;
    Ok(Self {
      inner: Arc::new(EnvInner::new(environment, txn_storage)),
    })
  }

  /// Asynchronously creates a read-write transaction for use with the environment. This method
  /// will yield while there are any other read-write transaction open on the environment.
  ///
  /// # Note
  /// Only for single process access. Multi-process access will still block the thread if there
  /// is an active read-write transaction.
  pub async fn begin_rw_txn_async<'env>(&'env self) -> Result<RwTxn<'env>> {
    self.inner.begin_rw_txn(None).await
  }

  /// Creates an idempotent read-write transaction, which can be applied multiple times without
  /// changing the result beyond the initial application. If subsequent transaction are begun
  /// with the same `id`, the transaction will be aborted on commit.
  ///
  /// To check if the transaction was previously committed, use `RwTxn::recover`.
  pub async fn begin_idemp_txn<'env>(&'env self, id: u128) -> Result<RwTxn<'env>> {
    self.inner.begin_rw_txn(Some(id)).await
  }
}

impl Deref for Env {
  type Target = Environment;

  fn deref(&self) -> &Self::Target {
    &self.inner.environment
  }
}

#[derive(Debug)]
struct EnvInner {
  environment: Environment,
  semaphore: Semaphore,
  txn_store: TxnStorage,
}

impl EnvInner {
  fn new(environment: Environment, txn_store: TxnStorage) -> Self {
    Self {
      environment,
      semaphore: Semaphore::new(1),
      txn_store,
    }
  }

  async fn begin_rw_txn<'env>(&'env self, id: Option<u128>) -> Result<RwTxn<'env>> {
    let permit = self.semaphore.acquire().await.expect("acquire permit");
    let txn = self.environment.begin_rw_txn()?;
    let store = if id.is_some() {
      Some(&self.txn_store)
    } else {
      None
    };

    Ok(RwTxn {
      txn,
      permit,
      id,
      store,
    })
  }
}

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
struct TxnStorage {
  ops: Database,
  ops_set: SortedSet,
}

impl TxnStorage {
  /// Opens the transaction storage attached to the environament.
  fn open(env: &Environment) -> Result<Self> {
    use crate::collections::SortedSetDb;
    use lmdb::DatabaseFlags;

    let (skiplist, elements) = SortedSetDb::create_dbs(env, Some("ops"))?;
    let ops_set = SortedSet::new(skiplist, elements, None);
    let ops = env.create_db(Some("__ops"), DatabaseFlags::default())?;

    Ok(Self { ops, ops_set })
  }

  /// Inserts a new idempotent transaction with the provided data. If the transaction
  /// exists, then it is overwritten.
  fn insert<D: ?Sized>(&self, txn: &mut RwTransaction, id: u128, data: &D) -> Result<()>
  where
    D: serde::Serialize,
  {
    use crate::extension::TransactionRwExt;

    let key = id.to_be_bytes();
    self.ops_set.add(txn, now_as_millis(), &key)?;
    txn.put_data(self.ops, &key, data, lmdb::WriteFlags::default())
  }

  /// Retrieves the response of the transaction `id`.
  fn retrieve<'env, T, D>(&self, txn: &'env T, id: u128) -> Result<Option<D>>
  where
    D: serde::Deserialize<'env>,
    T: Transaction,
  {
    use crate::extension::TransactionExt;
    txn.get_data(self.ops, id.to_be_bytes())
  }

  /// Returns `true` if the transacion exists for the specified `id`.
  fn contains<'env, T>(&self, txn: &'env T, id: u128) -> Result<bool>
  where
    T: Transaction,
  {
    use crate::extension::TransactionExt;
    Ok(txn.get_opt(self.ops, id.to_be_bytes())?.is_some())
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::{create_env, utf8_to_str};
  use lmdb::{DatabaseFlags, WriteFlags};

  #[tokio::test]
  async fn no_tls() -> Result<()> {
    use lmdb::Transaction;
    use std::time::Duration;
    // THIS TEST MUST FAIL WHEN NO_TLS IS DISABLED FOR THE ENVIRONMENT
    // See https://github.com/hecsalazarf/bjsys/issues/52 for more info.

    // create_env() automatically inserts NO_TLS
    let (_tmpdir, raw_env) = create_env()?;
    let env = Env::open(raw_env)?;

    let db = env.create_db(Some("hello"), DatabaseFlags::default())?;
    let mut txw = env.begin_rw_txn_async().await?;
    txw.put(db, &"hello", b"world", WriteFlags::default())?;
    txw.commit()?;

    let env2 = env.clone();
    let handler = tokio::spawn(async move {
      tokio::time::sleep(Duration::from_millis(100)).await;
      let tx2 = env2.begin_ro_txn().expect("ro txn");
      assert_eq!(Ok(&b"world"[..]), tx2.get(db, &"hello"));
      tokio::time::sleep(Duration::from_millis(300)).await;
    });

    let mut txw = env.begin_rw_txn_async().await?;
    txw.put(db, &"hello", b"friend", WriteFlags::default())?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    txw.commit()?;
    // If NO_TLS disabled, here the transaction will fail since the spawned tokio
    // task runs on the same OS thread, and such task still locks the table slot.
    let tx1 = env.begin_ro_txn()?;
    assert_eq!(Ok(&b"friend"[..]), tx1.get(db, &"hello"));
    handler.await.unwrap();
    Ok(())
  }

  #[tokio::test]
  async fn rw_txn_async() -> Result<()> {
    let (_tmpdir, raw_env) = create_env()?;
    let env_1 = Env::open(raw_env)?;
    let db = env_1.create_db(Some("test_db"), DatabaseFlags::empty())?;
    let env_2 = env_1.clone();

    let join = tokio::spawn(async move {
      let mut txn = env_2.begin_rw_txn_async().await.unwrap();
      txn.put(db, &"A", &"letter A", WriteFlags::empty()).unwrap();
      txn.commit().unwrap();
    });

    let mut txn = env_1.begin_rw_txn_async().await?;
    txn.put(db, &"B", &"letter B", WriteFlags::empty())?;
    txn.commit()?;
    join.await.unwrap();

    let txn = env_1.begin_ro_txn()?;
    assert_eq!(Ok("letter A"), utf8_to_str(txn.get(db, &"A")));
    assert_eq!(Ok("letter B"), utf8_to_str(txn.get(db, &"B")));
    Ok(())
  }

  #[tokio::test]
  async fn create_idemp_txn() -> Result<()> {
    let (_tmpdir, raw_env) = create_env()?;
    let env_1 = Env::open(raw_env)?;
    let db = env_1.create_db(Some("test_db"), DatabaseFlags::empty())?;

    let mut txn = env_1.begin_idemp_txn(1).await?;
    assert_eq!(Ok(None), txn.recover::<()>());
    txn.put(db, &"B", &"letter B", WriteFlags::empty())?;
    txn.commit_with("B was saved")?;

    let mut txn = env_1.begin_idemp_txn(1).await?;
    assert_eq!(Ok(Some("B was saved")), txn.recover());
    txn.put(db, &"B", &"another letter B", WriteFlags::empty())?;
    txn.commit()?;

    let txn = env_1.begin_idemp_txn(1).await?;
    assert_eq!(Ok("letter B"), utf8_to_str(txn.get(db, &"B")));
    txn.abort();
    Ok(())
  }
}
