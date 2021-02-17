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
  pub(crate) fn new(environment: Environment, ops: Database) -> Self {
    Self {
      inner: Arc::new(EnvInner::new(environment, ops)),
    }
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
  ops: Database,
}

impl EnvInner {
  fn new(environment: Environment, ops: Database) -> Self {
    Self {
      environment,
      semaphore: Semaphore::new(1),
      ops,
    }
  }

  async fn begin_rw_txn<'env>(&'env self, id: Option<u128>) -> Result<RwTxn<'env>> {
    let permit = self.semaphore.acquire().await.expect("acquire permit");
    let txn = self.environment.begin_rw_txn()?;
    let ops = if id.is_some() { Some(self.ops) } else { None };

    Ok(RwTxn {
      txn,
      permit,
      id,
      ops,
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
  ops: Option<Database>,
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
    use crate::extension::TransactionExt;
    if let Some(id) = self.id {
      return self.txn.get_data(self.ops.unwrap(), id.to_be_bytes());
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
    use crate::extension::{TransactionExt, TransactionRwExt};
    use lmdb::WriteFlags;

    let mut txn = self.txn;
    if let Some(id) = self.id {
      let key = id.to_be_bytes();
      let ops = self.ops.unwrap();

      if txn.get_opt(ops, &key)?.is_some() {
        return Ok(());
      }
      txn.put_data(ops, key, val, WriteFlags::default())?;
    }

    txn.commit()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::{create_env, utf8_to_str};
  use lmdb::{DatabaseFlags, WriteFlags};

  #[tokio::test]
  async fn rw_txn_async() -> Result<()> {
    let (_tmpdir, raw_env) = create_env()?;
    let ops_db = raw_env.create_db(Some("ops"), DatabaseFlags::default())?;
    let env_1 = Env::new(raw_env, ops_db);
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
  async fn rw_idemp_txn() -> Result<()> {
    let (_tmpdir, raw_env) = create_env()?;
    let ops_db = raw_env.create_db(Some("ops"), DatabaseFlags::default())?;
    let env_1 = Env::new(raw_env, ops_db);
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
