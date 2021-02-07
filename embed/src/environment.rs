use lmdb::{sys, Environment, Result, RwTransaction, Transaction};
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
  pub(crate) fn new(environment: Environment) -> Self {
    Self {
      inner: Arc::new(EnvInner::new(environment)),
    }
  }

  /// Asynchronously create a read-write transaction for use with the environment. This method
  /// will yield while there are any other read-write transactions open on the environment.
  pub async fn begin_rw_txn_async<'env>(&'env self) -> Result<RwTxn<'env>> {
    self.inner.begin_rw_txn_async().await
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
}

impl EnvInner {
  fn new(environment: Environment) -> Self {
    Self {
      environment,
      semaphore: Semaphore::new(1),
    }
  }

  async fn begin_rw_txn_async<'env>(&'env self) -> Result<RwTxn<'env>> {
    let permit = self.semaphore.acquire().await.expect("acquire permit");
    let txn = self.environment.begin_rw_txn()?;

    Ok(RwTxn { txn, permit })
  }
}

/// An LMDB read-write transaction that is created asynchronously.
///
/// `RwTxn` implements `Deref<Target = RwTransaction>`.
#[derive(Debug)]
pub struct RwTxn<'env> {
  permit: SemaphorePermit<'env>,
  txn: RwTransaction<'env>,
}

impl<'env> Transaction for RwTxn<'env> {
  fn txn(&self) -> *mut sys::MDB_txn {
    self.txn.txn()
  }

  fn commit(self) -> Result<()> {
    // Explicitly commit the transaction so that the permit destructor
    // is invoked and no deadlocks occur.
    self.txn.commit()
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::{create_env, utf8_to_str};
  use lmdb::{DatabaseFlags, WriteFlags};

  #[tokio::test]
  async fn rw_txn_async() -> Result<()> {
    let (_tmpdir, raw_env) = create_env()?;
    let env_1 = Env::new(raw_env);
    let db = env_1.create_db(Some("test_db"), DatabaseFlags::empty())?;
    let env_2 = env_1.clone();

    let join = tokio::spawn(async move {
      let mut tx = env_2.begin_rw_txn_async().await.unwrap();
      tx.put(db, &"A", &"letter A", WriteFlags::empty()).unwrap();
      tx.commit().unwrap();
    });

    let mut tx = env_1.begin_rw_txn_async().await?;
    tx.put(db, &"B", &"letter B", WriteFlags::empty())?;
    tx.commit()?;
    join.await.unwrap();

    let tx = env_1.begin_ro_txn()?;
    assert_eq!(Ok("letter A"), utf8_to_str(tx.get(db, &"A")));
    assert_eq!(Ok("letter B"), utf8_to_str(tx.get(db, &"B")));
    Ok(())
  }
}
