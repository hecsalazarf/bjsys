//! A queue with persitent storage.
use crate::extension::{TransactionExt, TransactionRwExt};
use lmdb::{Cursor, Database, Environment, Error, Iter, Result, RwTransaction, WriteFlags};
use uuid::Uuid;

type MetaKey = [u8; 17];
type ElementKey = [u8; 24];

#[derive(Clone, Debug)]
struct QueueKeys {
  /// Queue's tail.
  tail: MetaKey,
}

impl QueueKeys {
  const TAIL_SUFFIX: &'static [u8] = &[0];

  fn new(uuid: &Uuid) -> Self {
    let uuid_bytes = uuid.as_bytes();
    let tail = Self::create_key(uuid_bytes, Self::TAIL_SUFFIX);

    Self { tail }
  }

  fn create_key(uuid: &[u8; 16], suffix: &[u8]) -> MetaKey {
    let mut key = MetaKey::default();
    let head_chain = uuid.iter().chain(suffix);
    key
      .iter_mut()
      .zip(head_chain)
      .for_each(|(new, chained)| *new = *chained);

    key
  }
}

/// A queue with persitent storage.
///
/// It is a collection of elements sorted according to the order of insertion, also
/// know as linked list. Call [`push`] to add elements to the tail, and [`pop`] to
/// remove from the head.
///
/// [`push`]: Queue::push
/// [`pop`]: Queue::pop
#[derive(Clone, Debug)]
pub struct Queue {
  /// Grouped keys for meta retrieval
  keys: QueueKeys,
  /// Elements (members)
  elements: Database,
  /// Metadata DB, containing the head and tail values
  meta: Database,
  /// UUID
  uuid: Uuid,
}

impl Queue {
  /// Meta DB name.
  const META_DB_NAME: &'static str = "__queue_meta";
  /// Elements DB name.
  const ELEMENTS_DB_NAME: &'static str = "__queue_elements";

  /// Open a queue with the provided key.
  pub fn open<K>(env: &Environment, key: K) -> Result<Self>
  where
    K: AsRef<[u8]>,
  {
    let db_flags = lmdb::DatabaseFlags::default();
    let elements = env.create_db(Some(Self::ELEMENTS_DB_NAME), db_flags)?;
    let meta = env.create_db(Some(Self::META_DB_NAME), db_flags)?;
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_ref());
    let keys = QueueKeys::new(&uuid);

    Ok(Self {
      elements,
      meta,
      uuid,
      keys,
    })
  }

  /// Preppend one element to the queue's tail.
  pub fn push<V: AsRef<[u8]>>(&self, txn: &mut RwTransaction, val: V) -> Result<()> {
    let write_flags = WriteFlags::default();

    let (next_tail, _) = txn.incr_by(self.meta, self.keys.tail, 1, write_flags)?;
    let encoded_key = self.encode_members_key(&next_tail.to_be_bytes());
    if txn.get_opt(self.elements, encoded_key)?.is_some() {
      // Error if full, transaction must abort
      return Err(Error::KeyExist);
    }

    txn.put(self.elements, &encoded_key, &val, write_flags)
  }

  /// Pop one element from the queue's head. Return `None` if queue is empty.
  pub fn pop<'txn>(&self, txn: &'txn mut RwTransaction) -> Result<Option<&'txn [u8]>> {
    let mut cursor = txn.open_rw_cursor(self.elements)?;
    let iter = cursor.iter_from(self.uuid.as_bytes());
    let opt_pop = iter.take(1).next().transpose()?;

    if opt_pop.is_some() {
      let write_flags = WriteFlags::default();
      cursor.del(write_flags)?;
    }

    Ok(opt_pop.map(|(_, val)| val))
  }

  /// Remove the first `count` elements equal to `val` from the queue.
  pub fn remove<V>(&self, txn: &mut RwTransaction, count: usize, val: V) -> Result<usize>
  where
    V: AsRef<[u8]>,
  {
    let mut iter = self.iter(txn)?;
    let val = val.as_ref();
    let mut removed = 0;

    while let Some((key, value)) = iter.next_inner().transpose()? {
      if val == value {
        txn.del(self.elements, &key, None)?;
        removed += 1;
        if removed == count {
          break;
        }
      }
    }

    Ok(removed)
  }

  /// Create an iterator over the elements of the queue.
  pub fn iter<'txn, T>(&self, txn: &'txn T) -> Result<QueueIter<'txn>>
  where
    T: lmdb::Transaction,
  {
    let mut cursor = txn.open_ro_cursor(self.elements)?;

    Ok(QueueIter {
      inner: cursor.iter_from(self.uuid.as_bytes()),
      uuid: self.uuid,
    })
  }

  fn encode_members_key(&self, index: &[u8]) -> ElementKey {
    let chain = self.uuid.as_bytes().iter().chain(index);
    let mut key = ElementKey::default();

    key
      .iter_mut()
      .zip(chain)
      .for_each(|(new, chained)| *new = *chained);
    key
  }
}

/// Iterator on queue's elements.
pub struct QueueIter<'txn> {
  inner: Iter<'txn>,
  uuid: Uuid,
}

impl<'txn> QueueIter<'txn> {
  fn next_inner(&mut self) -> Option<Result<(&'txn [u8], &'txn [u8])>> {
    let next = self.inner.next();
    if let Some(Ok((key, _))) = next {
      // Extract the uuid and compare it, so that we know we are still
      // on the same queue
      let uuid_bytes = self.uuid.as_bytes();
      if &key[..uuid_bytes.len()] == uuid_bytes {
        next
      } else {
        // Different uuid, iterator is over
        None
      }
    } else {
      next
    }
  }
}

impl<'txn> Iterator for QueueIter<'txn> {
  type Item = Result<&'txn [u8]>;

  fn next(&mut self) -> Option<Self::Item> {
    self.next_inner().map(|res| res.map(|(_, val)| val))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::{create_env, utf8_to_str};
  use lmdb::Transaction;

  #[test]
  fn push() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue = Queue::open(&env, "myqueue")?;
    let mut tx = env.begin_rw_txn()?;
    queue.push(&mut tx, "Y")?;
    queue.push(&mut tx, "Z")?;
    queue.push(&mut tx, "X")?;
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    let mut iter = queue.iter(&tx)?;
    assert_eq!(Some(Ok("Y")), iter.next().map(utf8_to_str));
    assert_eq!(Some(Ok("Z")), iter.next().map(utf8_to_str));
    assert_eq!(Some(Ok("X")), iter.next().map(utf8_to_str));
    assert_eq!(None, iter.next().map(utf8_to_str));
    tx.commit()?;

    let queue2 = Queue::open(&env, "anotherqueue")?;
    let mut tx = env.begin_rw_txn()?;
    queue2.push(&mut tx, "A")?;
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    let mut iter = queue2.iter(&tx)?;
    assert_eq!(Some(Ok("A")), iter.next().map(utf8_to_str));
    assert_eq!(None, iter.next().map(utf8_to_str));
    tx.commit()
  }

  #[test]
  fn push_full_queue() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue = Queue::open(&env, "myqueue")?;
    let mut tx = env.begin_rw_txn()?;
    queue.push(&mut tx, "X")?;
    // Increment the index to set it back to zero
    tx.incr_by(
      queue.meta,
      queue.keys.tail,
      u64::MAX - 1,
      WriteFlags::default(),
    )?;
    tx.commit()?;

    let mut tx = env.begin_rw_txn()?;
    // Ok because zero position is empty
    assert_eq!(Ok(()), queue.push(&mut tx, "Y"));
    // Err because the first push appended a value at index 1
    assert_eq!(Err(Error::KeyExist), queue.push(&mut tx, "Z"));
    Ok(())
  }

  #[test]
  fn pop() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue = Queue::open(&env, "myqueue")?;
    let mut tx = env.begin_rw_txn()?;
    queue.push(&mut tx, "Y")?;
    queue.push(&mut tx, "Z")?;
    tx.commit()?;

    let queue2 = Queue::open(&env, "anotherqueue")?;
    let mut tx = env.begin_rw_txn()?;
    queue2.push(&mut tx, "A")?;
    tx.commit()?;

    let mut tx = env.begin_rw_txn()?;
    let opt_pop = queue.pop(&mut tx).transpose();
    assert_eq!(Some(Ok("Y")), opt_pop.map(utf8_to_str));
    tx.commit()?;

    let mut tx = env.begin_rw_txn()?;
    let opt_pop = queue.pop(&mut tx).transpose();
    assert_eq!(Some(Ok("Z")), opt_pop.map(utf8_to_str));
    tx.commit()?;

    let mut tx = env.begin_rw_txn()?;
    let opt_pop = queue.pop(&mut tx).transpose();
    assert_eq!(None, opt_pop.map(utf8_to_str));
    tx.commit()
  }

  #[test]
  fn remove() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue = Queue::open(&env, "myqueue")?;
    let mut tx = env.begin_rw_txn()?;
    queue.push(&mut tx, "X")?;
    queue.push(&mut tx, "X")?;
    queue.push(&mut tx, "Y")?;
    tx.commit()?;

    let mut tx = env.begin_rw_txn()?;
    let removed = queue.remove(&mut tx, 2, "X")?;
    tx.commit()?;
    assert_eq!(2, removed);

    let tx = env.begin_ro_txn()?;
    let mut iter = queue.iter(&tx)?;
    assert_eq!(Some(Ok("Y")), iter.next().map(utf8_to_str));
    assert_eq!(None, iter.next());
    Ok(())
  }
}
