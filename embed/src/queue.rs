//! A queue with persitent storage.
use crate::extension::{TransactionExt, TransactionRwExt};
use lmdb::{
  Cursor, Database, Environment, Error, Iter, Result, RwTransaction, Transaction, WriteFlags,
};
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
  /// Preppend one element to the queue's tail.
  pub fn push<V: AsRef<[u8]>>(&self, txn: &mut RwTransaction, val: V) -> Result<()> {
    let mut txn = txn.begin_nested_txn()?;
    let write_flags = WriteFlags::default();

    let (next_tail, _) = txn.incr_by(self.meta, self.keys.tail, 1, write_flags)?;
    let encoded_key = self.encode_members_key(&next_tail.to_be_bytes());
    if txn.get_opt(self.elements, encoded_key)?.is_some() {
      // Error if full, transaction must abort
      return Err(Error::KeyExist);
    }

    txn.put(self.elements, &encoded_key, &val, write_flags)?;
    txn.commit()
  }

  /// Pop one element from the queue's head. Return `None` if queue is empty.
  pub fn pop<'txn>(&self, txn: &'txn mut RwTransaction) -> Result<Option<&'txn [u8]>> {
    let mut txn = txn.begin_nested_txn()?;
    let mut cursor = txn.open_rw_cursor(self.elements)?;
    let iter = cursor.iter_from(self.uuid.as_bytes());
    let opt_pop = iter.take(1).next().transpose()?;

    if opt_pop.is_some() {
      let write_flags = WriteFlags::default();
      cursor.del(write_flags)?;
    }
    drop(cursor);
    txn.commit()?;

    Ok(opt_pop.map(|(_, val)| val))
  }

  /// Remove the first `count` elements equal to `val` from the queue.
  pub fn remove<V>(&self, txn: &mut RwTransaction, count: usize, val: V) -> Result<usize>
  where
    V: AsRef<[u8]>,
  {
    let mut txn = txn.begin_nested_txn()?;
    let mut iter = self.iter(&txn)?;
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
    txn.commit()?;
    Ok(removed)
  }

  /// Create an iterator over the elements of the queue.
  pub fn iter<'txn, T>(&self, txn: &'txn T) -> Result<QueueIter<'txn>>
  where
    T: Transaction,
  {
    let mut cursor = txn.open_ro_cursor(self.elements)?;

    Ok(QueueIter {
      inner: cursor.iter_from(self.uuid.as_bytes()),
      uuid: self.uuid,
    })
  }

  /// Returns and removes the head element of the queue, and pushes the element
  /// at the tail of destination.
  pub fn pop_and_move<'txn>(
    &self,
    txn: &'txn mut RwTransaction,
    dest: &Self,
  ) -> Result<Option<Vec<u8>>> {
    if let Some(elm) = self.pop(txn)? {
      // Hate to copy, but rust does not allow to pass txn as mutable twice
      // when holding the reference to elm.
      let copy = elm.to_vec();
      dest.push(txn, &copy)?;
      return Ok(Some(copy));
    }

    Ok(None)
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

/// Queues handler in a dedicated LMDB database.
#[derive(Debug, Clone)]
pub struct QueueDb {
  elements: Database,
  meta: Database,
}

impl QueueDb {
  /// Meta DB name.
  const META_DB_NAME: &'static str = "__queue_meta";
  /// Elements DB name.
  const ELEMENTS_DB_NAME: &'static str = "__queue_elements";

  // Open a database of queues.
  pub fn open(env: &Environment) -> Result<Self> {
    let db_flags = lmdb::DatabaseFlags::default();
    let elements = env.create_db(Some(Self::ELEMENTS_DB_NAME), db_flags)?;
    let meta = env.create_db(Some(Self::META_DB_NAME), db_flags)?;

    Ok(Self { elements, meta })
  }

  /// Get a queue with the provided key.
  pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Queue {
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_ref());
    let keys = QueueKeys::new(&uuid);
    let elements = self.elements;
    let meta = self.meta;

    Queue {
      elements,
      meta,
      uuid,
      keys,
    }
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
    let queue_db = QueueDb::open(&env)?;
    let queue_1 = queue_db.get("myqueue");
    let queue_2 = queue_db.get("anotherqueue");

    let mut tx = env.begin_rw_txn()?;
    queue_1.push(&mut tx, "Y")?;
    queue_1.push(&mut tx, "Z")?;
    queue_1.push(&mut tx, "X")?;
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    let mut iter = queue_1.iter(&tx)?;
    assert_eq!(Some(Ok("Y")), iter.next().map(utf8_to_str));
    assert_eq!(Some(Ok("Z")), iter.next().map(utf8_to_str));
    assert_eq!(Some(Ok("X")), iter.next().map(utf8_to_str));
    assert_eq!(None, iter.next().map(utf8_to_str));
    tx.commit()?;

    let mut tx = env.begin_rw_txn()?;
    queue_2.push(&mut tx, "A")?;
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    let mut iter = queue_2.iter(&tx)?;
    assert_eq!(Some(Ok("A")), iter.next().map(utf8_to_str));
    assert_eq!(None, iter.next().map(utf8_to_str));
    tx.commit()
  }

  #[test]
  fn push_full_queue() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue_db = QueueDb::open(&env)?;
    let queue = queue_db.get("myqueue");
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
    let queue_db = QueueDb::open(&env)?;
    let queue_1 = queue_db.get("myqueue");
    let queue_2 = queue_db.get("anotherqueue");

    let mut tx = env.begin_rw_txn()?;
    queue_1.push(&mut tx, "Y")?;
    queue_1.push(&mut tx, "Z")?;
    queue_2.push(&mut tx, "A")?;

    let opt_pop = queue_1.pop(&mut tx).transpose();
    assert_eq!(Some(Ok("Y")), opt_pop.map(utf8_to_str));
    let opt_pop = queue_1.pop(&mut tx).transpose();
    assert_eq!(Some(Ok("Z")), opt_pop.map(utf8_to_str));

    let opt_pop = queue_1.pop(&mut tx).transpose();
    assert_eq!(None, opt_pop.map(utf8_to_str));
    tx.commit()
  }

  #[test]
  fn remove() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue_db = QueueDb::open(&env)?;
    let queue = queue_db.get("myqueue");
    let mut tx = env.begin_rw_txn()?;
    queue.push(&mut tx, "X")?;
    queue.push(&mut tx, "X")?;
    queue.push(&mut tx, "Y")?;

    let removed = queue.remove(&mut tx, 2, "X")?;
    assert_eq!(2, removed);
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    let mut iter = queue.iter(&tx)?;
    assert_eq!(Some(Ok("Y")), iter.next().map(utf8_to_str));
    assert_eq!(None, iter.next());
    Ok(())
  }

  #[test]
  fn pop_and_move() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue_db = QueueDb::open(&env)?;
    let queue_1 = queue_db.get("myqueue1");
    let queue_2 = queue_db.get("myqueue2");
    let mut tx = env.begin_rw_txn()?;
    queue_1.push(&mut tx, &[100])?;
    queue_1.push(&mut tx, &[200])?;
    let removed = queue_1.pop_and_move(&mut tx, &queue_2)?;
    assert_eq!(Some(vec![100]), removed);
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    let mut iter_1 = queue_1.iter(&tx)?;
    assert_eq!(Some(Ok(&[200][..])), iter_1.next());
    assert_eq!(None, iter_1.next());
    let mut iter_2 = queue_2.iter(&tx)?;
    assert_eq!(Some(Ok(&[100][..])), iter_2.next());
    assert_eq!(None, iter_2.next());
    Ok(())
  }
}
