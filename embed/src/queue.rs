//! A queue with persitent storage.
use crate::extension::TransactionExt;
use lmdb::{Cursor, Database, Environment, Error, Iter, Result, RwTransaction, WriteFlags};
use uuid::Uuid;

type MetaKey = [u8; 17];
type ElementKey = [u8; 24];

#[derive(Clone)]
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
#[derive(Clone)]
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
  /// Zero pointer of queue (0_i64)
  const ZERO_PTR: &'static [u8] = &[0, 0, 0, 0, 0, 0, 0, 0];
  /// Initial pointer of tail (-1_i64)
  const INIT_TAIL: &'static [u8] = &[255, 255, 255, 255, 255, 255, 255, 255];
  /// Min pointer of queue, meaning the pointer got wrapped.
  const WRAP_PTR: &'static [u8] = &[128, 0, 0, 0, 0, 0, 0, 0];
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

    let tx = env.begin_ro_txn()?;
    if tx.get_opt(elements, &keys.tail)?.is_none() {
      use lmdb::Transaction;
      let mut tx = env.begin_rw_txn()?;
      let flags = WriteFlags::default();
      tx.put(meta, &keys.tail, &Self::INIT_TAIL, flags)?;
      tx.commit()?;
    }

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

    let opt_tail = txn.get_opt(self.meta, &self.keys.tail)?;

    let mut next_tail = incr_slice(opt_tail, 1)?;

    if next_tail == Self::WRAP_PTR {
      // Set tail to its initial state
      txn.put(self.meta, &self.keys.tail, &Self::ZERO_PTR, write_flags)?;
      next_tail = [0; 8];
    } else {
      txn.put(self.meta, &self.keys.tail, &next_tail, write_flags)?;
    }

    let encoded_key = self.encode_members_key(&next_tail);
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
    let iter = self.scan(txn)?;
    let val = val.as_ref();
    let mut removed = 0;

    for i in iter {
      let (key, value) = i?;
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
    let inner = self.scan(txn)?;
    Ok(QueueIter { inner })
  }

  /// Scan over all the elements in this queue.
  fn scan<'txn, T>(&self, txn: &'txn T) -> Result<InnerIter<'txn>>
  where
    T: lmdb::Transaction,
  {
    let mut cursor = txn.open_ro_cursor(self.elements)?;
    let inner = InnerIter {
      iter: cursor.iter_from(self.uuid.as_bytes()),
      uuid: self.uuid,
    };

    Ok(inner)
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

/// Auxiliar function that increments a &[u8] by `incr`. Returns error if `slice`
/// is not a valid `i64`.
fn incr_slice(slice: Option<&[u8]>, incr: i64) -> Result<[u8; 8]> {
  use std::convert::TryInto;

  if let Some(val) = slice {
    let arr = val.try_into().map_err(|_| Error::Invalid)?;
    let new_incr = i64::from_be_bytes(arr).wrapping_add(incr);
    Ok(new_incr.to_be_bytes())
  } else {
    // If key is empty, initialize with incr
    Ok(incr.to_be_bytes())
  }
}

struct InnerIter<'txn> {
  iter: Iter<'txn>,
  uuid: Uuid,
}

impl<'txn> Iterator for InnerIter<'txn> {
  type Item = Result<(&'txn [u8], &'txn [u8])>;

  fn next(&mut self) -> Option<Self::Item> {
    let next = self.iter.next();
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

pub struct QueueIter<'txn> {
  inner: InnerIter<'txn>,
}

impl<'txn> Iterator for QueueIter<'txn> {
  type Item = Result<&'txn [u8]>;

  fn next(&mut self) -> Option<Self::Item> {
    self.inner.next().map(|res| res.map(|(_, val)| val))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use lmdb::Transaction;
  use tempfile::TempDir;

  fn new_env() -> (TempDir, Environment) {
    let tmp_dir = tempfile::Builder::new()
      .prefix("lmdb")
      .tempdir()
      .expect("tmp dir");
    let mut builder = Environment::new();
    builder.set_max_dbs(10);
    let env = builder.open(tmp_dir.path()).expect("open env");

    (tmp_dir, env)
  }

  fn convert_to_str(val: &[u8]) -> &str {
    std::str::from_utf8(val).expect("convert slice")
  }

  #[test]
  fn queue_push() {
    let (_tmpdir, env) = new_env();
    let queue = Queue::open(&env, "myqueue").expect("open queue");
    let mut tx = env.begin_rw_txn().expect("rw txn");
    queue.push(&mut tx, "Y").unwrap();
    queue.push(&mut tx, "Z").unwrap();
    queue.push(&mut tx, "X").unwrap();
    tx.commit().unwrap();

    let tx = env.begin_ro_txn().expect("ro txn");
    let mut iter = queue.iter(&tx).expect("iter").map(|i| i.unwrap());
    assert_eq!(Some("Y"), iter.next().map(convert_to_str));
    assert_eq!(Some("Z"), iter.next().map(convert_to_str));
    assert_eq!(Some("X"), iter.next().map(convert_to_str));
    assert_eq!(None, iter.next().map(convert_to_str));
    tx.commit().unwrap();

    let queue2 = Queue::open(&env, "anotherqueue").expect("open queue");
    let mut tx = env.begin_rw_txn().expect("rw txn");
    queue2.push(&mut tx, "A").unwrap();
    tx.commit().unwrap();

    let tx = env.begin_ro_txn().expect("ro txn");
    let mut iter = queue2.iter(&tx).expect("iter").map(|i| i.unwrap());
    assert_eq!(Some("A"), iter.next().map(convert_to_str));
    assert_eq!(None, iter.next().map(convert_to_str));
    tx.commit().unwrap();
  }

  #[test]
  fn queue_pop() {
    let (_tmpdir, env) = new_env();
    let queue = Queue::open(&env, "myqueue").expect("open queue");
    let mut tx = env.begin_rw_txn().expect("rw txn");
    queue.push(&mut tx, "Y").unwrap();
    queue.push(&mut tx, "Z").unwrap();
    tx.commit().unwrap();

    let queue2 = Queue::open(&env, "anotherqueue").expect("open queue");
    let mut tx = env.begin_rw_txn().expect("rw txn");
    queue2.push(&mut tx, "A").unwrap();
    tx.commit().unwrap();

    let mut tx = env.begin_rw_txn().expect("rw txn");
    let opt_pop = queue.pop(&mut tx).unwrap();
    assert_eq!(Some("Y"), opt_pop.map(convert_to_str));
    tx.commit().unwrap();

    let mut tx = env.begin_rw_txn().expect("rw txn");
    let opt_pop = queue.pop(&mut tx).unwrap();
    assert_eq!(Some("Z"), opt_pop.map(convert_to_str));
    tx.commit().unwrap();

    let mut tx = env.begin_rw_txn().expect("rw txn");
    let opt_pop = queue.pop(&mut tx).unwrap();
    assert_eq!(None, opt_pop.map(convert_to_str));
    tx.commit().unwrap();
  }

  #[test]
  fn queue_remove() {
    let (_tmpdir, env) = new_env();
    let queue = Queue::open(&env, "myqueue").expect("open queue");
    let mut tx = env.begin_rw_txn().expect("rw txn");
    queue.push(&mut tx, "X").unwrap();
    queue.push(&mut tx, "X").unwrap();
    queue.push(&mut tx, "Y").unwrap();
    tx.commit().unwrap();

    let mut tx = env.begin_rw_txn().expect("rw txn");
    let removed = queue.remove(&mut tx, 2, "X").unwrap();
    tx.commit().unwrap();
    assert_eq!(2, removed);

    let tx = env.begin_ro_txn().expect("ro txn");
    let mut iter = queue.iter(&tx).expect("iter").map(|i| i.unwrap());
    assert_eq!(Some("Y"), iter.next().map(convert_to_str));
    assert_eq!(None, iter.next());
  }
}
