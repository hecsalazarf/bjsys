//! A queue with persitent storage.
use crate::tree_ext::TreeExt;
use sled::transaction::{abort, TransactionResult as Result, Transactional};
use sled::{Db, Error, IVec, Iter, Tree};
use std::iter::{DoubleEndedIterator, Iterator};
use std::result::Result as StdResult;
use uuid::Uuid;

#[derive(Clone)]
struct QueueKeys {
  /// Queue's head.
  head: IVec,
  /// Queue's tail.
  tail: IVec,
}

impl QueueKeys {
  const HEAD_SUFFIX: &'static [u8] = &[0];
  const TAIL_SUFFIX: &'static [u8] = &[1];

  fn new(uuid: &[u8]) -> Self {
    let mut head = uuid.to_vec();
    head.extend(Self::HEAD_SUFFIX);
    let mut tail = uuid.to_vec();
    tail.extend(Self::TAIL_SUFFIX);

    Self {
      head: head.into(),
      tail: tail.into(),
    }
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
  elements: Tree,
  /// Metadata tree, containing the head and tail values
  meta: Tree,
  /// UUID
  uuid: IVec,
}

impl Queue {
  /// Zero pointer of queue (0_i64)
  const ZERO_PTR: &'static [u8] = &[0, 0, 0, 0, 0, 0, 0, 0];
  /// Initial pointer of tail (-1_i64)
  const INIT_TAIL: &'static [u8] = &[255, 255, 255, 255, 255, 255, 255, 255];
  /// Min pointer of queue, meaning the pointer got wrapped.
  const WRAP_PTR: &'static [u8] = &[128, 0, 0, 0, 0, 0, 0, 0];
  /// Meta tree name.
  const META_TREE_NAME: &'static str = "__queue_meta";
  /// Elements tree name.
  const ELEMENTS_TREE_NAME: &'static str = "__queue_elements";

  /// Open a queue with the provided key.
  pub fn open<K>(db: &Db, key: K) -> Result<Self>
  where
    K: AsRef<[u8]>,
  {
    let elements = db.open_tree(Self::ELEMENTS_TREE_NAME)?;
    let meta = db.open_tree(Self::META_TREE_NAME)?;
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_ref())
      .as_bytes()
      .into();
    let keys = QueueKeys::new(key.as_ref());
    let queue = Self {
      keys,
      elements,
      meta,
      uuid,
    };

    if queue.meta.get(&queue.keys.head)?.is_none() {
      queue.init_meta()?;
    }

    Ok(queue)
  }

  /// Preppend one element to the queue's tail.
  pub fn push<V: AsRef<[u8]>>(&self, val: V) -> Result<(), Error> {
    let tx_group = (&self.meta, &self.elements);
    tx_group.transaction(|(meta, elements)| {
      let mut next_tail = meta.incr_by(&self.keys.tail, 1)?;
      if next_tail.as_ref() == Self::WRAP_PTR {
        // Set tail to its initial state
        meta.insert(&self.keys.tail, Self::ZERO_PTR)?;
        next_tail = IVec::from(Self::ZERO_PTR);
      }
      let head = meta.get(&self.keys.head)?.unwrap();
      if next_tail == head && elements.get(&head)?.is_some() {
        // Abort transaction if full
        return abort(Error::Unsupported("Full queue".into()));
      }
      let encoded_key = self.encode_members_key(next_tail.as_ref());
      elements.insert(&encoded_key, val.as_ref())?;
      Ok(())
    })
  }

  /// Pop one element from the queue's head. Return `None` if queue is empty.
  pub fn pop(&self) -> Result<Option<IVec>, Error> {
    let tx_group = (&self.meta, &self.elements);
    tx_group.transaction(|(meta, elements)| {
      let head = meta.get(&self.keys.head)?.unwrap();
      // Pop element from the head
      let encoded_key = self.encode_members_key(&head);
      let el = elements.remove(&encoded_key)?;

      if el.is_some() {
        // If queue has some element, increment the head pointer
        let next_head = meta.incr_by(&self.keys.head, 1)?;
        if next_head.as_ref() == Self::WRAP_PTR {
          // Set head to zero when reached the max value
          meta.insert(&self.keys.head, Self::ZERO_PTR)?;
        }
      }
      Ok(el)
    })
  }

  /// Pop one element from the queue's head or wait until one is available.
  pub async fn bpop(&self) -> Result<IVec, Error> {
    use sled::Event;

    if let Some(value) = self.pop()? {
      return Ok(value);
    }
    let mut subscriber = self.elements.watch_prefix(&self.uuid);

    loop {
      if let Some(Event::Insert { key: _, value }) = (&mut subscriber).await {
        return Ok(value);
      }
    }
  }

  /// Clear the whole queue.
  pub fn clear(&self) -> Result<()> {
    self.elements.clear()?;
    self.init_meta()
  }

  /// Create a double-ended iterator over the elements of the queue.
  pub fn iter(&self) -> QueueIter {
    QueueIter(self.elements.iter())
  }

  /// Return the number of elements in this queue.
  pub fn len(&self) -> usize {
    self.elements.len()
  }

  fn init_meta(&self) -> Result<()> {
    self.meta.insert_many(&[
      (&self.keys.head, Self::ZERO_PTR),
      (&self.keys.tail, Self::INIT_TAIL),
    ])?;
    Ok(())
  }

  fn encode_members_key(&self, index: &[u8]) -> [u8; 24] {
    let chain = self.uuid.iter().chain(index);
    let mut key = [0; 24];
    key
      .iter_mut()
      .zip(chain)
      .for_each(|(new, chained)| *new = *chained);
    key
  }
}

pub struct QueueIter(Iter);

impl Iterator for QueueIter {
  type Item = Result<IVec>;

  fn next(&mut self) -> Option<Self::Item> {
    self.0.next().map(map_iter)
  }

  fn last(self) -> Option<Self::Item> {
    self.0.last().map(map_iter)
  }
}

impl DoubleEndedIterator for QueueIter {
  fn next_back(&mut self) -> Option<Self::Item> {
    self.0.next_back().map(map_iter)
  }
}

fn map_iter(res: StdResult<(IVec, IVec), Error>) -> Result<IVec> {
  res.map(|(_, value)| value).map_err(|err| err.into())
}

#[cfg(test)]
mod tests {
  use super::*;

  fn new_queue() -> Queue {
    let db = sled::Config::new().temporary(true).open().unwrap();
    Queue::open(&db, "my_list").unwrap()
  }

  #[test]
  fn queue_push() {
    let queue = new_queue();
    queue.push("Y").unwrap();
    queue.push("Z").unwrap();
    queue.push("X").unwrap();
    assert_eq!(3, queue.len());
    let mut iter = queue.iter();
    assert_eq!(Some(Ok(IVec::from("Y"))), iter.next());
    assert_eq!(Some(Ok(IVec::from("Z"))), iter.next());
    assert_eq!(Some(Ok(IVec::from("X"))), iter.next());
  }

  #[test]
  fn queue_pop() {
    let queue = new_queue();
    assert_eq!(Ok(None), queue.pop());
    queue.push("Z").unwrap();
    assert_eq!(Ok(Some(IVec::from("Z"))), queue.pop());
    queue.push("X").unwrap();
    queue.push("X").unwrap();

    assert_eq!(Ok(Some(IVec::from("X"))), queue.pop());
    assert_eq!(Ok(Some(IVec::from("X"))), queue.pop());
    assert_eq!(Ok(None), queue.pop());
    queue.push("Y").unwrap();
    assert_eq!(Ok(Some(IVec::from("Y"))), queue.pop());
  }

  #[tokio::test]
  async fn queue_bpop() {
    let queue_1 = new_queue();
    let queue_2 = queue_1.clone();
    tokio::spawn(async move {
      tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;
      queue_2.push("Z").unwrap();
    });

    println!("Waiting for 1 sec");
    assert_eq!(Ok(IVec::from("Z")), queue_1.bpop().await);
  }
}
