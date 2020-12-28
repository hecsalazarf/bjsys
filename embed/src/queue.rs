//! A queue with persitent storage.
use crate::tree_ext::TreeExt;
use sled::transaction::{abort, TransactionResult as Result, Transactional};
use sled::{Db, Error, IVec, Result as SledRes, Tree};
use std::iter::DoubleEndedIterator;

#[derive(Clone)]
struct QueueKeys {
  head: IVec,
  tail: IVec,
  queue: IVec,
}

impl QueueKeys {
  fn new(key: &str) -> Self {
    let head = IVec::from(format!("{}_h", key).into_bytes());
    let tail = IVec::from(format!("{}_t", key).into_bytes());
    let queue = IVec::from(format!("__queue_{}", key).into_bytes());
    Self { head, tail, queue }
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
  /// Metadata tree, containing the head and tail
  meta: Tree,
}

impl Queue {
  /// Zero pointer of queue (0_i64)
  const ZERO_PTR: &'static [u8] = &[0, 0, 0, 0, 0, 0, 0, 0];
  /// Initial pointer of tail (-1_i64)
  const INIT_TAIL: &'static [u8] = &[255, 255, 255, 255, 255, 255, 255, 255];
  /// Min pointer of queue, meaning the pointer got wrapped.
  const WRAP_PTR: &'static [u8] = &[128, 0, 0, 0, 0, 0, 0, 0];

  /// Open a queue with the provided key.
  pub fn open(db: &Db, key: &str) -> Result<Self> {
    let keys = QueueKeys::new(key);
    let elements = db.open_tree(&keys.queue)?;
    let meta = db.open_tree("__lists_meta")?;
    let queue = Self {
      keys,
      elements,
      meta,
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

      elements.insert(next_tail, val.as_ref())?;
      Ok(())
    })
  }

  /// Pop one element from the queue's head. Return `None` if queue is empty.
  pub fn pop(&self) -> Result<Option<IVec>, Error> {
    let tx_group = (&self.meta, &self.elements);
    tx_group.transaction(|(meta, elements)| {
      let head = meta.get(&self.keys.head)?.unwrap();
      // Pop element from the head
      let el = elements.remove(&head)?;

      if el.is_some() {
        // If queue has some element, incremebt the head pointer
        let next_head = meta.incr_by(&self.keys.head, 1)?;
        if next_head.as_ref() == Self::WRAP_PTR {
          // Set head to zero when reached the max value
          meta.insert(&self.keys.head, Self::ZERO_PTR)?;
        }
      }
      Ok(el)
    })
  }

  /// Clear the whole queue.
  pub fn clear(&self) -> Result<()> {
    self.elements.clear()?;
    self.init_meta()
  }

  /// Create a double-ended iterator over the elements of the queue.
  pub fn iter(&self) -> impl DoubleEndedIterator<Item = SledRes<IVec>> {
    self.elements.iter().values()
  }

  /// Return the number of elements in this queue.
  pub fn len(&self) -> usize {
    self.elements.len()
  }

  fn init_meta(&self) -> Result<()> {
    (&self.meta).transaction(|meta| {
      meta.insert(&self.keys.head, Self::ZERO_PTR)?;
      meta.insert(&self.keys.tail, Self::INIT_TAIL)?;
      Ok(())
    })
  }

  /* fn wait_insert(&self) -> IVec {
    let mut subscriber = self.elements.watch_prefix(vec![]);
    loop {
      // There might be remove events in which case, we block again
      if let Some(Event::Insert { key: _, value }) = subscriber.next() {
        return value;
      }
    }
  }

  async fn wait_insert_async(&self) -> IVec {
    let mut subscriber = self.elements.watch_prefix(vec![]);
    loop {
      // There might be remove events in which case, we block again
      if let Some(Event::Insert { key: _, value }) = (&mut subscriber).await {
        return value;
      }
    }
  } */
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

  /* #[test]
  fn queue_bpop() {
    let queue = new_queue();
    let list_2 = queue.clone();
    std::thread::spawn(move || {
      std::thread::sleep(std::time::Duration::from_secs(1));
      list_2.rpush("Y").unwrap();
    });

    // Blocking
    println!("Blocking for 1 sec");
    assert_eq!(Ok(IVec::from("Y")), queue.blpop());
  }

  #[tokio::test]
  async fn queue_bpop_async() {
    let queue = new_queue();
    let list_2 = queue.clone();
    tokio::spawn(async move {
      tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;
      list_2.rpush("Z").unwrap();
    });

    // Non-blocking
    println!("Waiting for 1 secs");
    assert_eq!(Ok(IVec::from("Z")), queue.brpop_async().await);
  } */
}
