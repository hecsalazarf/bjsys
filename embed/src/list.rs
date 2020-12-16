//! A list with persitent storage aka `VecDeque`.
use sled::{Batch, Db, Event, IVec, Result, Tree};
use std::convert::TryInto;
use std::iter::DoubleEndedIterator;

/// A list with persitent storage aka `VecDeque`.
///
/// It is a collection of elements sorted according to the order of insertion, also
/// know as a `VecDequeue` but persisted on disk. The typical usage is as a queue,
/// where [`lpush`] adds elements to the queue, and [`rpop`] to remove from the queue.
/// 
/// [`lpush`]: List::lpush
/// [`rpop`]: List::rpop
#[derive(Clone)]
pub struct List {
  tree: Tree,
}

impl List {
  /// Open a list with the provided key.
  pub fn open(db: &Db, key: &str) -> Result<Self> {
    let tree = db.open_tree(key)?;
    Ok(Self { tree })
  }

  /// Preppend one element to the list.
  pub fn lpush<V: Into<IVec>>(&self, val: V) -> Result<()> {
    let next = self.next_first()?;
    self.tree.insert(next.to_be_bytes(), val)?;
    Ok(())
  }

  /// Preppend multiple elements to the list.
  pub fn lpush_multi<I, V>(&self, values: I) -> Result<()>
  where
    I: IntoIterator<Item = V>,
    V: Into<IVec>,
  {
    let mut batch = Batch::default();
    let mut next = self.next_first()?;

    for val in values {
      batch.insert(&next.to_be_bytes(), val);
      next -= 1;
    }
    self.tree.apply_batch(batch)?;
    Ok(())
  }

  /// Append one element to the list.
  pub fn rpush<V: Into<IVec>>(&self, val: V) -> Result<()> {
    let next = self.next_last()?;
    self.tree.insert(next.to_be_bytes(), val)?;
    Ok(())
  }

  /// Append multiple elements to the list.
  pub fn rpush_multi<I, V>(&self, values: I) -> Result<()>
  where
    I: IntoIterator<Item = V>,
    V: Into<IVec>,
  {
    let mut batch = Batch::default();
    let mut next = self.next_last()?;

    for val in values {
      batch.insert(&next.to_be_bytes(), val);
      next += 1;
    }
    self.tree.apply_batch(batch)?;
    Ok(())
  }

  /// Remove and get the first element in the list.
  pub fn lpop(&self) -> Result<Option<IVec>> {
    let opt_val = self.tree.pop_min()?.map(|(_, v)| v);
    Ok(opt_val)
  }

  /// Remove and get the first element in the list, or block until one
  /// is available.
  pub fn blpop(&self) -> Result<IVec> {
    let val = self.lpop()?.unwrap_or_else(|| self.wait_insert());
    Ok(val)
  }

  /// Remove and get the first element in the list, or asynchronously
  /// wait until one is available.
  pub async fn blpop_async(&self) -> Result<IVec> {
    if let Some(val) = self.lpop()? {
      return Ok(val)
    }

    Ok(self.wait_insert_async().await)
  }

  /// Remove and get the last element in the list.
  pub fn rpop(&self) -> Result<Option<IVec>> {
    let opt_val = self.tree.pop_max()?.map(|(_, v)| v);
    Ok(opt_val)
  }

  /// Remove and get the last element in the list, or block until one
  /// is available.
  pub fn brpop(&self) -> Result<IVec> {
    let val = self.rpop()?.unwrap_or_else(|| self.wait_insert());
    Ok(val)
  }

  /// Remove and get the last element in the list, or asynchronously
  /// wait until one is available.
  pub async fn brpop_async(&self) -> Result<IVec> {
    if let Some(val) = self.rpop()? {
      return Ok(val)
    }

    Ok(self.wait_insert_async().await)
  }

  /// Clear the whole list.
  pub fn clear(&self) -> Result<()> {
    self.tree.clear()
  }

  /// Create a double-ended iterator over the elements of the list.
  pub fn iter(&self) -> impl DoubleEndedIterator<Item = Result<IVec>> {
    self.tree.iter().values()
  }

  /// Return the number of elements in this list.
  pub fn len(&self) -> usize {
    self.tree.len()
  }

  /// The key of a new element to append, is obtained by decrementing
  /// the previous(first) key. If the list is empty, the key is
  /// `isize::MAX`
  fn next_first(&self) -> Result<isize> {
    let next = match self.first_key()? {
      Some(key) if key > isize::MIN => key - 1,
      _ => isize::MAX,
    };
    Ok(next)
  }

  /// The key of a new element to preppend, is obtained by incrementing
  /// the previous(last) key. If the list is empty, the key is
  /// `isize::MIN`
  fn next_last(&self) -> Result<isize> {
    let next = match self.last_key()? {
      Some(key) if key < isize::MAX => key + 1,
      _ => isize::MIN,
    };
    Ok(next)
  }

  fn first_key(&self) -> Result<Option<isize>> {
    Ok(Self::extract_key(self.tree.first()?))
  }

  fn last_key(&self) -> Result<Option<isize>> {
    Ok(Self::extract_key(self.tree.last()?))
  }

  fn wait_insert(&self) -> IVec {
    let mut subscriber = self.tree.watch_prefix(vec![]);
    loop {
      // There might be remove events in which case, we block again
      if let Some(Event::Insert { key: _, value }) = subscriber.next() {
        return value;
      }
    }
  }

  async fn wait_insert_async(&self) -> IVec {
    let mut subscriber = self.tree.watch_prefix(vec![]);
    loop {
      // There might be remove events in which case, we block again
      if let Some(Event::Insert { key: _, value }) = (&mut subscriber).await {
        return value;
      }
    }
  }

  fn extract_key(tuple: Option<(IVec, IVec)>) -> Option<isize> {
    tuple.as_ref().map(|(k, _)| ivec_to_isize(k))
  }
}

fn ivec_to_isize(val: &IVec) -> isize {
  isize::from_be_bytes(val.as_ref().try_into().unwrap())
}

#[cfg(test)]
mod tests {
  use super::*;

  fn new_list() -> List {
    let db = sled::Config::new().temporary(true).open().unwrap();
    List::open(&db, "my_list").unwrap()
  }

  #[test]
  fn list_push() {
    let list = new_list();
    list.rpush("X").unwrap();
    list.lpush("Y").unwrap();
    list.rpush("Z").unwrap();

    let mut iter = list.iter();
    assert_eq!(Some(Ok(IVec::from("Y"))), iter.next());
    assert_eq!(Some(Ok(IVec::from("X"))), iter.next());
    assert_eq!(Some(Ok(IVec::from("Z"))), iter.next());
    assert_eq!(3, list.len());
  }

  #[test]
  fn list_push_multiple() {
    let list = new_list();
    list.lpush_multi(vec!["X", "Y", "Z"]).unwrap();

    let mut iter = list.iter();
    assert_eq!(Some(Ok(IVec::from("Z"))), iter.next());
    assert_eq!(Some(Ok(IVec::from("Y"))), iter.next());
    assert_eq!(Some(Ok(IVec::from("X"))), iter.next());
    assert_eq!(3, list.len());
  }

  #[test]
  fn list_pop() {
    let list = new_list();
    list.rpush_multi(vec!["X", "Y", "Z"]).unwrap();

    assert_eq!(Ok(Some(IVec::from("X"))), list.lpop());
    assert_eq!(Ok(Some(IVec::from("Z"))), list.rpop());
    assert_eq!(Ok(Some(IVec::from("Y"))), list.rpop());
    assert_eq!(0, list.len());
  }

  #[test]
  fn list_bpop() {
    let list = new_list();
    let list_2 = list.clone();
    std::thread::spawn(move || {
      std::thread::sleep(std::time::Duration::from_secs(1));
      list_2.rpush("Y").unwrap();
    });

    // Blocking
    println!("Blocking for 1 sec");
    assert_eq!(Ok(IVec::from("Y")), list.blpop());
  }

  #[tokio::test]
  async fn list_bpop_async() {
    let list = new_list();
    let list_2 = list.clone();
    tokio::spawn(async move {
      tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;
      list_2.rpush("Z").unwrap();
    });

    // Non-blocking
    println!("Waiting for 1 secs");
    assert_eq!(Ok(IVec::from("Z")), list.brpop_async().await);
  }
}
