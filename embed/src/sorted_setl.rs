//! A sorted set with persistent storage.
use crate::extension::TransactionExt;
use lmdb::{
  Cursor, Database, Environment, Iter, Result, RoTransaction, RwTransaction, Transaction,
  WriteFlags,
};
use std::ops::{Bound, RangeBounds};
use uuid::Uuid;

/// A sorted set with persistent storage.
///
/// Sorted sets are composed of unique, non-repeating elements, sorted by a `u64`
/// number called score.
#[derive(Clone, Debug)]
pub struct SortedSet {
  /// DB storing <hashed key + score + element> encoded in the its key,
  /// serving as a skip list.
  skiplist: Database,
  /// DB mapping elements to their scores, simply a hash table.
  elements: Database,
  /// UUID v5 produced from sorted set key.
  uuid: Uuid,
}

type BoundLimit = [u8; SortedSet::SKIPLIST_PREFIX_LEN];

impl SortedSet {
  /// UUID length.
  const UUID_LEN: usize = 16;
  /// Prefix length of skiplist.
  const SKIPLIST_PREFIX_LEN: usize = 24;
  /// Skiplist DB name.
  const SKIPLIST_DB_NAME: &'static str = "__sorted_set_skiplist";
  /// Members DB name.
  const ELEMENTS_DB_NAME: &'static str = "__sorted_set_elements";

  /// Open the sorted set with the specified key.
  pub fn open<K>(env: &Environment, key: K) -> Result<Self>
  where
    K: AsRef<[u8]>,
  {
    let db_flags = lmdb::DatabaseFlags::default();
    let skiplist = env.create_db(Some(Self::SKIPLIST_DB_NAME), db_flags)?;
    let elements = env.create_db(Some(Self::ELEMENTS_DB_NAME), db_flags)?;
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_ref());

    Ok(Self {
      skiplist,
      uuid,
      elements,
    })
  }

  /// Add one element with the specified score. If specified element is already
  /// a member of the sorted set, the score is updated and the element reinserted
  /// at the right position to ensure the correct ordering.
  pub fn add<V>(&self, txn: &mut RwTransaction, score: u64, val: V) -> Result<()>
  where
    V: AsRef<[u8]>,
  {
    let encoded_element = self.encode_elements_key(val.as_ref());
    if let Some(old_score) = txn.get_opt(self.elements, &encoded_element)? {
      // If the member already exists, remove it from skiplist before new insertion
      let encoded_key = self.encode_skiplist_key(old_score, val.as_ref());
      txn.del(self.skiplist, &encoded_key, None)?;
    }

    let write_flags = WriteFlags::default();
    let encoded_score = score.to_be_bytes();
    let encoded_key = self.encode_skiplist_key(&encoded_score, val.as_ref());
    // Insert new element into both skiplist and members databases
    txn.put(self.skiplist, &encoded_key, &[], write_flags)?;
    txn.put(self.elements, &encoded_element, &encoded_score, write_flags)?;
    Ok(())
  }

  /// Return all the elements in the sorted set with a score between `range`.
  /// The elements are considered to be sorted from low to high scores.
  pub fn range_by_score<'txn, R>(
    &self,
    txn: &'txn RoTransaction,
    range: R,
  ) -> Result<SortedRange<'txn>>
  where
    R: RangeBounds<u64>,
  {
    let (start, end) = self.to_bytes_range(range);
    let mut cursor = txn.open_ro_cursor(self.skiplist)?;
    let iter = cursor.iter_from(start);
    let uuid = self.uuid;

    Ok(SortedRange { end, iter, uuid })
  }

  /// Remove the specified element from the sorted set, returning `true` when
  /// the member existed and was removed. If member is non-existant the result
  /// is `false`.
  pub fn remove<V>(&self, txn: &mut RwTransaction, val: V) -> Result<bool>
  where
    V: AsRef<[u8]>,
  {
    let encoded_member = self.encode_elements_key(val.as_ref());
    if let Some(score) = txn.get_opt(self.elements, &encoded_member)? {
      let encoded_key = self.encode_skiplist_key(score, val.as_ref());
      txn.del(self.skiplist, &encoded_key, None)?;
      txn.del(self.elements, &encoded_member, None)?;
      return Ok(true);
    }
    Ok(false)
  }

  fn encode_elements_key(&self, val: &[u8]) -> Vec<u8> {
    let mut key = self.uuid.as_bytes().to_vec();
    key.extend(val);
    key
  }

  fn encode_skiplist_key(&self, score: &[u8], val: &[u8]) -> Vec<u8> {
    let mut key = self.uuid.as_bytes().to_vec();
    key.extend(score);
    key.extend(val);
    key
  }

  fn to_bytes_range<R>(&self, range: R) -> (BoundLimit, Bound<BoundLimit>)
  where
    R: RangeBounds<u64>,
  {
    let uuid_bytes = self.uuid.as_bytes();
    let start = match range.start_bound() {
      Bound::Excluded(score) => {
        // Increment one to exclude the start range
        // TODO Analyze edge case when score is u64::MAX. Such case should
        // return an empty interator
        Self::create_bound_limit(uuid_bytes, &score.saturating_add(1))
      }
      Bound::Included(score) => {
        // Included bound
        Self::create_bound_limit(uuid_bytes, score)
      }
      Bound::Unbounded => {
        // Unbounded start has zeroed score
        Self::create_bound_limit(uuid_bytes, &u64::MIN)
      }
    };

    let end = match range.end_bound() {
      Bound::Excluded(score) => {
        let bound = Self::create_bound_limit(uuid_bytes, score);
        Bound::Excluded(bound)
      }
      Bound::Included(score) => {
        // We add one to an included score, so that the last member is included
        // in the returned iterator
        if let Some(s) = score.checked_add(1) {
          let bound = Self::create_bound_limit(uuid_bytes, &s);
          Bound::Included(bound)
        } else {
          // However, the max score can overflow. Such case requires to increment
          // the UUID to cover the whole set
          self.create_unbounded_limit()
        }
      }
      Bound::Unbounded => self.create_unbounded_limit(),
    };

    (start, end)
  }

  fn create_unbounded_limit(&self) -> Bound<BoundLimit> {
    use std::convert::TryInto;

    // Copy the uuid to increment the last byte only
    let uuid_bytes = self.uuid.as_bytes().as_ref();
    let mut uuid: [u8; Self::UUID_LEN] = uuid_bytes.try_into().expect("uuid into array");
    let last = uuid.last_mut().unwrap();
    if let Some(l) = last.checked_add(1) {
      *last = l;
      let bound = Self::create_bound_limit(&uuid, &u64::MIN);
      Bound::Included(bound)
    } else {
      // But even the UUID can overflow, meaning we reached the sorted set max
      // UUID. Very improbable scenario, but theoretically posible. Just in case
      Bound::Unbounded
    }
  }

  fn create_bound_limit(uuid: &[u8], score: &u64) -> BoundLimit {
    use std::convert::TryInto;

    let uuid_slice: &[u8; Self::UUID_LEN] = uuid.try_into().expect("uuid into array");
    let score_bytes = score.to_be_bytes();
    // Chain uuid with score
    let chain = uuid_slice.iter().chain(&score_bytes);
    let mut limit = [0; Self::SKIPLIST_PREFIX_LEN];
    // Copy chain to the limit array
    limit
      .iter_mut()
      .zip(chain)
      .for_each(|(new, chained)| *new = *chained);
    limit
  }
}

/// Iterator with elements returned after calling SortedSet::range_by_score.
#[derive(Debug)]
pub struct SortedRange<'txn> {
  end: Bound<BoundLimit>,
  iter: Iter<'txn>,
  uuid: Uuid,
}

impl<'txn> SortedRange<'txn> {
  fn next_inner(&mut self) -> Option<Result<&'txn [u8]>> {
    let res = self.iter.next()?;
    if res.is_err() {
      return Some(Err(res.unwrap_err()));
    }

    let key = res.unwrap().0;
    let prefix_key = &key[..SortedSet::SKIPLIST_PREFIX_LEN];

    let is_in_range = match self.end {
      Bound::Excluded(k) => prefix_key < &k[..],
      Bound::Included(k) => prefix_key <= &k[..],
      Bound::Unbounded => {
        let uuid_bytes = self.uuid.as_bytes();
        &key[..uuid_bytes.len()] != uuid_bytes
      }
    };

    if is_in_range {
      Some(Ok(&key[SortedSet::SKIPLIST_PREFIX_LEN..]))
    } else {
      None
    }
  }
}

impl<'txn> Iterator for SortedRange<'txn> {
  type Item = Result<&'txn [u8]>;

  fn next(&mut self) -> Option<Self::Item> {
    self.next_inner()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
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
  fn sorted_setl_range_by_score() {
    let (_tmpdir, env) = new_env();
    let set_a = SortedSet::open(&env, "set_a").unwrap();
    let set_b = SortedSet::open(&env, "set_b").unwrap();

    // Add to first set
    let mut tx = env.begin_rw_txn().expect("rw txn");
    set_a.add(&mut tx, 100, "Elephant").unwrap();
    set_a.add(&mut tx, 50, "Bear").unwrap();
    set_a.add(&mut tx, 20, "Cat").unwrap();
    set_a.add(&mut tx, u64::MAX, "Bigger Elephant").unwrap();
    tx.commit().unwrap();

    // Add to second set
    let mut tx = env.begin_rw_txn().expect("rw txn");
    set_b.add(&mut tx, 100, "Truck").unwrap();
    set_b.add(&mut tx, 50, "Sedan").unwrap();
    tx.commit().unwrap();

    // Get a subset
    let tx = env.begin_ro_txn().expect("ro txn");
    let mut range = set_a
      .range_by_score(&tx, 20..=50)
      .unwrap()
      .map(|i| i.unwrap());
    assert_eq!(range.next().map(convert_to_str), Some("Cat"));
    assert_eq!(range.next().map(convert_to_str), Some("Bear"));
    assert_eq!(range.next().map(convert_to_str), None);

    // Exclude last member
    let tx = tx.reset().renew().unwrap();
    let mut range = set_a
      .range_by_score(&tx, 100..u64::MAX)
      .unwrap()
      .map(|i| i.unwrap());
    assert_eq!(range.next().map(convert_to_str), Some("Elephant"));
    assert_eq!(range.next().map(convert_to_str), None);

    // Include last member
    let tx = tx.reset().renew().unwrap();
    let mut range = set_a
      .range_by_score(&tx, 100..=u64::MAX)
      .unwrap()
      .map(|i| i.unwrap());
    assert_eq!(range.next().map(convert_to_str), Some("Elephant"));
    assert_eq!(range.next().map(convert_to_str), Some("Bigger Elephant"));
    assert_eq!(range.next(), None);

    // Get all members with an unbounded range
    let tx = tx.reset().renew().unwrap();
    let mut range = set_b.range_by_score(&tx, ..).unwrap().map(|i| i.unwrap());
    assert_eq!(range.next().map(convert_to_str), Some("Sedan"));
    assert_eq!(range.next().map(convert_to_str), Some("Truck"));
    assert_eq!(range.next(), None);
  }

  #[test]
  fn sorted_setl_unique_member() {
    let (_tmpdir, env) = new_env();
    let set_a = SortedSet::open(&env, "set_a").unwrap();
    let mut tx = env.begin_rw_txn().expect("rw txn");
    set_a.add(&mut tx, 100, "Elephant").unwrap();
    // Update the same member with a different score
    set_a.add(&mut tx, 2000, "Elephant").unwrap();
    tx.commit().unwrap();

    // Get the whole set
    let tx = env.begin_ro_txn().expect("ro txn");
    let mut range = set_a.range_by_score(&tx, ..).unwrap().map(|i| i.unwrap());
    assert_eq!(range.next().map(convert_to_str), Some("Elephant"));
    assert_eq!(range.next(), None);
  }

  #[test]
  fn sorted_setl_same_score_diff_member() {
    let (_tmpdir, env) = new_env();
    let set_a = SortedSet::open(&env, "set_a").unwrap();
    let mut tx = env.begin_rw_txn().expect("rw txn");
    set_a.add(&mut tx, 100, "Asian Elephant").unwrap();
    // Add new member with the same score
    set_a.add(&mut tx, 100, "African Elephant").unwrap();
    tx.commit().unwrap();

    // Get the whole set
    let tx = env.begin_ro_txn().expect("ro txn");
    let mut range = set_a.range_by_score(&tx, ..).unwrap().map(|i| i.unwrap());
    assert_eq!(range.next().map(convert_to_str), Some("African Elephant"));
    assert_eq!(range.next().map(convert_to_str), Some("Asian Elephant"));
    assert_eq!(range.next(), None);
  }

  #[test]
  fn sorted_setl_remove_member() {
    let (_tmpdir, env) = new_env();
    let set_a = SortedSet::open(&env, "set_a").unwrap();

    let mut tx = env.begin_rw_txn().expect("rw txn");
    set_a.add(&mut tx, 2000, "Elephant").unwrap();
    tx.commit().unwrap();

    let mut tx = env.begin_rw_txn().expect("rw txn");
    assert_eq!(Ok(true), set_a.remove(&mut tx, "Elephant"));
    tx.commit().unwrap();

    let mut tx = env.begin_rw_txn().expect("rw txn");
    assert_eq!(Ok(false), set_a.remove(&mut tx, "Elephant"));
  }
}
