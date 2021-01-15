//! A sorted set with persistent storage.
use sled::transaction::{TransactionResult as Result, Transactional};
use sled::{Db, IVec, Iter, Tree};
use std::ops::{Bound, RangeBounds};
use uuid::Uuid;

/// A sorted set with persistent storage.
///
/// Sorted sets are composed of unique, non-repeating elements, sorted by `u64`
/// number called score.
#[derive(Clone, Debug)]
pub struct SortedSet {
  /// Tree storing <hashed key + score + member> encoded in the tree's key,
  /// serving as a skip list.
  skiplist: Tree,
  /// Tree mapping members to their scores, simply a hash table.
  members: Tree,
  /// UUID v5 produced from sorted set key.
  uuid: IVec,
}

type BoundLimit = [u8; SortedSet::SKIPLIST_PREFIX_LEN];

impl SortedSet {
  /// UUID length.
  const UUID_LEN: usize = 16;
  /// Prefix length of skiplist.
  const SKIPLIST_PREFIX_LEN: usize = 24;
  /// Skiplist tree name.
  const SKIPLIST_TREE_NAME: &'static str = "__sorted_set_skiplist";
  /// Members tree name.
  const MEMBERS_TREE_NAME: &'static str = "__sorted_set_members";

  /// Open the sorted set with the specified key.
  pub fn open<K>(db: &Db, key: K) -> Result<Self>
  where
    K: AsRef<[u8]>,
  {
    let skiplist = db.open_tree(Self::SKIPLIST_TREE_NAME)?;
    let members = db.open_tree(Self::MEMBERS_TREE_NAME)?;
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_ref())
      .as_bytes()
      .into();

    Ok(Self {
      skiplist,
      uuid,
      members,
    })
  }

  /// Add one member with the specified score. If specified member is already
  /// a member of the sorted set, the score is updated and the element reinserted
  /// at the right position to ensure the correct ordering.
  pub fn add<M>(&self, score: u64, member: M) -> Result<()>
  where
    M: AsRef<[u8]>,
  {
    let tx_group = (&self.skiplist, &self.members);
    tx_group.transaction(|(skiplist, members)| {
      let encoded_member = self.encode_members_key(member.as_ref());
      if let Some(old_score) = members.get(&encoded_member)? {
        // If the member already exists, remove it before new insertion
        let encoded_key = self.encode_skiplist_key(old_score.as_ref(), member.as_ref());
        skiplist.remove(encoded_key)?;
      }
      let encoded_score = &score.to_be_bytes();
      let encoded_key = self.encode_skiplist_key(encoded_score, member.as_ref());
      skiplist.insert(encoded_key, &[])?;
      members.insert(encoded_member, encoded_score)?;
      Ok(())
    })
  }

  /// Return all the elements in the sorted set with a score between `range`.
  /// The elements are considered to be sorted from low to high scores.
  pub fn range_by_score<R>(&self, range: R) -> RangeSet
  where
    R: RangeBounds<u64>,
  {
    let bytes_range = self.to_bytes_range(range);
    self.skiplist.range(bytes_range).into()
  }

  /// Remove the specified member from the sorted set, returning `true` when
  /// the member existed and was removed. If member is non-existant the result
  /// is `false`.
  pub fn remove<M>(&self, member: M) -> Result<bool>
  where
    M: AsRef<[u8]>,
  {
    let tx_group = (&self.skiplist, &self.members);
    tx_group.transaction(|(skiplist, members)| {
      let encoded_member = self.encode_members_key(member.as_ref());
      if let Some(score) = members.remove(encoded_member)? {
        let encoded_key = self.encode_skiplist_key(score.as_ref(), member.as_ref());
        skiplist.remove(encoded_key)?;
        Ok(true)
      } else {
        Ok(false)
      }
    })
  }

  fn encode_skiplist_key(&self, score: &[u8], member: &[u8]) -> Vec<u8> {
    let mut key = self.uuid.to_vec();
    key.extend(&score[..]);
    key.extend(member);
    key
  }

  fn encode_members_key(&self, member: &[u8]) -> Vec<u8> {
    let mut key = self.uuid.to_vec();
    key.extend(member);
    key
  }

  fn to_bytes_range<R>(&self, range: R) -> (Bound<BoundLimit>, Bound<BoundLimit>)
  where
    R: RangeBounds<u64>,
  {
    let uuid_slice = self.uuid.as_ref();
    let start = match range.start_bound() {
      Bound::Excluded(score) => {
        let bound = Self::create_bound_limit(uuid_slice, score);
        Bound::Excluded(bound)
      }
      Bound::Included(score) => {
        let bound = Self::create_bound_limit(uuid_slice, score);
        Bound::Included(bound)
      }
      Bound::Unbounded => {
        // Unbounded start has zeroed score
        let bound = Self::create_bound_limit(uuid_slice, &u64::MIN);
        Bound::Included(bound)
      }
    };

    let end = match range.end_bound() {
      Bound::Excluded(score) => {
        let bound = Self::create_bound_limit(uuid_slice, score);
        Bound::Excluded(bound)
      }
      Bound::Included(score) => {
        // We add one to an included score, so that the last member is included
        // in the returned iterator
        if let Some(s) = score.checked_add(1) {
          let bound = Self::create_bound_limit(uuid_slice, &s);
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
    let mut uuid: [u8; Self::UUID_LEN] = self.uuid.as_ref().try_into().expect("uuid into array");
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

/// Iterator with members returned after calling SortedSet::range_by_score.
pub struct RangeSet {
  inner: Iter,
}

impl From<Iter> for RangeSet {
  fn from(iter: Iter) -> Self {
    Self { inner: iter }
  }
}

impl Iterator for RangeSet {
  type Item = Result<IVec>;

  fn next(&mut self) -> Option<Self::Item> {
    self.inner.next().map(|res| {
      // Extract member from the key
      res
        .map(|(key, _)| {
          key.subslice(
            SortedSet::SKIPLIST_PREFIX_LEN,
            key.len() - SortedSet::SKIPLIST_PREFIX_LEN,
          )
        })
        .map_err(|e| e.into())
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn sorted_set_range_by_score() {
    let db = sled::Config::new().temporary(true).open().unwrap();
    let set_a = SortedSet::open(&db, "set_a").unwrap();
    let set_b = SortedSet::open(&db, "set_b").unwrap();

    // Add to first set
    set_a.add(100, "Elephant").unwrap();
    set_a.add(50, "Bear").unwrap();
    set_a.add(20, "Cat").unwrap();
    set_a.add(u64::MAX, "Bigger Elephant").unwrap();

    // Add to second set
    set_b.add(100, "Truck").unwrap();
    set_b.add(50, "Sedan").unwrap();

    // Get a subset
    let mut range = set_a.range_by_score(20..=50);
    assert_eq!(range.next(), Some(Ok(IVec::from("Cat"))));
    assert_eq!(range.next(), Some(Ok(IVec::from("Bear"))));
    assert_eq!(range.next(), None);

    // Exclude last member
    let mut range = set_a.range_by_score(100..u64::MAX);
    assert_eq!(range.next(), Some(Ok(IVec::from("Elephant"))));
    assert_eq!(range.next(), None);

    // Include last member
    let mut range = set_a.range_by_score(100..=u64::MAX);
    assert_eq!(range.next(), Some(Ok(IVec::from("Elephant"))));
    assert_eq!(range.next(), Some(Ok(IVec::from("Bigger Elephant"))));
    assert_eq!(range.next(), None);

    // Get all members with an unbounded range
    let mut range = set_b.range_by_score(..);
    assert_eq!(range.next(), Some(Ok(IVec::from("Sedan"))));
    assert_eq!(range.next(), Some(Ok(IVec::from("Truck"))));
    assert_eq!(range.next(), None);
  }

  #[test]
  fn sorted_set_unique_member() {
    let db = sled::Config::new().temporary(true).open().unwrap();
    let set_a = SortedSet::open(&db, "set_a").unwrap();
    set_a.add(100, "Elephant").unwrap();
    // Update the same member with a different score
    set_a.add(2000, "Elephant").unwrap();

    // Get the whole set
    let mut range = set_a.range_by_score(..);
    assert_eq!(range.next(), Some(Ok(IVec::from("Elephant"))));
    assert_eq!(range.next(), None);
    assert_eq!(set_a.members.len(), 1);
  }

  #[test]
  fn sorted_set_remove_member() {
    let db = sled::Config::new().temporary(true).open().unwrap();
    let set_a = SortedSet::open(&db, "set_a").unwrap();
    set_a.add(2000, "Elephant").unwrap();

    assert_eq!(Ok(true), set_a.remove("Elephant"));
    assert!(set_a.members.is_empty());
    assert!(set_a.skiplist.is_empty());
  }
}
