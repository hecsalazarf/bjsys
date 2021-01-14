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

impl SortedSet {
  const PREFIX_LEN: usize = 24;

  /// Open the sorted set with the specified key.
  pub fn open<K>(db: &Db, key: K) -> Result<Self>
  where
    K: AsRef<[u8]>,
  {
    let skiplist = db.open_tree("__sorted_set_skiplist")?;
    let members = db.open_tree("__sorted_set_members")?;
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

  fn to_bytes_range<R>(&self, range: R) -> (Bound<Vec<u8>>, Bound<Vec<u8>>)
  where
    R: RangeBounds<u64>,
  {
    let mut bound = self.uuid.to_vec();
    let start = match range.start_bound() {
      Bound::Excluded(score) => {
        bound.extend(&score.to_be_bytes());
        Bound::Excluded(bound)
      }
      Bound::Included(score) => {
        bound.extend(&score.to_be_bytes());
        Bound::Included(bound)
      }
      Bound::Unbounded => {
        // Unbounded start has zeroed score
        let score = u64::MIN.to_be_bytes();
        bound.extend(&score);
        Bound::Included(bound)
      }
    };

    let mut bound = self.uuid.to_vec();
    let end = match range.end_bound() {
      Bound::Excluded(score) => {
        bound.extend(&score.to_be_bytes());
        Bound::Excluded(bound)
      }
      Bound::Included(score) => {
        // We add one to an included score, so that the last member is included
        // in the returned iterator
        if let Some(s) = score.checked_add(1) {
          bound.extend(&s.to_be_bytes());
          Bound::Included(bound)
        } else {
          // However, the max score can overflow. Such case requires to increment
          // the UUID to cover the whole set.
          // Only the last byte of the UUID is incremented.
          let last = bound.as_mut_slice().last_mut().unwrap();
          if let Some(l) = last.checked_add(1) {
            *last = l;
            Bound::Included(bound)
          } else {
            // But even the UUID can overflow, meaning we reached the sorted set max
            // UUID. Very improbable scenario, but theoretically posible. Just in case
            Bound::Unbounded
          }
        }
      }
      Bound::Unbounded => {
        let last = bound.as_mut_slice().last_mut().unwrap();
        if let Some(l) = last.checked_add(1) {
          *last = l;
          Bound::Included(bound)
        } else {
          Bound::Unbounded
        }
      }
    };

    (start, end)
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
        .map(|(key, _)| key.subslice(SortedSet::PREFIX_LEN, key.len() - SortedSet::PREFIX_LEN))
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
}
