use sled::transaction::TransactionResult as Result;
use sled::{Db, IVec, Iter, Tree};
use std::ops::{Bound, RangeBounds};
use uuid::Uuid;

pub struct SortedSet {
  data: Tree,
  uuid: IVec,
}

impl SortedSet {
  const PREFIX_LEN: usize = 24;

  pub fn open(db: &Db, key: &str) -> Result<Self> {
    let data = db.open_tree(key)?;
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_ref())
      .as_bytes()
      .into();

    Ok(Self { data, uuid })
  }

  pub fn add<M>(&self, score: u64, member: M) -> Result<()>
  where
    M: AsRef<[u8]>,
  {
    let encoded_key = self.encode_key(score.to_be_bytes(), member.as_ref());
    self.data.insert(encoded_key, &[])?;
    Ok(())
  }

  pub fn range_by_score<R>(&self, range: R) -> RangeSet
  where
    R: RangeBounds<u64>,
  {
    let bytes_range = self.to_bytes_range(range);
    self.data.range(bytes_range).into()
  }

  fn encode_key(&self, score: [u8; 8], member: &[u8]) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend(self.uuid.as_ref());
    key.extend(&score[..]);
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
        if let Some(s) = score.checked_add(1) {
          bound.extend(&s.to_be_bytes());
          Bound::Included(bound)
        } else {
          let last = bound.as_mut_slice().last_mut().unwrap();
          if let Some(l) = last.checked_add(1) {
            *last = l;
            Bound::Included(bound)
          } else {
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
}
