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
    let set = SortedSet::open(&db, "set_a").unwrap();

    set.add(100, "Elephant").unwrap();
    set.add(50, "Bear").unwrap();
    set.add(20, "Cat").unwrap();
    set.add(101, "Bigger Elephant").unwrap();

    let mut range = set.range_by_score(20..=100);
    assert_eq!(range.next(), Some(Ok(IVec::from("Cat"))));
    assert_eq!(range.next(), Some(Ok(IVec::from("Bear"))));
    assert_eq!(range.next(), Some(Ok(IVec::from("Elephant"))));
    assert_eq!(range.next(), None);
  }
}
