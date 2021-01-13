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
    let start = self.score_range(range.start_bound());
    let end = self.score_range(range.end_bound());
    (start, end)
  }

  fn score_range(&self, bound: Bound<&u64>) -> Bound<Vec<u8>> {
    let mut bounded = Vec::<u8>::new();
    bounded.extend(self.uuid.as_ref());
    match bound {
      Bound::Excluded(b) => {
        let score = b.to_be_bytes();
        bounded.extend(&score[..]);
        Bound::Excluded(bounded)
      }
      Bound::Included(b) => {
        let score = b.to_be_bytes();
        bounded.extend(&score[..]);
        Bound::Included(bounded)
      }
      Bound::Unbounded => Bound::Unbounded,
    }
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

    // FIX: We should not add one to get the upper bound element
    let mut range = set.range_by_score(20..=101);
    assert_eq!(range.next(), Some(Ok(IVec::from("Cat"))));
    assert_eq!(range.next(), Some(Ok(IVec::from("Bear"))));
    assert_eq!(range.next(), Some(Ok(IVec::from("Elephant"))));
  }
}
