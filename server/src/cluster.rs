use crate::stub::raft2::{ClientRequest, ClientResponse};
use async_raft::raft::{Entry, MembershipConfig};
use async_raft::storage::{CurrentSnapshotData, HardState, InitialState, RaftStorage};
use embed::{Database, Env, Store, Transaction};
use std::path::{Path, PathBuf};
use tokio::fs::File;

type LogEntry = Entry<ClientRequest>;

pub struct Storage {
  id: u64,
  log: Store<LogEntry>,
  meta: Database,
  snap_path: PathBuf,
  env: Env,
}

impl Storage {
  pub fn open(path: &Path) -> embed::Result<Self> {
    // TODO: Pass configuration
    let mut env_flags = embed::EnvironmentFlags::empty();
    env_flags.insert(embed::EnvironmentFlags::NO_SYNC);
    env_flags.insert(embed::EnvironmentFlags::NO_SUB_DIR);
    let mut env_builder = embed::Environment::new();

    env_builder
      // 1Gb of data
      .set_map_size(1024 * 1024 * 1024)
      // No particular reason, currently less than 10 Dbs are needed
      .set_max_dbs(10)
      .set_flags(env_flags);

    let env = embed::Manager::singleton()
      .write()
      .expect("manager write guard")
      .get_or_init(env_builder, path.join("rlog"))?;

    let log = Store::open(&env, "__cluster_log")?;
    let meta = env.create_db(Some("__cluster_meta"), embed::DatabaseFlags::default())?;
    let id = Self::retrieve_id(&env, meta)?;
    let snap_path = path.join("snapshot");

    let storage = Self {
      env,
      id,
      log,
      meta,
      snap_path,
    };

    Ok(storage)
  }

  fn retrieve_id(env: &Env, meta: Database) -> embed::Result<u64> {
    use embed::{TransactionExt, TransactionRwExt};
    use rand::Rng;
    let mut tx = env.begin_rw_txn()?;
    if let Some(id) = tx.get_data(meta, "NODE_ID")? {
      tracing::debug!("Node ID found, retrieving from storage");
      return Ok(id);
    }

    tracing::debug!("Node ID not found, generating one");
    let id: u64 = rand::thread_rng().gen();
    tx.put_data(meta, "NODE_ID", &id, embed::WriteFlags::default())?;
    tx.commit()?;

    Ok(id)
  }

  fn last_applied_log<T>(&self, txn: &T) -> embed::Result<u64>
  where
    T: Transaction,
  {
    use embed::TransactionExt;
    let last = txn
      .get_data(self.meta, "LAST_APPLIED_LOG")?
      .unwrap_or_default();
    Ok(last)
  }

  fn index_and_term<T>(&self, txn: &T) -> embed::Result<(u64, u64)>
  where
    T: Transaction,
  {
    let index_term = self
      .log
      .last(txn)?
      .map(|(_, entry)| (entry.index, entry.term))
      .unwrap_or_default();

    Ok(index_term)
  }

  fn log_entries<T>(&self, txn: &T, start: u64, stop: u64) -> embed::Result<Vec<LogEntry>>
  where
    T: Transaction,
  {
    let iter = self.log.iter_from(txn, start.to_be_bytes())?;

    let tw = iter
      .take_while(|res| {
        if let Ok((key, _)) = res {
          return *key < &stop.to_be_bytes()[..];
        }
        true
      })
      .map(|res| res.map(|(_, v)| v));
    tw.collect()
  }

  fn membership_config<T>(&self, txn: &T) -> embed::Result<MembershipConfig>
  where
    T: Transaction,
  {
    use async_raft::raft::EntryPayload;

    let cfg_opt = self.log.iter_end_backwards(txn)?.find_map(|res| {
      if let Ok((_, val)) = res {
        match val.payload {
          EntryPayload::ConfigChange(change) => Some(change.membership),
          EntryPayload::SnapshotPointer(pointer) => Some(pointer.membership),
          _ => None,
        }
      } else {
        None
      }
    });

    Ok(cfg_opt.unwrap_or(MembershipConfig::new_initial(self.id)))
  }

  fn hard_state<T>(&self, txn: &T) -> embed::Result<Option<HardState>>
  where
    T: Transaction,
  {
    use embed::TransactionExt;
    txn.get_data(self.meta, "HARD_STATE")
  }
}

#[tonic::async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for Storage {
  type Snapshot = File;
  type ShutdownError = embed::Error;

  async fn get_membership_config(&self) -> anyhow::Result<MembershipConfig> {
    let txn = self.env.begin_ro_txn()?;
    let config = self.membership_config(&txn)?;
    Ok(config)
  }

  async fn get_initial_state(&self) -> anyhow::Result<InitialState> {
    let state;
    let txn = self.env.begin_ro_txn()?;
    if let Some(hard_state) = self.hard_state(&txn)? {
      let membership = self.membership_config(&txn)?;
      let (last_log_index, last_log_term) = self.index_and_term(&txn)?;
      let last_applied_log = self.last_applied_log(&txn)?;
      state = InitialState {
        last_log_index,
        last_log_term,
        last_applied_log,
        hard_state,
        membership,
      };
    } else {
      txn.abort();
      state = InitialState::new_initial(self.id);
      self.save_hard_state(&state.hard_state).await?;
    }

    Ok(state)
  }

  async fn save_hard_state(&self, hs: &HardState) -> anyhow::Result<()> {
    use embed::{TransactionRwExt, WriteFlags};
    let mut txn = self.env.begin_rw_txn_async().await?;
    txn.put_data(self.meta, "HARD_STATE", hs, WriteFlags::empty())?;
    txn.commit()?;
    Ok(())
  }

  async fn get_log_entries(&self, start: u64, stop: u64) -> anyhow::Result<Vec<LogEntry>> {
    if start > stop {
      tracing::error!("Failed to get log, start[{}] > stop[{}]", start, stop);
      return Ok(Vec::new());
    }
    let txn = self.env.begin_ro_txn()?;
    let entries = self.log_entries(&txn, start, stop)?;

    Ok(entries)
  }

  async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> anyhow::Result<()> {
    if stop.as_ref().map(|stop| &start > stop).unwrap_or(false) {
      tracing::error!("invalid request, start > stop");
      return Ok(());
    }
    let stop = stop.map(|s| s.to_be_bytes());
    let mut txn = self.env.begin_rw_txn_async().await?;
    let iter = self.log.iter_from(&txn, start.to_be_bytes())?;
    for k in iter {
      let key = k?.0;
      if let Some(ref s) = stop {
        if key >= &s[..] {
          break;
        }
      }
      self.log.delete(&mut txn, key)?;
    }

    txn.commit()?;
    Ok(())
  }

  async fn append_entry_to_log(&self, entry: &LogEntry) -> anyhow::Result<()> {
    let mut txn = self.env.begin_rw_txn_async().await?;
    self.log.put(&mut txn, entry.index.to_be_bytes(), entry)?;
    txn.commit()?;
    Ok(())
  }

  async fn replicate_to_log(&self, entries: &[LogEntry]) -> anyhow::Result<()> {
    let mut txn = self.env.begin_rw_txn_async().await?;
    for entry in entries {
      self.log.put(&mut txn, entry.index.to_be_bytes(), entry)?;
    }
    txn.commit()?;
    Ok(())
  }

  async fn apply_entry_to_state_machine(
    &self,
    _index: &u64,
    _data: &ClientRequest,
  ) -> anyhow::Result<ClientResponse> {
    todo!()
  }

  async fn replicate_to_state_machine(
    &self,
    _entries: &[(&u64, &ClientRequest)],
  ) -> anyhow::Result<()> {
    todo!()
  }

  async fn do_log_compaction(&self) -> anyhow::Result<CurrentSnapshotData<File>> {
    todo!()
  }

  async fn create_snapshot(&self) -> anyhow::Result<(String, Box<File>)> {
    let file = File::create(&self.snap_path).await?;
    Ok((String::new(), Box::new(file)))
  }

  async fn finalize_snapshot_installation(
    &self,
    _index: u64,
    _term: u64,
    _delete_through: Option<u64>,
    _id: String,
    _snapshot: Box<File>,
  ) -> anyhow::Result<()> {
    todo!()
  }

  async fn get_current_snapshot(&self) -> anyhow::Result<Option<CurrentSnapshotData<File>>> {
    todo!()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn temp_dir() -> tempfile::TempDir {
    tempfile::Builder::new()
      .prefix("cluster")
      .tempdir()
      .expect("tmp dir")
  }

  fn create_membership() -> MembershipConfig {
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(1);
    set.insert(2);
    set.insert(3);
    MembershipConfig {
      members: set.clone(),
      members_after_consensus: Some(set),
    }
  }

  fn normal_entry(index: u64, term: u64) -> LogEntry {
    use crate::stub::raft2::client_request::Request;
    use async_raft::raft::{EntryNormal, EntryPayload};
    use common::service::CreateRequest;

    let request = ClientRequest {
      client: "Client".into(),
      serial: 15651,
      request: Some(Request::Create(CreateRequest::default())),
    };

    LogEntry {
      term,
      index,
      payload: EntryPayload::Normal(EntryNormal { data: request }),
    }
  }

  fn config_entry(membership: MembershipConfig, index: u64, term: u64) -> LogEntry {
    use async_raft::raft::{EntryConfigChange, EntryPayload};
    LogEntry {
      term,
      index,
      payload: EntryPayload::ConfigChange(EntryConfigChange { membership }),
    }
  }

  #[tokio::test]
  async fn hard_state() -> anyhow::Result<()> {
    let tmp_dir = temp_dir();
    let storage = Storage::open(tmp_dir.path())?;
    let hs = HardState {
      current_term: 3,
      voted_for: Some(5416541),
    };
    storage.save_hard_state(&hs).await?;

    let txn = storage.env.begin_ro_txn()?;
    assert_eq!(Ok(Some(hs)), storage.hard_state(&txn));
    Ok(())
  }

  #[tokio::test]
  async fn initial_state_empty() -> anyhow::Result<()> {
    let tmp_dir = temp_dir();
    let storage = Storage::open(tmp_dir.path())?;
    let initial_state = storage.get_initial_state().await?;
    let empty_state = InitialState::new_initial(1);
    assert_eq!(empty_state.last_applied_log, initial_state.last_applied_log);
    assert_eq!(empty_state.last_log_index, initial_state.last_log_index);
    assert_eq!(empty_state.last_log_term, initial_state.last_log_term);
    Ok(())
  }

  #[tokio::test]
  async fn initial_state_filled() -> anyhow::Result<()> {
    let tmp_dir = temp_dir();
    let storage = Storage::open(tmp_dir.path())?;
    storage.get_initial_state().await?;

    let membership = create_membership();
    let config = config_entry(membership.clone(), 5, 1);
    storage.append_entry_to_log(&config).await?;
    let entry = normal_entry(6, 1);
    storage.append_entry_to_log(&entry).await?;

    let initial_state = storage.get_initial_state().await?;
    assert_eq!(0, initial_state.last_applied_log);
    assert_eq!(entry.index, initial_state.last_log_index);
    assert_eq!(entry.term, initial_state.last_log_term);
    assert_eq!(membership, initial_state.membership);

    Ok(())
  }

  #[tokio::test]
  async fn log_entries() -> anyhow::Result<()> {
    const N: u64 = 6;
    let mut entries = Vec::with_capacity(N as usize);
    for i in 0..N {
      let entry = normal_entry(i, 1);
      entries.push(entry);
    }

    let tmp_dir = temp_dir();
    let storage = Storage::open(tmp_dir.path())?;
    storage.replicate_to_log(&entries).await?;

    assert_eq!(entries, storage.get_log_entries(0, N).await?);
    assert_eq!(&entries[1..3], storage.get_log_entries(1, 3).await?);

    storage.delete_logs_from(0, Some(3)).await?;
    assert_eq!(&entries[3..], storage.get_log_entries(0, N).await?);
    storage.delete_logs_from(0, None).await?;
    assert!(storage.get_log_entries(0, N).await?.is_empty());

    Ok(())
  }
}
