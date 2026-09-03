use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::Any;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

extern crate self as cultcache_rs;

pub use cultcache_rs_derive::DatabaseEntry;

pub trait DatabaseEntry: Serialize + DeserializeOwned + Clone + Send + 'static {
    const TYPE: &'static str;
    const SCHEMA_NAME: &'static str = "DatabaseEntry";
}

pub trait CultCacheRegistry {
    fn register_entries(&self, cache: &mut CultCache) -> Result<()>;
}

pub trait SoaDocument: DatabaseEntry {
    fn soa_columns(rows: &[Self]) -> BTreeMap<&'static str, CultSoaColumnValues>;
}

pub struct CultSoaColumnValues {
    type_name: &'static str,
    values: Box<dyn Any + Send>,
}

impl CultSoaColumnValues {
    pub fn new<TValue: Clone + Send + 'static>(values: Vec<TValue>) -> Self {
        Self {
            type_name: std::any::type_name::<TValue>(),
            values: Box::new(values),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CultSoaColumn<TValue> {
    name: String,
    values: Vec<TValue>,
}

impl<TValue> CultSoaColumn<TValue> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn values(&self) -> &[TValue] {
        &self.values
    }
}

pub struct CultSoaTable<TDocument> {
    keys: Vec<String>,
    documents: Vec<TDocument>,
    columns: BTreeMap<String, CultSoaColumnValues>,
    _document: PhantomData<TDocument>,
}

impl<TDocument> CultSoaTable<TDocument> {
    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    pub fn documents(&self) -> &[TDocument] {
        &self.documents
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn column<TValue: Clone + 'static>(&self, name: &str) -> Result<CultSoaColumn<TValue>> {
        let Some(column) = self.columns.get(name) else {
            return Err(anyhow!("SoA table has no column {name:?}"));
        };
        let Some(values) = column.values.downcast_ref::<Vec<TValue>>() else {
            return Err(anyhow!(
                "SoA table column {:?} stores {}, not {}",
                name,
                column.type_name,
                std::any::type_name::<TValue>()
            ));
        };
        Ok(CultSoaColumn {
            name: name.to_string(),
            values: values.clone(),
        })
    }
}

#[macro_export]
macro_rules! cultcache_registry {
    ($name:ident { $($entry:ty),* $(,)? }) => {
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name;

        impl $crate::CultCacheRegistry for $name {
            fn register_entries(&self, cache: &mut $crate::CultCache) -> ::anyhow::Result<()> {
                $(
                    cache.register_entry_type::<$entry>()?;
                )*
                Ok(())
            }
        }
    };
}

#[macro_export]
macro_rules! cultcache_soa {
    ($entry:ty { $($column:literal => $extractor:expr),* $(,)? }) => {
        impl $crate::SoaDocument for $entry {
            fn soa_columns(rows: &[Self]) -> ::std::collections::BTreeMap<&'static str, $crate::CultSoaColumnValues> {
                let mut columns = ::std::collections::BTreeMap::new();
                $(
                    let values = rows.iter().map($extractor).collect::<Vec<_>>();
                    columns.insert($column, $crate::CultSoaColumnValues::new(values));
                )*
                columns
            }
        }
    };
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CultCacheEnvelope {
    pub key: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub stored_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CultCacheSoaTable {
    pub keys: Vec<String>,
    pub types: Vec<String>,
    pub payloads: Vec<Vec<u8>>,
    pub stored_ats: Vec<String>,
    pub schema_ids: Vec<Option<String>>,
}

impl CultCacheSoaTable {
    pub fn from_envelopes(entries: impl IntoIterator<Item = CultCacheEnvelope>) -> Self {
        let mut table = Self::default();
        for entry in entries {
            table.keys.push(entry.key);
            table.types.push(entry.r#type);
            table.payloads.push(entry.payload);
            table.stored_ats.push(entry.stored_at);
            table.schema_ids.push(entry.schema_id);
        }
        table
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn validate(&self) -> Result<()> {
        let len = self.keys.len();
        for (name, candidate) in [
            ("types", self.types.len()),
            ("payloads", self.payloads.len()),
            ("storedAts", self.stored_ats.len()),
            ("schemaIds", self.schema_ids.len()),
        ] {
            if candidate != len {
                return Err(anyhow!(
                    "CultCache SoA column {name} has length {candidate}, expected {len}"
                ));
            }
        }
        for (index, key) in self.keys.iter().enumerate() {
            if key.trim().is_empty() {
                return Err(anyhow!("CultCache SoA row {index} has an empty key"));
            }
            if self.types[index].trim().is_empty() {
                return Err(anyhow!("CultCache SoA row {index} has an empty type"));
            }
            if self.stored_ats[index].trim().is_empty() {
                return Err(anyhow!("CultCache SoA row {index} has an empty stored_at"));
            }
        }
        Ok(())
    }

    pub fn into_envelopes(self) -> Result<Vec<CultCacheEnvelope>> {
        self.validate()?;
        let Self {
            keys,
            types,
            payloads,
            stored_ats,
            schema_ids,
        } = self;
        Ok(keys
            .into_iter()
            .zip(types)
            .zip(payloads)
            .zip(stored_ats)
            .zip(schema_ids)
            .map(
                |((((key, r#type), payload), stored_at), schema_id)| CultCacheEnvelope {
                    key,
                    r#type,
                    payload,
                    stored_at,
                    schema_id,
                },
            )
            .collect())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PersistedStoreSnapshot(
    String,
    Vec<PersistedSchemaCatalogEntry>,
    Vec<PersistedRecord>,
);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PersistedSchemaCatalogEntry(
    String,
    String,
    String,
    String,
    String,
    Vec<String>,
    Vec<PersistedSchemaCatalogMember>,
);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PersistedSchemaCatalogMember(
    u32,
    String,
    String,
    bool,
    bool,
    Option<String>,
    bool,
    Option<String>,
);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PersistedRecord(
    String,
    String,
    String,
    #[serde(with = "serde_bytes")] Vec<u8>,
);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushAllOptions {
    pub soft: bool,
}

pub trait CacheBackingStore: Send {
    fn pull_all(&self) -> Result<Vec<CultCacheEnvelope>>;
    fn push(&mut self, entry: &CultCacheEnvelope) -> Result<()>;
    fn delete(&mut self, entry: &CultCacheEnvelope) -> Result<()>;

    fn push_all(&mut self, entries: &[CultCacheEnvelope], _options: PushAllOptions) -> Result<()> {
        let existing = self.pull_all()?;
        for entry in existing {
            self.delete(&entry)?;
        }
        for entry in entries {
            self.push(entry)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SingleFileMessagePackBackingStore {
    path: PathBuf,
}

impl SingleFileMessagePackBackingStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Holds the pre-created sibling lock shared while a read-only consumer
    /// evaluates and acts on one snapshot. The reader never creates or writes
    /// either file. Writers using the sibling lock cannot replace the snapshot
    /// until `action` returns.
    ///
    /// Releasing the lock is RAII cleanup. A release failure therefore cannot
    /// rewrite the completed action's result; consumers that need to observe
    /// that cleanup failure can use
    /// [`Self::with_read_only_shared_snapshot_and_unlock_diagnostic`].
    pub fn with_read_only_shared_snapshot<T>(
        &self,
        action: impl FnOnce(Vec<CultCacheEnvelope>) -> Result<T>,
    ) -> Result<T> {
        self.with_read_only_shared_snapshot_and_unlock_diagnostic(action, |_| {})
    }

    /// The diagnostic-bearing form of
    /// [`Self::with_read_only_shared_snapshot`]. `on_unlock_failure` observes a
    /// cleanup failure separately and never changes the action result.
    pub fn with_read_only_shared_snapshot_and_unlock_diagnostic<T>(
        &self,
        action: impl FnOnce(Vec<CultCacheEnvelope>) -> Result<T>,
        on_unlock_failure: impl FnOnce(anyhow::Error),
    ) -> Result<T> {
        self.with_read_only_shared_snapshot_using(
            action,
            |lock| {
                fs2::FileExt::unlock(lock)
                    .with_context(|| format!("failed to unlock {}", self.lock_path().display()))
            },
            on_unlock_failure,
        )
    }

    fn with_read_only_shared_snapshot_using<T, U, D>(
        &self,
        action: impl FnOnce(Vec<CultCacheEnvelope>) -> Result<T>,
        unlock: U,
        on_unlock_failure: D,
    ) -> Result<T>
    where
        U: FnOnce(&File) -> Result<()>,
        D: FnOnce(anyhow::Error),
    {
        let lock_path = self.lock_path();
        let lock = OpenOptions::new()
            .read(true)
            .open(&lock_path)
            .with_context(|| format!("failed to open pre-created {}", lock_path.display()))?;
        fs2::FileExt::lock_shared(&lock)
            .with_context(|| format!("failed to lock {}", lock_path.display()))?;
        let guard = ReadOnlySharedLockGuard::new(lock, unlock, on_unlock_failure);
        let result = self.read_all_unlocked().and_then(action);
        drop(guard);
        result
    }

    /// Replaces one exact envelope under the store's cross-process exclusive
    /// lock. A changed or missing expected envelope returns false without a write.
    pub fn compare_and_swap_entry(
        &self,
        expected: &CultCacheEnvelope,
        replacement: CultCacheEnvelope,
    ) -> Result<bool> {
        self.with_exclusive_lock(|| {
            let mut entries = self.read_all_unlocked()?;
            let Some(index) = entries
                .iter()
                .position(|candidate| entry_id(candidate) == entry_id(expected))
            else {
                return Ok(false);
            };
            if entries[index] != *expected {
                return Ok(false);
            }
            entries[index] = replacement;
            entries.sort_by_key(entry_id);
            self.write_all_unlocked(&entries)?;
            Ok(true)
        })
    }

    /// Inserts one envelope only when its polymorphic type/key identity is
    /// absent, under the same cross-process lock used by all single-file writes.
    pub fn insert_entry_if_absent(&self, entry: CultCacheEnvelope) -> Result<bool> {
        self.with_exclusive_lock(|| {
            let mut entries = self.read_all_unlocked()?;
            if entries
                .iter()
                .any(|candidate| entry_id(candidate) == entry_id(&entry))
            {
                return Ok(false);
            }
            entries.push(entry);
            entries.sort_by_key(entry_id);
            self.write_all_unlocked(&entries)?;
            Ok(true)
        })
    }

    /// Atomically replaces an exact set of existing envelopes and inserts any
    /// additional companion envelopes. Every precondition is checked beneath
    /// the same cross-process lock as the single snapshot write.
    pub fn compare_and_swap_batch(
        &self,
        expected: &[CultCacheEnvelope],
        replacements: Vec<CultCacheEnvelope>,
    ) -> Result<bool> {
        if replacements.is_empty() {
            return Err(anyhow!(
                "conditional batch requires a non-empty replacement set"
            ));
        }
        let expected_ids = unique_batch_ids(expected, "expected")?;
        let replacement_ids = unique_batch_ids(&replacements, "replacement")?;
        if !expected_ids.is_subset(&replacement_ids) {
            return Err(anyhow!(
                "conditional batch must replace every expected identity"
            ));
        }

        self.with_exclusive_lock(|| {
            let mut entries = self.read_all_unlocked()?;
            for expected_entry in expected {
                let Some(current) = entries
                    .iter()
                    .find(|candidate| entry_id(candidate) == entry_id(expected_entry))
                else {
                    return Ok(false);
                };
                if current != expected_entry {
                    return Ok(false);
                }
            }
            for replacement in &replacements {
                let identity = entry_id(replacement);
                if !expected_ids.contains(&identity)
                    && entries
                        .iter()
                        .any(|candidate| entry_id(candidate) == identity)
                {
                    return Ok(false);
                }
            }

            entries.retain(|candidate| !expected_ids.contains(&entry_id(candidate)));
            entries.extend(replacements);
            entries.sort_by_key(entry_id);
            self.write_all_unlocked(&entries)?;
            Ok(true)
        })
    }

    /// Atomically appends companions only when the complete backing-store
    /// snapshot is byte-for-byte unchanged. Unlike identity-scoped CAS, this
    /// fences predicate cardinality: a concurrently inserted unknown identity
    /// makes the expected snapshot stale.
    pub fn append_if_snapshot_unchanged(
        &self,
        expected_snapshot: &[CultCacheEnvelope],
        additions: Vec<CultCacheEnvelope>,
    ) -> Result<bool> {
        if additions.is_empty() {
            return Err(anyhow!("conditional snapshot append requires additions"));
        }
        unique_batch_ids(expected_snapshot, "expected snapshot")?;
        let addition_ids = unique_batch_ids(&additions, "snapshot additions")?;
        self.with_exclusive_lock(|| {
            let mut current = self.read_all_unlocked()?;
            let mut expected = expected_snapshot.to_vec();
            current.sort_by_key(entry_id);
            expected.sort_by_key(entry_id);
            if current != expected {
                return Ok(false);
            }
            if current
                .iter()
                .any(|row| addition_ids.contains(&entry_id(row)))
            {
                return Ok(false);
            }
            current.extend(additions);
            current.sort_by_key(entry_id);
            self.write_all_unlocked(&current)?;
            Ok(true)
        })
    }

    /// Atomically deletes an exact set of envelopes. Any changed or missing
    /// member refuses the entire deletion without a partial write.
    pub fn delete_batch_if_unchanged(&self, expected: &[CultCacheEnvelope]) -> Result<bool> {
        if expected.is_empty() {
            return Err(anyhow!(
                "conditional delete requires a non-empty expected set"
            ));
        }
        let expected_ids = unique_batch_ids(expected, "delete expected")?;
        self.with_exclusive_lock(|| {
            let mut entries = self.read_all_unlocked()?;
            for expected_entry in expected {
                let Some(current) = entries
                    .iter()
                    .find(|candidate| entry_id(candidate) == entry_id(expected_entry))
                else {
                    return Ok(false);
                };
                if current != expected_entry {
                    return Ok(false);
                }
            }
            entries.retain(|candidate| !expected_ids.contains(&entry_id(candidate)));
            self.write_all_unlocked(&entries)?;
            Ok(true)
        })
    }

    /// Atomically replaces existing rows and appends companion rows only while
    /// the complete backing-store snapshot remains byte-exact.
    pub fn replace_and_append_if_snapshot_unchanged(
        &self,
        expected_snapshot: &[CultCacheEnvelope],
        replacements: Vec<CultCacheEnvelope>,
    ) -> Result<bool> {
        if replacements.is_empty() {
            return Err(anyhow!(
                "conditional snapshot replacement requires replacements"
            ));
        }
        unique_batch_ids(expected_snapshot, "expected snapshot")?;
        let replacement_ids = unique_batch_ids(&replacements, "snapshot replacements")?;
        self.with_exclusive_lock(|| {
            let mut current = self.read_all_unlocked()?;
            let mut expected = expected_snapshot.to_vec();
            current.sort_by_key(entry_id);
            expected.sort_by_key(entry_id);
            if current != expected {
                return Ok(false);
            }
            current.retain(|row| !replacement_ids.contains(&entry_id(row)));
            current.extend(replacements);
            current.sort_by_key(entry_id);
            self.write_all_unlocked(&current)?;
            Ok(true)
        })
    }

    /// Atomically replaces and deletes rows only while the complete store
    /// snapshot remains byte-exact. This is the single-file compaction
    /// primitive: summary publication and history deletion share one write.
    pub fn replace_and_delete_if_snapshot_unchanged(
        &self,
        expected_snapshot: &[CultCacheEnvelope],
        replacements: Vec<CultCacheEnvelope>,
        deletions: &[CultCacheEnvelope],
    ) -> Result<bool> {
        if replacements.is_empty() || deletions.is_empty() {
            return Err(anyhow!(
                "conditional compaction requires replacements and deletions"
            ));
        }
        let expected_ids = unique_batch_ids(expected_snapshot, "expected snapshot")?;
        let replacement_ids = unique_batch_ids(&replacements, "replacement")?;
        let deletion_ids = unique_batch_ids(deletions, "deletion")?;
        if !deletion_ids.is_subset(&expected_ids) {
            return Err(anyhow!(
                "compaction deletion is absent from expected snapshot"
            ));
        }
        if !deletion_ids.is_disjoint(&replacement_ids) {
            return Err(anyhow!("compaction cannot replace and delete one identity"));
        }
        self.with_exclusive_lock(|| {
            let mut current = self.read_all_unlocked()?;
            let mut expected = expected_snapshot.to_vec();
            current.sort_by_key(entry_id);
            expected.sort_by_key(entry_id);
            if current != expected {
                return Ok(false);
            }
            for row in &replacements {
                if !expected_ids.contains(&entry_id(row))
                    && current
                        .iter()
                        .any(|candidate| entry_id(candidate) == entry_id(row))
                {
                    return Ok(false);
                }
            }
            current.retain(|row| !deletion_ids.contains(&entry_id(row)));
            for row in replacements {
                current.retain(|candidate| entry_id(candidate) != entry_id(&row));
                current.push(row);
            }
            current.sort_by_key(entry_id);
            self.write_all_unlocked(&current)?;
            Ok(true)
        })
    }

    fn read_all_unlocked(&self) -> Result<Vec<CultCacheEnvelope>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(&self.path)
            .with_context(|| format!("failed to read {}", self.path.display()))?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        decode_store_snapshot(&bytes)
            .or_else(|_| rmp_serde::from_slice(&bytes))
            .with_context(|| format!("failed to decode MessagePack {}", self.path.display()))
    }

    /// Reads one filesystem snapshot without creating or opening the sibling
    /// CultCache lock file.
    ///
    /// This is deliberately narrower than `pull_all`: use it only at a
    /// provider-owned, read-only crossing where the producer replaces the
    /// complete file atomically and the consumer has no authority to mutate
    /// the provider directory. An absent file is an empty snapshot. Writers
    /// and locally owned stores must continue to use the locked APIs.
    pub fn pull_all_read_only_snapshot(&self) -> Result<Vec<CultCacheEnvelope>> {
        self.read_all_unlocked()
    }

    fn write_all_unlocked(&self, entries: &[CultCacheEnvelope]) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        remove_abandoned_staging_files(&self.path)?;
        let bytes = rmp_serde::to_vec(&encode_store_snapshot(entries)?)
            .context("failed to encode MessagePack")?;
        let tmp_path = temporary_path_for(&self.path);
        let mut staged = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
            .with_context(|| format!("failed to create {}", tmp_path.display()))?;
        use std::io::Write;
        staged
            .write_all(&bytes)
            .with_context(|| format!("failed to write {}", tmp_path.display()))?;
        staged
            .sync_all()
            .with_context(|| format!("failed to sync {}", tmp_path.display()))?;
        drop(staged);
        replace_file_atomically(&tmp_path, &self.path)?;
        sync_parent_directory(&self.path)?;
        Ok(())
    }

    fn with_shared_lock<T>(&self, action: impl FnOnce() -> Result<T>) -> Result<T> {
        let lock = self.open_lock_file()?;
        fs2::FileExt::lock_shared(&lock)
            .with_context(|| format!("failed to lock {}", self.lock_path().display()))?;
        let result = action();
        fs2::FileExt::unlock(&lock)
            .with_context(|| format!("failed to unlock {}", self.lock_path().display()))?;
        result
    }

    fn with_exclusive_lock<T>(&self, action: impl FnOnce() -> Result<T>) -> Result<T> {
        let lock = self.open_lock_file()?;
        fs2::FileExt::lock_exclusive(&lock)
            .with_context(|| format!("failed to lock {}", self.lock_path().display()))?;
        let result = action();
        fs2::FileExt::unlock(&lock)
            .with_context(|| format!("failed to unlock {}", self.lock_path().display()))?;
        result
    }

    fn open_lock_file(&self) -> Result<File> {
        let lock_path = self.lock_path();
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("failed to open {}", lock_path.display()))
    }

    fn lock_path(&self) -> PathBuf {
        let mut lock_name = self
            .path
            .file_name()
            .map(|value| value.to_os_string())
            .unwrap_or_else(|| "cultcache.cc".into());
        lock_name.push(".lock");
        self.path.with_file_name(lock_name)
    }
}

struct ReadOnlySharedLockGuard<U, D>
where
    U: FnOnce(&File) -> Result<()>,
    D: FnOnce(anyhow::Error),
{
    lock: Option<File>,
    unlock: Option<U>,
    on_unlock_failure: Option<D>,
}

impl<U, D> ReadOnlySharedLockGuard<U, D>
where
    U: FnOnce(&File) -> Result<()>,
    D: FnOnce(anyhow::Error),
{
    fn new(lock: File, unlock: U, on_unlock_failure: D) -> Self {
        Self {
            lock: Some(lock),
            unlock: Some(unlock),
            on_unlock_failure: Some(on_unlock_failure),
        }
    }
}

impl<U, D> Drop for ReadOnlySharedLockGuard<U, D>
where
    U: FnOnce(&File) -> Result<()>,
    D: FnOnce(anyhow::Error),
{
    fn drop(&mut self) {
        let (Some(lock), Some(unlock)) = (self.lock.as_ref(), self.unlock.take()) else {
            return;
        };
        if let Err(error) = unlock(lock)
            && let Some(on_unlock_failure) = self.on_unlock_failure.take()
        {
            on_unlock_failure(error);
        }
    }
}

fn unique_batch_ids(
    entries: &[CultCacheEnvelope],
    label: &str,
) -> Result<BTreeSet<(String, String)>> {
    let ids = entries.iter().map(entry_id).collect::<BTreeSet<_>>();
    if ids.len() != entries.len() {
        return Err(anyhow!(
            "conditional batch {label} set contains duplicate identities"
        ));
    }
    Ok(ids)
}

impl CacheBackingStore for SingleFileMessagePackBackingStore {
    fn pull_all(&self) -> Result<Vec<CultCacheEnvelope>> {
        if !self.path.exists() && !self.lock_path().exists() {
            return Ok(Vec::new());
        }
        self.with_shared_lock(|| self.read_all_unlocked())
    }

    fn push(&mut self, entry: &CultCacheEnvelope) -> Result<()> {
        self.with_exclusive_lock(|| {
            let mut entries = self.read_all_unlocked()?;
            entries.retain(|candidate| entry_id(candidate) != entry_id(entry));
            entries.push(entry.clone());
            entries.sort_by_key(entry_id);
            self.write_all_unlocked(&entries)
        })
    }

    fn delete(&mut self, entry: &CultCacheEnvelope) -> Result<()> {
        self.with_exclusive_lock(|| {
            let mut entries = self.read_all_unlocked()?;
            entries.retain(|candidate| entry_id(candidate) != entry_id(entry));
            self.write_all_unlocked(&entries)
        })
    }

    fn push_all(&mut self, entries: &[CultCacheEnvelope], _options: PushAllOptions) -> Result<()> {
        self.with_exclusive_lock(|| {
            let mut entries = entries.to_vec();
            entries.sort_by_key(entry_id);
            self.write_all_unlocked(&entries)
        })
    }
}

const REDB_ENVELOPES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("cultcache_envelopes");

/// Transactional keyed CultCache storage. Each polymorphic `(type, key)`
/// identity is an independent redb row whose value is the exact MessagePack
/// serialization of its `CultCacheEnvelope`.
#[derive(Clone)]
pub struct RedbMessagePackBackingStore {
    path: PathBuf,
}

impl std::fmt::Debug for RedbMessagePackBackingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedbMessagePackBackingStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl RedbMessagePackBackingStore {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let store = Self { path };
        store.with_database(|database| {
            let write = database.begin_write()?;
            {
                write.open_table(REDB_ENVELOPES)?;
            }
            write.commit()?;
            Ok(())
        })?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn open_database(&self) -> Result<Database> {
        Database::create(&self.path)
            .with_context(|| format!("failed to open redb CultCache {}", self.path.display()))
    }

    /// redb permits one open writable database handle per path. The external
    /// CultCache lock therefore owns the complete open/transaction/close
    /// interval, preserving the fresh-handle and cross-process construction
    /// model used by the other backing store.
    fn with_database<T>(&self, action: impl FnOnce(&Database) -> Result<T>) -> Result<T> {
        let lock_path = self.lock_path();
        if let Some(parent) = lock_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("failed to open {}", lock_path.display()))?;
        fs2::FileExt::lock_exclusive(&lock)
            .with_context(|| format!("failed to lock {}", lock_path.display()))?;
        let result = self.open_database().and_then(|database| action(&database));
        fs2::FileExt::unlock(&lock)
            .with_context(|| format!("failed to unlock {}", lock_path.display()))?;
        result
    }

    fn lock_path(&self) -> PathBuf {
        let mut lock_name = self
            .path
            .file_name()
            .map(|value| value.to_os_string())
            .unwrap_or_else(|| "cultcache.cc".into());
        lock_name.push(".lock");
        self.path.with_file_name(lock_name)
    }

    pub fn compare_and_swap_entry(
        &self,
        expected: &CultCacheEnvelope,
        replacement: CultCacheEnvelope,
    ) -> Result<bool> {
        if entry_id(expected) != entry_id(&replacement) {
            return Err(anyhow!("entry CAS replacement must preserve identity"));
        }
        self.with_database(|database| {
            let write = database.begin_write()?;
            let matched = {
                let mut table = write.open_table(REDB_ENVELOPES)?;
                if read_redb_entry(&table, expected)? != Some(expected.clone()) {
                    false
                } else {
                    insert_redb_entry(&mut table, &replacement)?;
                    true
                }
            };
            if matched {
                write.commit()?;
            } else {
                write.abort()?;
            }
            Ok(matched)
        })
    }

    pub fn insert_entry_if_absent(&self, entry: CultCacheEnvelope) -> Result<bool> {
        self.with_database(|database| {
            let write = database.begin_write()?;
            let inserted = {
                let mut table = write.open_table(REDB_ENVELOPES)?;
                let key = redb_identity(&entry)?;
                if table.get(key.as_slice())?.is_some() {
                    false
                } else {
                    insert_redb_entry(&mut table, &entry)?;
                    true
                }
            };
            if inserted {
                write.commit()?;
            } else {
                write.abort()?;
            }
            Ok(inserted)
        })
    }

    pub fn compare_and_swap_batch(
        &self,
        expected: &[CultCacheEnvelope],
        replacements: Vec<CultCacheEnvelope>,
    ) -> Result<bool> {
        if replacements.is_empty() {
            return Err(anyhow!(
                "conditional batch requires a non-empty replacement set"
            ));
        }
        let expected_ids = unique_batch_ids(expected, "expected")?;
        let replacement_ids = unique_batch_ids(&replacements, "replacement")?;
        if !expected_ids.is_subset(&replacement_ids) {
            return Err(anyhow!(
                "conditional batch must replace every expected identity"
            ));
        }
        self.with_database(|database| {
            let write = database.begin_write()?;
            let matched = {
                let mut table = write.open_table(REDB_ENVELOPES)?;
                let mut valid = true;
                for row in expected {
                    if read_redb_entry(&table, row)? != Some(row.clone()) {
                        valid = false;
                        break;
                    }
                }
                if valid {
                    for row in &replacements {
                        if !expected_ids.contains(&entry_id(row))
                            && read_redb_entry(&table, row)?.is_some()
                        {
                            valid = false;
                            break;
                        }
                    }
                }
                if valid {
                    for row in &replacements {
                        insert_redb_entry(&mut table, row)?;
                    }
                }
                valid
            };
            if matched {
                write.commit()?;
            } else {
                write.abort()?;
            }
            Ok(matched)
        })
    }

    pub fn append_if_snapshot_unchanged(
        &self,
        expected_snapshot: &[CultCacheEnvelope],
        additions: Vec<CultCacheEnvelope>,
    ) -> Result<bool> {
        if additions.is_empty() {
            return Err(anyhow!("conditional snapshot append requires additions"));
        }
        unique_batch_ids(expected_snapshot, "expected snapshot")?;
        let addition_ids = unique_batch_ids(&additions, "snapshot additions")?;
        self.with_database(|database| {
            let write = database.begin_write()?;
            let matched = {
                let mut table = write.open_table(REDB_ENVELOPES)?;
                let mut current = read_all_redb(&table)?;
                let mut expected = expected_snapshot.to_vec();
                current.sort_by_key(entry_id);
                expected.sort_by_key(entry_id);
                let valid = current == expected
                    && !current
                        .iter()
                        .any(|row| addition_ids.contains(&entry_id(row)));
                if valid {
                    for row in &additions {
                        insert_redb_entry(&mut table, row)?;
                    }
                }
                valid
            };
            if matched {
                write.commit()?;
            } else {
                write.abort()?;
            }
            Ok(matched)
        })
    }

    pub fn replace_and_append_if_snapshot_unchanged(
        &self,
        expected_snapshot: &[CultCacheEnvelope],
        replacements: Vec<CultCacheEnvelope>,
    ) -> Result<bool> {
        if replacements.is_empty() {
            return Err(anyhow!(
                "conditional snapshot replacement requires replacements"
            ));
        }
        unique_batch_ids(expected_snapshot, "expected snapshot")?;
        unique_batch_ids(&replacements, "snapshot replacements")?;
        self.with_database(|database| {
            let write = database.begin_write()?;
            let matched = {
                let mut table = write.open_table(REDB_ENVELOPES)?;
                let mut current = read_all_redb(&table)?;
                let mut expected = expected_snapshot.to_vec();
                current.sort_by_key(entry_id);
                expected.sort_by_key(entry_id);
                let valid = current == expected;
                if valid {
                    for row in &replacements {
                        insert_redb_entry(&mut table, row)?;
                    }
                }
                valid
            };
            if matched {
                write.commit()?;
            } else {
                write.abort()?;
            }
            Ok(matched)
        })
    }

    /// Atomically replaces and deletes rows only while the complete store
    /// snapshot remains byte-exact. This is the compaction primitive for a
    /// non-owning keyed handle: a stale reader cannot summarize one history
    /// while deleting another.
    pub fn replace_and_delete_if_snapshot_unchanged(
        &self,
        expected_snapshot: &[CultCacheEnvelope],
        replacements: Vec<CultCacheEnvelope>,
        deletions: &[CultCacheEnvelope],
    ) -> Result<bool> {
        if replacements.is_empty() || deletions.is_empty() {
            return Err(anyhow!(
                "conditional compaction requires replacements and deletions"
            ));
        }
        let expected_ids = unique_batch_ids(expected_snapshot, "expected snapshot")?;
        let replacement_ids = unique_batch_ids(&replacements, "replacement")?;
        let deletion_ids = unique_batch_ids(deletions, "deletion")?;
        if !deletion_ids.is_subset(&expected_ids) {
            return Err(anyhow!(
                "compaction deletion is absent from expected snapshot"
            ));
        }
        if !deletion_ids.is_disjoint(&replacement_ids) {
            return Err(anyhow!("compaction cannot replace and delete one identity"));
        }
        self.with_database(|database| {
            let write = database.begin_write()?;
            let matched = {
                let mut table = write.open_table(REDB_ENVELOPES)?;
                let mut current = read_all_redb(&table)?;
                let mut expected = expected_snapshot.to_vec();
                current.sort_by_key(entry_id);
                expected.sort_by_key(entry_id);
                let mut valid = current == expected;
                if valid {
                    for row in &replacements {
                        if !expected_ids.contains(&entry_id(row))
                            && read_redb_entry(&table, row)?.is_some()
                        {
                            valid = false;
                            break;
                        }
                    }
                }
                if valid {
                    for identity in deletion_ids {
                        let key = rmp_serde::to_vec(&identity)
                            .context("failed to encode CultCache identity")?;
                        table.remove(key.as_slice())?;
                    }
                    for row in &replacements {
                        insert_redb_entry(&mut table, row)?;
                    }
                }
                valid
            };
            if matched {
                write.commit()?;
            } else {
                write.abort()?;
            }
            Ok(matched)
        })
    }

    pub fn delete_batch_if_unchanged(&self, expected: &[CultCacheEnvelope]) -> Result<bool> {
        if expected.is_empty() {
            return Err(anyhow!(
                "conditional delete requires a non-empty expected set"
            ));
        }
        unique_batch_ids(expected, "delete expected")?;
        self.with_database(|database| {
            let write = database.begin_write()?;
            let matched = {
                let mut table = write.open_table(REDB_ENVELOPES)?;
                let mut valid = true;
                for row in expected {
                    if read_redb_entry(&table, row)? != Some(row.clone()) {
                        valid = false;
                        break;
                    }
                }
                if valid {
                    for row in expected {
                        let key = redb_identity(row)?;
                        table.remove(key.as_slice())?;
                    }
                }
                valid
            };
            if matched {
                write.commit()?;
            } else {
                write.abort()?;
            }
            Ok(matched)
        })
    }
}

struct OwnedRedbInner {
    database: Database,
    _owner_lock: File,
    file_identity: String,
}

/// A pinned redb store for one long-lived service owner. Unlike the transient
/// redb store, this handle holds the CultCache external lock, an open file
/// handle, and the redb database for its entire lifetime. Clones share that
/// exact ownership authority.
#[derive(Clone)]
pub struct OwnedRedbMessagePackBackingStore {
    path: PathBuf,
    inner: Arc<OwnedRedbInner>,
}

impl std::fmt::Debug for OwnedRedbMessagePackBackingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnedRedbMessagePackBackingStore")
            .field("path", &self.path)
            .field("file_identity", &self.inner.file_identity)
            .finish_non_exhaustive()
    }
}

impl OwnedRedbMessagePackBackingStore {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let lock_path = redb_lock_path(&path);
        let owner_lock = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("failed to open {}", lock_path.display()))?;
        fs2::FileExt::try_lock_exclusive(&owner_lock).with_context(|| {
            format!(
                "redb CultCache {} already has an active owner",
                path.display()
            )
        })?;

        // Give redb ownership of the already pinned file, structurally closing
        // path substitution during database creation. The post-open identity
        // comparison separately verifies that the pathname still names it.
        let database_file = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .with_context(|| format!("failed to pin redb CultCache {}", path.display()))?;
        let held_identity = file_identity_from_file(&database_file)?;
        let database = Database::builder()
            .create_file(database_file)
            .with_context(|| format!("failed to open redb CultCache {}", path.display()))?;
        let path_identity_file = File::open(&path)?;
        let path_identity = file_identity_from_file(&path_identity_file)?;
        if held_identity != path_identity {
            bail!(
                "redb CultCache path identity changed while opening: held {held_identity}, path {path_identity}"
            );
        }
        let write = database.begin_write()?;
        {
            write.open_table(REDB_ENVELOPES)?;
        }
        write.commit()?;
        Ok(Self {
            path,
            inner: Arc::new(OwnedRedbInner {
                database,
                _owner_lock: owner_lock,
                file_identity: held_identity,
            }),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_identity(&self) -> &str {
        &self.inner.file_identity
    }

    pub fn require_file_identity(&self, expected: &str) -> Result<()> {
        if expected != self.file_identity() {
            bail!(
                "redb CultCache file identity mismatch: expected {expected}, owned {}",
                self.file_identity()
            );
        }
        Ok(())
    }

    pub fn validate_path_identity(&self) -> Result<()> {
        let current_file = File::open(&self.path)
            .with_context(|| format!("owned redb path {} is missing", self.path.display()))?;
        let current = file_identity_from_file(&current_file)?;
        self.require_file_identity(&current).with_context(|| {
            format!(
                "owned redb path {} no longer names the pinned file",
                self.path.display()
            )
        })
    }

    pub fn compare_and_swap_entry(
        &self,
        expected: &CultCacheEnvelope,
        replacement: CultCacheEnvelope,
    ) -> Result<bool> {
        if entry_id(expected) != entry_id(&replacement) {
            return Err(anyhow!("entry CAS replacement must preserve identity"));
        }
        let write = self.inner.database.begin_write()?;
        let matched = {
            let mut table = write.open_table(REDB_ENVELOPES)?;
            if read_redb_entry(&table, expected)? != Some(expected.clone()) {
                false
            } else {
                insert_redb_entry(&mut table, &replacement)?;
                true
            }
        };
        if matched {
            write.commit()?;
        } else {
            write.abort()?;
        }
        Ok(matched)
    }

    pub fn insert_entry_if_absent(&self, entry: CultCacheEnvelope) -> Result<bool> {
        let write = self.inner.database.begin_write()?;
        let inserted = {
            let mut table = write.open_table(REDB_ENVELOPES)?;
            let key = redb_identity(&entry)?;
            if table.get(key.as_slice())?.is_some() {
                false
            } else {
                insert_redb_entry(&mut table, &entry)?;
                true
            }
        };
        if inserted {
            write.commit()?;
        } else {
            write.abort()?;
        }
        Ok(inserted)
    }

    pub fn compare_and_swap_batch(
        &self,
        expected: &[CultCacheEnvelope],
        replacements: Vec<CultCacheEnvelope>,
    ) -> Result<bool> {
        if replacements.is_empty() {
            return Err(anyhow!(
                "conditional batch requires a non-empty replacement set"
            ));
        }
        let expected_ids = unique_batch_ids(expected, "expected")?;
        let replacement_ids = unique_batch_ids(&replacements, "replacement")?;
        if !expected_ids.is_subset(&replacement_ids) {
            return Err(anyhow!(
                "conditional batch must replace every expected identity"
            ));
        }
        let write = self.inner.database.begin_write()?;
        let matched = {
            let mut table = write.open_table(REDB_ENVELOPES)?;
            let mut valid = true;
            for row in expected {
                if read_redb_entry(&table, row)? != Some(row.clone()) {
                    valid = false;
                    break;
                }
            }
            if valid {
                for row in &replacements {
                    if !expected_ids.contains(&entry_id(row))
                        && read_redb_entry(&table, row)?.is_some()
                    {
                        valid = false;
                        break;
                    }
                }
            }
            if valid {
                for row in &replacements {
                    insert_redb_entry(&mut table, row)?;
                }
            }
            valid
        };
        if matched {
            write.commit()?;
        } else {
            write.abort()?;
        }
        Ok(matched)
    }

    pub fn append_if_snapshot_unchanged(
        &self,
        expected_snapshot: &[CultCacheEnvelope],
        additions: Vec<CultCacheEnvelope>,
    ) -> Result<bool> {
        if additions.is_empty() {
            return Err(anyhow!("conditional snapshot append requires additions"));
        }
        unique_batch_ids(expected_snapshot, "expected snapshot")?;
        let addition_ids = unique_batch_ids(&additions, "snapshot additions")?;
        let write = self.inner.database.begin_write()?;
        let matched = {
            let mut table = write.open_table(REDB_ENVELOPES)?;
            let mut current = read_all_redb(&table)?;
            let mut expected = expected_snapshot.to_vec();
            current.sort_by_key(entry_id);
            expected.sort_by_key(entry_id);
            let valid = current == expected
                && !current
                    .iter()
                    .any(|row| addition_ids.contains(&entry_id(row)));
            if valid {
                for row in &additions {
                    insert_redb_entry(&mut table, row)?;
                }
            }
            valid
        };
        if matched {
            write.commit()?;
        } else {
            write.abort()?;
        }
        Ok(matched)
    }

    pub fn replace_and_append_if_snapshot_unchanged(
        &self,
        expected_snapshot: &[CultCacheEnvelope],
        replacements: Vec<CultCacheEnvelope>,
    ) -> Result<bool> {
        if replacements.is_empty() {
            return Err(anyhow!(
                "conditional snapshot replacement requires replacements"
            ));
        }
        unique_batch_ids(expected_snapshot, "expected snapshot")?;
        unique_batch_ids(&replacements, "snapshot replacements")?;
        let write = self.inner.database.begin_write()?;
        let matched = {
            let mut table = write.open_table(REDB_ENVELOPES)?;
            let mut current = read_all_redb(&table)?;
            let mut expected = expected_snapshot.to_vec();
            current.sort_by_key(entry_id);
            expected.sort_by_key(entry_id);
            let valid = current == expected;
            if valid {
                for row in &replacements {
                    insert_redb_entry(&mut table, row)?;
                }
            }
            valid
        };
        if matched {
            write.commit()?;
        } else {
            write.abort()?;
        }
        Ok(matched)
    }

    pub fn delete_batch_if_unchanged(&self, expected: &[CultCacheEnvelope]) -> Result<bool> {
        if expected.is_empty() {
            return Err(anyhow!(
                "conditional delete requires a non-empty expected set"
            ));
        }
        let expected_ids = unique_batch_ids(expected, "delete expected")?;
        let write = self.inner.database.begin_write()?;
        let matched = {
            let mut table = write.open_table(REDB_ENVELOPES)?;
            let mut valid = true;
            for row in expected {
                if read_redb_entry(&table, row)? != Some(row.clone()) {
                    valid = false;
                    break;
                }
            }
            if valid {
                for identity in expected_ids {
                    let key = rmp_serde::to_vec(&identity)
                        .context("failed to encode CultCache identity")?;
                    table.remove(key.as_slice())?;
                }
            }
            valid
        };
        if matched {
            write.commit()?;
        } else {
            write.abort()?;
        }
        Ok(matched)
    }

    /// Atomically replaces and deletes rows only while the complete store
    /// snapshot remains byte-exact. This is the compaction primitive: a stale
    /// reader cannot summarize one history while deleting another.
    pub fn replace_and_delete_if_snapshot_unchanged(
        &self,
        expected_snapshot: &[CultCacheEnvelope],
        replacements: Vec<CultCacheEnvelope>,
        deletions: &[CultCacheEnvelope],
    ) -> Result<bool> {
        if replacements.is_empty() || deletions.is_empty() {
            return Err(anyhow!(
                "conditional compaction requires replacements and deletions"
            ));
        }
        let expected_ids = unique_batch_ids(expected_snapshot, "expected snapshot")?;
        let replacement_ids = unique_batch_ids(&replacements, "replacement")?;
        let deletion_ids = unique_batch_ids(deletions, "deletion")?;
        if !deletion_ids.is_subset(&expected_ids) {
            return Err(anyhow!(
                "compaction deletion is absent from expected snapshot"
            ));
        }
        if !deletion_ids.is_disjoint(&replacement_ids) {
            return Err(anyhow!("compaction cannot replace and delete one identity"));
        }
        let write = self.inner.database.begin_write()?;
        let matched = {
            let mut table = write.open_table(REDB_ENVELOPES)?;
            let mut current = read_all_redb(&table)?;
            let mut expected = expected_snapshot.to_vec();
            current.sort_by_key(entry_id);
            expected.sort_by_key(entry_id);
            let mut valid = current == expected;
            if valid {
                for row in &replacements {
                    if !expected_ids.contains(&entry_id(row))
                        && read_redb_entry(&table, row)?.is_some()
                    {
                        valid = false;
                        break;
                    }
                }
            }
            if valid {
                for identity in deletion_ids {
                    let key = rmp_serde::to_vec(&identity)
                        .context("failed to encode CultCache identity")?;
                    table.remove(key.as_slice())?;
                }
                for row in &replacements {
                    insert_redb_entry(&mut table, row)?;
                }
            }
            valid
        };
        if matched {
            write.commit()?;
        } else {
            write.abort()?;
        }
        Ok(matched)
    }
}

impl CacheBackingStore for OwnedRedbMessagePackBackingStore {
    fn pull_all(&self) -> Result<Vec<CultCacheEnvelope>> {
        let read = self.inner.database.begin_read()?;
        let table = read.open_table(REDB_ENVELOPES)?;
        read_all_redb(&table)
    }

    fn push(&mut self, entry: &CultCacheEnvelope) -> Result<()> {
        let write = self.inner.database.begin_write()?;
        {
            let mut table = write.open_table(REDB_ENVELOPES)?;
            insert_redb_entry(&mut table, entry)?;
        }
        write.commit()?;
        Ok(())
    }

    fn delete(&mut self, entry: &CultCacheEnvelope) -> Result<()> {
        let write = self.inner.database.begin_write()?;
        {
            let mut table = write.open_table(REDB_ENVELOPES)?;
            let key = redb_identity(entry)?;
            table.remove(key.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    fn push_all(&mut self, entries: &[CultCacheEnvelope], _options: PushAllOptions) -> Result<()> {
        unique_batch_ids(entries, "push all")?;
        let write = self.inner.database.begin_write()?;
        {
            let mut table = write.open_table(REDB_ENVELOPES)?;
            let keys = table
                .iter()?
                .map(|row| Ok(row?.0.value().to_vec()))
                .collect::<Result<Vec<_>>>()?;
            for key in keys {
                table.remove(key.as_slice())?;
            }
            for entry in entries {
                insert_redb_entry(&mut table, entry)?;
            }
        }
        write.commit()?;
        Ok(())
    }
}

fn redb_lock_path(path: &Path) -> PathBuf {
    let mut lock_name = path
        .file_name()
        .map(|value| value.to_os_string())
        .unwrap_or_else(|| "cultcache.cc".into());
    lock_name.push(".lock");
    path.with_file_name(lock_name)
}

#[cfg(unix)]
fn file_identity_from_file(file: &File) -> Result<String> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata()?;
    Ok(format!(
        "unix:{:016x}:{:016x}",
        metadata.dev(),
        metadata.ino()
    ))
}

#[cfg(windows)]
fn file_identity_from_file(file: &File) -> Result<String> {
    use std::mem::zeroed;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };
    let mut information: BY_HANDLE_FILE_INFORMATION = unsafe { zeroed() };
    let success =
        unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut information) };
    if success == 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to read Windows file identity");
    }
    let index = ((information.nFileIndexHigh as u64) << 32) | information.nFileIndexLow as u64;
    Ok(format!(
        "windows:{:08x}:{index:016x}",
        information.dwVolumeSerialNumber
    ))
}

fn redb_identity(entry: &CultCacheEnvelope) -> Result<Vec<u8>> {
    rmp_serde::to_vec(&entry_id(entry)).context("failed to encode CultCache identity")
}

fn read_redb_entry(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    identity: &CultCacheEnvelope,
) -> Result<Option<CultCacheEnvelope>> {
    let key = redb_identity(identity)?;
    table
        .get(key.as_slice())?
        .map(|value| {
            rmp_serde::from_slice(value.value()).context("failed to decode redb CultCache envelope")
        })
        .transpose()
}

fn insert_redb_entry(
    table: &mut redb::Table<&[u8], &[u8]>,
    entry: &CultCacheEnvelope,
) -> Result<()> {
    let key = redb_identity(entry)?;
    let value = rmp_serde::to_vec(entry).context("failed to encode redb CultCache envelope")?;
    table.insert(key.as_slice(), value.as_slice())?;
    Ok(())
}

fn read_all_redb(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<Vec<CultCacheEnvelope>> {
    let mut entries = table
        .iter()?
        .map(|row| {
            let (_, value) = row?;
            rmp_serde::from_slice(value.value()).context("failed to decode redb CultCache envelope")
        })
        .collect::<Result<Vec<_>>>()?;
    entries.sort_by_key(entry_id);
    Ok(entries)
}

impl CacheBackingStore for RedbMessagePackBackingStore {
    fn pull_all(&self) -> Result<Vec<CultCacheEnvelope>> {
        self.with_database(|database| {
            let read = database.begin_read()?;
            let table = read.open_table(REDB_ENVELOPES)?;
            read_all_redb(&table)
        })
    }

    fn push(&mut self, entry: &CultCacheEnvelope) -> Result<()> {
        self.with_database(|database| {
            let write = database.begin_write()?;
            {
                let mut table = write.open_table(REDB_ENVELOPES)?;
                insert_redb_entry(&mut table, entry)?;
            }
            write.commit()?;
            Ok(())
        })
    }

    fn delete(&mut self, entry: &CultCacheEnvelope) -> Result<()> {
        self.with_database(|database| {
            let write = database.begin_write()?;
            {
                let mut table = write.open_table(REDB_ENVELOPES)?;
                let key = redb_identity(entry)?;
                table.remove(key.as_slice())?;
            }
            write.commit()?;
            Ok(())
        })
    }

    fn push_all(&mut self, entries: &[CultCacheEnvelope], _options: PushAllOptions) -> Result<()> {
        unique_batch_ids(entries, "push all")?;
        self.with_database(|database| {
            let write = database.begin_write()?;
            {
                let mut table = write.open_table(REDB_ENVELOPES)?;
                let keys = table
                    .iter()?
                    .map(|row| Ok(row?.0.value().to_vec()))
                    .collect::<Result<Vec<_>>>()?;
                for key in keys {
                    table.remove(key.as_slice())?;
                }
                for entry in entries {
                    insert_redb_entry(&mut table, entry)?;
                }
            }
            write.commit()?;
            Ok(())
        })
    }
}

struct CultCacheStoreRegistration {
    store: Box<dyn CacheBackingStore>,
    types: BTreeSet<String>,
}

pub struct CultCache {
    definitions: BTreeMap<String, &'static str>,
    entries: BTreeMap<(String, String), CultCacheEnvelope>,
    stores: Vec<CultCacheStoreRegistration>,
}

impl CultCache {
    pub fn new() -> Self {
        Self {
            definitions: BTreeMap::new(),
            entries: BTreeMap::new(),
            stores: Vec::new(),
        }
    }

    pub fn register_entry_type<T: DatabaseEntry>(&mut self) -> Result<()> {
        if T::TYPE.trim().is_empty() {
            return Err(anyhow!(
                "CultCache entry types must declare a non-empty type"
            ));
        }
        if let Some(existing_schema) = self.definitions.get(T::TYPE)
            && *existing_schema != T::SCHEMA_NAME
        {
            return Err(anyhow!(
                "CultCache already has a different definition registered for type {:?}",
                T::TYPE
            ));
        }
        self.definitions.insert(T::TYPE.to_string(), T::SCHEMA_NAME);
        Ok(())
    }

    pub fn register_document_type<T: DatabaseEntry>(&mut self) -> Result<()> {
        self.register_entry_type::<T>()
    }

    pub fn registered_entry_types(&self) -> Vec<String> {
        self.definitions.keys().cloned().collect()
    }

    pub fn register_registry<R: CultCacheRegistry>(&mut self, registry: R) -> Result<&mut Self> {
        registry.register_entries(self)?;
        Ok(self)
    }

    pub fn add_backing_store(
        &mut self,
        store: impl CacheBackingStore + 'static,
        types: impl IntoIterator<Item = impl Into<String>>,
    ) {
        self.stores.push(CultCacheStoreRegistration {
            store: Box::new(store),
            types: types.into_iter().map(Into::into).collect(),
        });
    }

    pub fn add_generic_backing_store(&mut self, store: impl CacheBackingStore + 'static) {
        self.add_backing_store(store, Vec::<String>::new());
    }

    pub fn pull_all_backing_stores(&mut self) -> Result<()> {
        self.entries.clear();
        let known_types: BTreeSet<String> = self.definitions.keys().cloned().collect();
        for registration in &mut self.stores {
            for entry in registration.store.pull_all()? {
                if !known_types.contains(&entry.r#type) {
                    return Err(anyhow!(
                        "No schema is registered for persisted entry type {:?}",
                        entry.r#type
                    ));
                }
                self.entries.insert(entry_id(&entry), entry);
            }
        }
        Ok(())
    }

    /// Returns the exact envelopes in the currently loaded cache image.
    /// Callers that need optimistic concurrency can validate typed documents
    /// and capture CAS expectations from one coherent read.
    pub fn snapshot_envelopes(&self) -> Vec<CultCacheEnvelope> {
        self.entries.values().cloned().collect()
    }

    pub fn get<T: DatabaseEntry>(&self, key: &str) -> Result<Option<T>> {
        self.require_entry_type::<T>()?;
        let Some(entry) = self.entries.get(&entry_id_parts(T::TYPE, key)) else {
            return Ok(None);
        };
        let payload = rmp_serde::from_slice(&entry.payload).with_context(|| {
            format!(
                "failed to decode CultCache entry {:?} at key {:?} as {}",
                T::TYPE,
                key,
                T::SCHEMA_NAME
            )
        })?;
        Ok(Some(payload))
    }

    pub fn get_required<T: DatabaseEntry>(&self, key: &str) -> Result<T> {
        self.get::<T>(key)?
            .ok_or_else(|| anyhow!("CultCache has no {:?} entry at key {:?}", T::TYPE, key))
    }

    pub fn get_envelope<T: DatabaseEntry>(&self, key: &str) -> Result<Option<CultCacheEnvelope>> {
        self.require_entry_type::<T>()?;
        Ok(self.entries.get(&entry_id_parts(T::TYPE, key)).cloned())
    }

    pub fn get_required_envelope<T: DatabaseEntry>(&self, key: &str) -> Result<CultCacheEnvelope> {
        self.get_envelope::<T>(key)?
            .ok_or_else(|| anyhow!("CultCache has no {:?} envelope at key {:?}", T::TYPE, key))
    }

    pub fn get_all<T: DatabaseEntry>(&self) -> Result<Vec<T>> {
        self.require_entry_type::<T>()?;
        let mut values = Vec::new();
        for entry in self.entries.values() {
            if entry.r#type != T::TYPE {
                continue;
            }
            values.push(rmp_serde::from_slice(&entry.payload).with_context(|| {
                format!(
                    "failed to decode CultCache entry {:?} at key {:?} as {}",
                    T::TYPE,
                    entry.key,
                    T::SCHEMA_NAME
                )
            })?);
        }
        Ok(values)
    }

    pub fn get_all_with_keys<T: DatabaseEntry>(&self) -> Result<Vec<(String, T)>> {
        self.require_entry_type::<T>()?;
        let mut values = Vec::new();
        for entry in self.entries.values() {
            if entry.r#type != T::TYPE {
                continue;
            }
            values.push((
                entry.key.clone(),
                rmp_serde::from_slice(&entry.payload).with_context(|| {
                    format!(
                        "failed to decode CultCache entry {:?} at key {:?} as {}",
                        T::TYPE,
                        entry.key,
                        T::SCHEMA_NAME
                    )
                })?,
            ));
        }
        Ok(values)
    }

    pub fn soa<T: SoaDocument>(&self) -> Result<CultSoaTable<T>> {
        let keyed_rows = self.get_all_with_keys::<T>()?;
        let (keys, documents): (Vec<_>, Vec<_>) = keyed_rows.into_iter().unzip();
        let columns = T::soa_columns(&documents)
            .into_iter()
            .map(|(name, values)| (name.to_string(), values))
            .collect();
        Ok(CultSoaTable {
            keys,
            documents,
            columns,
            _document: PhantomData,
        })
    }

    pub fn put<T: DatabaseEntry>(&mut self, key: impl Into<String>, value: &T) -> Result<T> {
        let (entry, parsed) = self.prepare_entry(key, value)?;
        let route = self.resolve_route_indices(T::TYPE);
        let Some(primary_index) = route.first().copied() else {
            return Err(anyhow!(
                "No backing store is registered for entry type {:?}",
                T::TYPE
            ));
        };
        self.stores[primary_index].store.push(&entry)?;
        for mirror_index in route.iter().skip(1).copied() {
            self.stores[mirror_index].store.push(&entry)?;
        }
        self.entries.insert(entry_id(&entry), entry);
        Ok(parsed)
    }

    pub fn prepare_entry<T: DatabaseEntry>(
        &self,
        key: impl Into<String>,
        value: &T,
    ) -> Result<(CultCacheEnvelope, T)> {
        self.require_entry_type::<T>()?;
        let key = key.into();
        let payload = rmp_serde::to_vec(value).with_context(|| {
            format!(
                "failed to encode CultCache entry {:?} at key {:?} as {}",
                T::TYPE,
                key,
                T::SCHEMA_NAME
            )
        })?;
        self.finish_prepared_entry::<T>(key, payload)
    }

    /// Prepares a typed entry while encoding ordinary nested structs as
    /// MessagePack maps. DatabaseEntry itself remains the stable numeric-slot
    /// tuple; named nested fields prevent serde's omitted optional fields from
    /// shifting later values into the wrong positions.
    pub fn prepare_entry_named<T: DatabaseEntry>(
        &self,
        key: impl Into<String>,
        value: &T,
    ) -> Result<(CultCacheEnvelope, T)> {
        self.require_entry_type::<T>()?;
        let key = key.into();
        let payload = rmp_serde::to_vec_named(value).with_context(|| {
            format!(
                "failed to encode named CultCache entry {:?} at key {:?} as {}",
                T::TYPE,
                key,
                T::SCHEMA_NAME
            )
        })?;
        self.finish_prepared_entry::<T>(key, payload)
    }

    fn finish_prepared_entry<T: DatabaseEntry>(
        &self,
        key: String,
        payload: Vec<u8>,
    ) -> Result<(CultCacheEnvelope, T)> {
        let parsed: T = rmp_serde::from_slice(&payload).with_context(|| {
            format!(
                "failed to validate CultCache entry {:?} at key {:?} as {}",
                T::TYPE,
                key,
                T::SCHEMA_NAME
            )
        })?;
        let entry = CultCacheEnvelope {
            key: key.clone(),
            r#type: T::TYPE.to_string(),
            payload,
            stored_at: now_utc_second(),
            schema_id: Some(T::TYPE.to_string()),
        };
        Ok((entry, parsed))
    }

    pub fn put_prepared_batch(&mut self, staged: Vec<CultCacheEnvelope>) -> Result<()> {
        if staged.is_empty() {
            return Err(anyhow!("CultCache batch must contain at least one entry"));
        }
        let mut store_index = None;
        for entry in staged.iter().chain(self.entries.values()) {
            if !self.definitions.contains_key(&entry.r#type) {
                return Err(anyhow!(
                    "CultCache batch entry type {:?} is not registered",
                    entry.r#type
                ));
            }
            let route = self.resolve_route_indices(&entry.r#type);
            if route.len() != 1 {
                return Err(anyhow!(
                    "CultCache atomic batch requires exactly one backing-store route for type {:?}, found {}",
                    entry.r#type,
                    route.len()
                ));
            }
            match store_index {
                Some(expected) if expected != route[0] => {
                    return Err(anyhow!("CultCache atomic batch cannot span backing stores"));
                }
                None => store_index = Some(route[0]),
                _ => {}
            }
        }
        let store_index = store_index.expect("non-empty batch establishes a store route");
        let mut next = self.entries.clone();
        for entry in staged {
            next.insert(entry_id(&entry), entry);
        }
        let snapshot: Vec<_> = next.values().cloned().collect();
        self.stores[store_index]
            .store
            .push_all(&snapshot, PushAllOptions::default())?;
        self.entries = next;
        Ok(())
    }

    pub fn put_envelope<T: DatabaseEntry>(&mut self, entry: CultCacheEnvelope) -> Result<T> {
        let parsed = self.validate_envelope::<T>(&entry)?;
        let route = self.resolve_route_indices(T::TYPE);
        let Some(primary_index) = route.first().copied() else {
            return Err(anyhow!(
                "No backing store is registered for entry type {:?}",
                T::TYPE
            ));
        };
        self.stores[primary_index].store.push(&entry)?;
        for mirror_index in route.iter().skip(1).copied() {
            self.stores[mirror_index].store.push(&entry)?;
        }
        self.entries.insert(entry_id(&entry), entry);
        Ok(parsed)
    }

    /// Publishes an envelope for a registered dynamic document type without
    /// claiming compile-time knowledge of its payload schema. This is the
    /// CultMesh/schema-registry boundary: callers own payload validation before
    /// admission; CultCache still enforces registered type, identity, timestamp,
    /// routing, and persistence.
    pub fn put_raw_envelope(&mut self, entry: CultCacheEnvelope) -> Result<()> {
        if !self.definitions.contains_key(&entry.r#type) {
            return Err(anyhow!(
                "No entry type registered for CultCache envelope type {:?}",
                entry.r#type
            ));
        }
        if entry.key.trim().is_empty() {
            return Err(anyhow!(
                "CultCache envelope keys for type {:?} must be non-empty",
                entry.r#type
            ));
        }
        if entry.stored_at.trim().is_empty() {
            return Err(anyhow!(
                "CultCache envelope stored_at for type {:?} must be non-empty",
                entry.r#type
            ));
        }
        let route = self.resolve_route_indices(&entry.r#type);
        let Some(primary_index) = route.first().copied() else {
            return Err(anyhow!(
                "No backing store is registered for entry type {:?}",
                entry.r#type
            ));
        };
        self.stores[primary_index].store.push(&entry)?;
        for mirror_index in route.iter().skip(1).copied() {
            self.stores[mirror_index].store.push(&entry)?;
        }
        self.entries.insert(entry_id(&entry), entry);
        Ok(())
    }

    /// Validates and admits an existing envelope into this cache image without
    /// publishing it to any backing store. This is the typed read primitive for
    /// owner-filtered views over a shared polymorphic store.
    pub fn load_envelope<T: DatabaseEntry>(&mut self, entry: CultCacheEnvelope) -> Result<T> {
        let parsed = self.validate_envelope::<T>(&entry)?;
        self.entries.insert(entry_id(&entry), entry);
        Ok(parsed)
    }

    fn validate_envelope<T: DatabaseEntry>(&self, entry: &CultCacheEnvelope) -> Result<T> {
        self.require_entry_type::<T>()?;
        if entry.r#type != T::TYPE {
            return Err(anyhow!(
                "CultCache envelope type {:?} does not match registered Rust type {:?}",
                entry.r#type,
                T::TYPE
            ));
        }
        if entry.key.trim().is_empty() {
            return Err(anyhow!(
                "CultCache envelope keys for type {:?} must be non-empty",
                T::TYPE
            ));
        }
        if entry.stored_at.trim().is_empty() {
            return Err(anyhow!(
                "CultCache envelope stored_at for type {:?} must be non-empty",
                T::TYPE
            ));
        }

        rmp_serde::from_slice(&entry.payload).with_context(|| {
            format!(
                "failed to validate CultCache envelope {:?} at key {:?} as {}",
                T::TYPE,
                entry.key,
                T::SCHEMA_NAME
            )
        })
    }

    pub fn update<T, F>(&mut self, key: &str, updater: F) -> Result<T>
    where
        T: DatabaseEntry,
        F: FnOnce(Option<T>) -> T,
    {
        let current = self.get::<T>(key)?;
        self.put::<T>(key.to_string(), &updater(current))
    }

    pub fn delete<T: DatabaseEntry>(&mut self, key: &str) -> Result<bool> {
        self.require_entry_type::<T>()?;
        let id = entry_id_parts(T::TYPE, key);
        let Some(entry) = self.entries.get(&id).cloned() else {
            return Ok(false);
        };
        let route = self.resolve_route_indices(T::TYPE);
        let Some(primary_index) = route.first().copied() else {
            return Err(anyhow!(
                "No backing store is registered for entry type {:?}",
                T::TYPE
            ));
        };
        self.stores[primary_index].store.delete(&entry)?;
        for mirror_index in route.iter().skip(1).copied() {
            self.stores[mirror_index].store.delete(&entry)?;
        }
        self.entries.remove(&id);
        Ok(true)
    }

    pub fn snapshot(&self) -> Vec<CultCacheEnvelope> {
        self.entries.values().cloned().collect()
    }

    pub fn snapshot_soa(&self) -> CultCacheSoaTable {
        CultCacheSoaTable::from_envelopes(self.snapshot())
    }

    pub fn load_soa(&mut self, table: CultCacheSoaTable) -> Result<()> {
        let known_types: BTreeSet<String> = self.definitions.keys().cloned().collect();
        self.entries.clear();
        for entry in table.into_envelopes()? {
            if !known_types.contains(&entry.r#type) {
                return Err(anyhow!(
                    "No schema is registered for SoA entry type {:?}",
                    entry.r#type
                ));
            }
            self.entries.insert(entry_id(&entry), entry);
        }
        Ok(())
    }

    fn require_entry_type<T: DatabaseEntry>(&self) -> Result<()> {
        match self.definitions.get(T::TYPE) {
            Some(schema_name) if *schema_name == T::SCHEMA_NAME => Ok(()),
            _ => Err(anyhow!(
                "CultCache entry type {:?} is not registered on this cache instance",
                T::TYPE
            )),
        }
    }

    fn resolve_route_indices(&self, type_id: &str) -> Vec<usize> {
        let type_specific: Vec<usize> = self
            .stores
            .iter()
            .enumerate()
            .filter_map(|(index, registration)| {
                registration.types.contains(type_id).then_some(index)
            })
            .collect();
        if !type_specific.is_empty() {
            return type_specific;
        }
        self.stores
            .iter()
            .enumerate()
            .filter_map(|(index, registration)| registration.types.is_empty().then_some(index))
            .collect()
    }
}

impl Default for CultCache {
    fn default() -> Self {
        Self::new()
    }
}

fn entry_id(entry: &CultCacheEnvelope) -> (String, String) {
    entry_id_parts(&entry.r#type, &entry.key)
}

fn entry_id_parts(r#type: &str, key: &str) -> (String, String) {
    (r#type.to_string(), key.to_string())
}

fn now_utc_second() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn encode_store_snapshot(entries: &[CultCacheEnvelope]) -> Result<PersistedStoreSnapshot> {
    let mut schema_types = BTreeMap::<String, String>::new();
    for entry in entries {
        let schema_id = entry
            .schema_id
            .clone()
            .unwrap_or_else(|| entry.r#type.clone());
        if let Some(existing_type) = schema_types.insert(schema_id.clone(), entry.r#type.clone()) {
            ensure!(
                existing_type == entry.r#type,
                "CultCache schema {schema_id:?} cannot identify both {existing_type:?} and {:?}",
                entry.r#type
            );
        }
    }

    let catalog = schema_types
        .into_iter()
        .map(|(schema_id, document_type)| {
            PersistedSchemaCatalogEntry(
                schema_id.clone(),
                document_type,
                format!("{schema_id}.v1"),
                schema_id.clone(),
                format!(
                    "{{\"schemaName\":\"{}\",\"schemaVersion\":\"{}.v1\",\"members\":[]}}",
                    escape_json_string(&schema_id),
                    escape_json_string(&schema_id)
                ),
                vec![schema_id],
                Vec::new(),
            )
        })
        .collect();
    let records = entries
        .iter()
        .map(|entry| {
            PersistedRecord(
                entry.key.clone(),
                entry
                    .schema_id
                    .clone()
                    .unwrap_or_else(|| entry.r#type.clone()),
                entry.stored_at.clone(),
                entry.payload.clone(),
            )
        })
        .collect();

    Ok(PersistedStoreSnapshot(
        "cultcache.store.v1".to_string(),
        catalog,
        records,
    ))
}

fn decode_store_snapshot(bytes: &[u8]) -> Result<Vec<CultCacheEnvelope>> {
    let snapshot: PersistedStoreSnapshot =
        rmp_serde::from_slice(bytes).context("failed to decode CultCache v1 snapshot")?;
    if snapshot.0 != "cultcache.store.v1" {
        return Err(anyhow!("unsupported CultCache snapshot {}", snapshot.0));
    }

    let catalog = snapshot
        .1
        .into_iter()
        .map(|entry| (entry.0, entry.1))
        .collect::<BTreeMap<_, _>>();
    snapshot
        .2
        .into_iter()
        .map(|record| {
            let r#type = catalog.get(&record.1).cloned().ok_or_else(|| {
                anyhow!(
                    "CultCache record {:?} references missing schema {:?}",
                    record.0,
                    record.1
                )
            })?;
            Ok(CultCacheEnvelope {
                key: record.0,
                r#type,
                stored_at: record.2,
                payload: record.3,
                schema_id: Some(record.1),
            })
        })
        .collect()
}

fn escape_json_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn temporary_path_for(path: &Path) -> PathBuf {
    let mut file_name = path
        .file_name()
        .map(|value| value.to_os_string())
        .unwrap_or_else(|| "cultcache.cc".into());
    file_name.push(format!(".{}.tmp", uuid::Uuid::new_v4()));
    path.with_file_name(file_name)
}

fn remove_abandoned_staging_files(destination: &Path) -> Result<()> {
    let Some(parent) = destination.parent() else {
        return Ok(());
    };
    let Some(destination_name) = destination.file_name().and_then(|value| value.to_str()) else {
        return Ok(());
    };
    let prefix = format!("{destination_name}.");
    for entry in fs::read_dir(parent).with_context(|| {
        format!(
            "failed to inspect {} for abandoned staging files",
            parent.display()
        )
    })? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(candidate) = name
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_suffix(".tmp"))
        else {
            continue;
        };
        if uuid::Uuid::parse_str(candidate).is_err() || !entry.file_type()?.is_file() {
            continue;
        }
        fs::remove_file(entry.path()).with_context(|| {
            format!(
                "failed to remove abandoned staging file {}",
                entry.path().display()
            )
        })?;
    }
    Ok(())
}

#[cfg(unix)]
fn replace_file_atomically(staged: &Path, destination: &Path) -> Result<()> {
    fs::rename(staged, destination).with_context(|| {
        format!(
            "failed to atomically replace {} with {}",
            destination.display(),
            staged.display()
        )
    })
}

#[cfg(windows)]
fn replace_file_atomically(staged: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let staged_wide = staged
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination_wide = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let success = unsafe {
        MoveFileExW(
            staged_wide.as_ptr(),
            destination_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if success == 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| {
            format!(
                "failed to atomically replace {} with {}",
                destination.display(),
                staged.display()
            )
        });
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("failed to sync {}", parent.display()))
}

#[cfg(windows)]
fn sync_parent_directory(_path: &Path) -> Result<()> {
    // MoveFileExW with MOVEFILE_WRITE_THROUGH owns publication durability.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    struct RefusingBatchStore {
        inner: SingleFileMessagePackBackingStore,
        refuse_batch: Arc<AtomicBool>,
    }

    impl CacheBackingStore for RefusingBatchStore {
        fn pull_all(&self) -> Result<Vec<CultCacheEnvelope>> {
            self.inner.pull_all()
        }
        fn push(&mut self, entry: &CultCacheEnvelope) -> Result<()> {
            self.inner.push(entry)
        }
        fn delete(&mut self, entry: &CultCacheEnvelope) -> Result<()> {
            self.inner.delete(entry)
        }
        fn push_all(
            &mut self,
            entries: &[CultCacheEnvelope],
            options: PushAllOptions,
        ) -> Result<()> {
            if self.refuse_batch.load(Ordering::SeqCst) {
                return Err(anyhow!("injected batch refusal"));
            }
            self.inner.push_all(entries, options)
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, DatabaseEntry)]
    #[cultcache(type = "settings")]
    struct Settings {
        #[cultcache(key = 0)]
        theme: String,
        #[cultcache(key = 1, default)]
        retries: u32,
    }

    #[derive(Clone, Debug, PartialEq, Eq, DatabaseEntry)]
    #[cultcache(type = "note")]
    struct Note {
        #[cultcache(key = 0)]
        title: String,
        #[cultcache(key = 1)]
        body: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, DatabaseEntry)]
    #[cultcache(type = "binary-record")]
    struct BinaryRecord {
        #[cultcache(key = 0, bytes)]
        required: Vec<u8>,
        #[cultcache(key = 1, bytes, default)]
        optional: Vec<u8>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct NestedOptionalValue {
        label: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        symbol: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, DatabaseEntry)]
    #[cultcache(type = "nested-note")]
    struct NestedNote {
        #[cultcache(key = 0)]
        value: NestedOptionalValue,
    }

    cultcache_soa!(Settings {
        "theme" => |row: &Settings| row.theme.clone(),
        "retries" => |row: &Settings| row.retries,
    });

    cultcache_registry!(TestEntries { Settings, Note });

    #[test]
    fn captured_snapshot_cannot_outlive_a_shared_consequence_gate() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("authority.cc");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        let released = CultCacheEnvelope {
            key: "brake".into(),
            r#type: "brake".into(),
            payload: b"released".to_vec(),
            stored_at: "2026-07-20T00:00:00Z".into(),
            schema_id: Some("brake".into()),
        };
        store.push(&released)?;

        let writer_path = store_path.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        store.with_read_only_shared_snapshot(|captured| {
            assert_eq!(captured, vec![released.clone()]);
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                let engaged = CultCacheEnvelope {
                    payload: b"engaged".to_vec(),
                    ..released
                };
                let mut writer = SingleFileMessagePackBackingStore::new(writer_path);
                writer.push(&engaged).unwrap();
                finished_tx.send(()).unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(1))?;
            assert!(
                finished_rx
                    .recv_timeout(Duration::from_millis(100))
                    .is_err()
            );
            Ok(())
        })?;
        finished_rx.recv_timeout(Duration::from_secs(1))?;
        assert_eq!(store.pull_all()?[0].payload, b"engaged");
        Ok(())
    }

    #[test]
    fn post_action_unlock_failure_is_diagnostic_not_action_result() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("authority.cc");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        store.push(&CultCacheEnvelope {
            key: "brake".into(),
            r#type: "brake".into(),
            payload: b"released".to_vec(),
            stored_at: "2026-07-20T00:00:00Z".into(),
            schema_id: Some("brake".into()),
        })?;

        let mut diagnostic = None;
        let result = store.with_read_only_shared_snapshot_using(
            |_| Ok(42),
            |_| Err(anyhow!("injected post-action unlock failure")),
            |error| diagnostic = Some(error.to_string()),
        );

        assert_eq!(result?, 42);
        assert_eq!(
            diagnostic.as_deref(),
            Some("injected post-action unlock failure")
        );
        Ok(())
    }

    #[test]
    fn familiar_cultcache_flow_persists_and_reloads_typed_documents() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("cache.cc");
        let settings = Settings {
            theme: "ash".to_string(),
            retries: 3,
        };

        let mut cache = CultCache::new();
        cache.register_entry_type::<Settings>()?;
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));
        cache.pull_all_backing_stores()?;
        cache.put("app", &settings)?;
        assert_eq!(cache.get_required::<Settings>("app")?, settings);

        let mut reloaded = CultCache::new();
        reloaded.register_entry_type::<Settings>()?;
        reloaded.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));
        reloaded.pull_all_backing_stores()?;
        assert_eq!(reloaded.get_required::<Settings>("app")?, settings);
        Ok(())
    }

    #[test]
    fn entry_identity_is_polymorphic_by_type_and_key() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("cache.cc");
        let mut cache = CultCache::new();
        cache.register_registry(TestEntries)?;
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));

        cache.put(
            "shared",
            &Settings {
                theme: "green".to_string(),
                retries: 1,
            },
        )?;
        cache.put(
            "shared",
            &Note {
                title: "same key".to_string(),
                body: "different type".to_string(),
            },
        )?;

        assert_eq!(cache.snapshot().len(), 2);
        assert_eq!(cache.get_required::<Note>("shared")?.title, "same key");
        assert_eq!(cache.get_required::<Settings>("shared")?.theme, "green");
        Ok(())
    }

    #[test]
    fn prepared_batch_publishes_heterogeneous_entries_in_one_snapshot() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("batch.cc");
        let mut cache = CultCache::new();
        cache.register_registry(TestEntries)?;
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));

        let (settings, _) = cache.prepare_entry(
            "app",
            &Settings {
                theme: "iron".to_string(),
                retries: 4,
            },
        )?;
        let (note, _) = cache.prepare_entry(
            "receipt",
            &Note {
                title: "committed".to_string(),
                body: "same snapshot".to_string(),
            },
        )?;
        cache.put_prepared_batch(vec![settings, note])?;

        let mut reloaded = CultCache::new();
        reloaded.register_registry(TestEntries)?;
        reloaded.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));
        reloaded.pull_all_backing_stores()?;
        assert_eq!(reloaded.get_required::<Settings>("app")?.theme, "iron");
        assert_eq!(reloaded.get_required::<Note>("receipt")?.title, "committed");
        Ok(())
    }

    #[test]
    fn named_preparation_preserves_omitted_nested_optional_slots() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("named.cc");
        let mut cache = CultCache::new();
        cache.register_entry_type::<NestedNote>()?;
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));
        cache.pull_all_backing_stores()?;
        let note = NestedNote {
            value: NestedOptionalValue {
                label: "nested".into(),
                count: None,
                symbol: Some("kept-in-its-own-field".into()),
            },
        };
        assert!(cache.prepare_entry("compact", &note).is_err());
        let (entry, parsed) = cache.prepare_entry_named("named", &note)?;
        assert_eq!(parsed, note);
        cache.put_prepared_batch(vec![entry])?;
        assert_eq!(cache.get_required::<NestedNote>("named")?, note);
        Ok(())
    }

    #[test]
    fn pulling_an_absent_store_does_not_create_its_parent_or_lock() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let missing_parent = temp.path().join("missing-body");
        let store_path = missing_parent.join("cache.cc");
        let store = SingleFileMessagePackBackingStore::new(&store_path);

        assert!(store.pull_all()?.is_empty());
        assert!(!missing_parent.exists());
        assert!(!store_path.with_file_name("cache.cc.lock").exists());
        Ok(())
    }

    #[test]
    fn provider_snapshot_read_does_not_create_or_open_a_lock() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("provider.cc");
        let lock_path = temp.path().join("provider.cc.lock");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        let expected = test_envelope("provider.delivery", "one", b"payload");
        store.push(&expected)?;
        std::fs::remove_file(&lock_path)?;

        assert_eq!(store.pull_all_read_only_snapshot()?, vec![expected]);
        assert!(!lock_path.exists());
        Ok(())
    }

    #[test]
    fn next_exclusive_write_removes_only_exact_abandoned_staging_files() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("cache.cc");
        let abandoned = temporary_path_for(&store_path);
        let unknown = temp.path().join("cache.cc.not-a-uuid.tmp");
        let foreign = temp
            .path()
            .join("other.cc.00000000-0000-0000-0000-000000000000.tmp");
        fs::write(&abandoned, b"partial staging bytes")?;
        fs::write(&unknown, b"operator-owned unknown file")?;
        fs::write(&foreign, b"foreign store staging")?;

        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        let expected = test_envelope("model", "current", b"one");
        store.push(&expected)?;

        assert!(!abandoned.exists());
        assert_eq!(fs::read(&unknown)?, b"operator-owned unknown file");
        assert_eq!(fs::read(&foreign)?, b"foreign store staging");
        assert_eq!(store.pull_all()?, vec![expected]);
        Ok(())
    }

    #[test]
    fn failed_snapshot_publication_preserves_the_committed_destination() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let destination = temp.path().join("cache.cc");
        let absent_staging = temp.path().join("absent.tmp");
        let committed = b"committed snapshot";
        fs::write(&destination, committed)?;

        assert!(replace_file_atomically(&absent_staging, &destination).is_err());
        assert_eq!(fs::read(&destination)?, committed);
        Ok(())
    }

    #[test]
    fn snapshot_publication_atomically_replaces_an_existing_destination() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let destination = temp.path().join("cache.cc");
        let staged = temp.path().join("cache.cc.staged");
        fs::write(&destination, b"old snapshot")?;
        fs::write(&staged, b"new snapshot")?;

        replace_file_atomically(&staged, &destination)?;

        assert_eq!(fs::read(&destination)?, b"new snapshot");
        assert!(!staged.exists());
        Ok(())
    }

    #[test]
    #[ignore = "operator SIGKILL probe; requires CULTCACHE_INTERRUPTION_STORE"]
    fn snapshot_publication_interruption_probe_writer() -> Result<()> {
        let store_path = std::env::var_os("CULTCACHE_INTERRUPTION_STORE")
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("CULTCACHE_INTERRUPTION_STORE is required"))?;
        let marker_path = std::env::var_os("CULTCACHE_INTERRUPTION_MARKER")
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("CULTCACHE_INTERRUPTION_MARKER is required"))?;
        let mut store = SingleFileMessagePackBackingStore::new(store_path);
        for sequence in 0_u64.. {
            store.push(&test_envelope(
                "interruption.probe",
                "current",
                &rmp_serde::to_vec(&sequence)?,
            ))?;
            fs::write(&marker_path, sequence.to_string())?;
        }
        unreachable!("the interruption writer is stopped by the operator")
    }

    #[test]
    #[ignore = "operator SIGKILL probe; requires CULTCACHE_INTERRUPTION_STORE"]
    fn snapshot_publication_interruption_probe_reader() -> Result<()> {
        let store_path = std::env::var_os("CULTCACHE_INTERRUPTION_STORE")
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("CULTCACHE_INTERRUPTION_STORE is required"))?;
        let rows = SingleFileMessagePackBackingStore::new(store_path).pull_all()?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].r#type, "interruption.probe");
        assert_eq!(rows[0].key, "current");
        let _: u64 = rmp_serde::from_slice(&rows[0].payload)?;
        Ok(())
    }

    fn test_envelope(r#type: &str, key: &str, payload: &[u8]) -> CultCacheEnvelope {
        CultCacheEnvelope {
            key: key.to_string(),
            r#type: r#type.to_string(),
            payload: payload.to_vec(),
            stored_at: "2026-07-13T00:00:00Z".to_string(),
            schema_id: Some(r#type.to_string()),
        }
    }

    #[test]
    fn redb_store_crud_and_reopen_preserve_polymorphic_rows() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("keyed.cc");
        let model = test_envelope("model", "shared", b"one");
        let receipt = test_envelope("receipt", "shared", b"two");
        {
            let mut store = RedbMessagePackBackingStore::new(&path)?;
            store.push(&model)?;
            store.push(&receipt)?;
            assert_eq!(store.pull_all()?.len(), 2);
            store.delete(&receipt)?;
        }
        let reopened = RedbMessagePackBackingStore::new(&path)?;
        assert_eq!(reopened.pull_all()?, vec![model]);
        Ok(())
    }

    #[test]
    fn redb_exact_cas_refuses_stale_value_without_mutation() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let mut store = RedbMessagePackBackingStore::new(temp.path().join("cas.cc"))?;
        let current = test_envelope("model", "current", b"one");
        store.push(&current)?;
        let stale = test_envelope("model", "current", b"stale");
        let replacement = test_envelope("model", "current", b"two");
        assert!(!store.compare_and_swap_entry(&stale, replacement.clone())?);
        assert_eq!(store.pull_all()?, vec![current.clone()]);
        assert!(store.compare_and_swap_entry(&current, replacement.clone())?);
        assert_eq!(store.pull_all()?, vec![replacement]);
        Ok(())
    }

    #[test]
    fn redb_batch_is_atomic_and_stale_member_refuses_every_write() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let mut store = RedbMessagePackBackingStore::new(temp.path().join("batch.cc"))?;
        let first = test_envelope("model", "first", b"one");
        let second = test_envelope("model", "second", b"one");
        store.push(&first)?;
        store.push(&second)?;
        let stale_second = test_envelope("model", "second", b"stale");
        let first_two = test_envelope("model", "first", b"two");
        let second_two = test_envelope("model", "second", b"two");
        let receipt = test_envelope("receipt", "batch", b"committed");
        assert!(!store.compare_and_swap_batch(
            &[first.clone(), stale_second],
            vec![first_two.clone(), second_two.clone(), receipt.clone()],
        )?);
        assert_eq!(store.pull_all()?, vec![first.clone(), second.clone()]);
        assert!(store.compare_and_swap_batch(
            &[first, second],
            vec![first_two.clone(), second_two.clone(), receipt.clone()],
        )?);
        assert_eq!(store.pull_all()?, vec![first_two, second_two, receipt]);
        Ok(())
    }

    #[test]
    fn redb_compaction_refuses_stale_snapshot_and_atomically_replaces_and_deletes() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let mut store = RedbMessagePackBackingStore::new(temp.path().join("compact.cc"))?;
        let old_head = test_envelope("retention", "current", b"one");
        let retired = test_envelope("receipt", "old", b"terminal");
        store.push(&old_head)?;
        store.push(&retired)?;
        let stale = store.pull_all()?;
        let concurrent = test_envelope("receipt", "running", b"active");
        assert!(store.insert_entry_if_absent(concurrent.clone())?);
        let new_head = test_envelope("retention", "current", b"two");
        assert!(!store.replace_and_delete_if_snapshot_unchanged(
            &stale,
            vec![new_head.clone()],
            std::slice::from_ref(&retired),
        )?);
        assert!(store.pull_all()?.contains(&retired));
        let exact = store.pull_all()?;
        assert!(store.replace_and_delete_if_snapshot_unchanged(
            &exact,
            vec![new_head.clone()],
            &[retired],
        )?);
        assert_eq!(store.pull_all()?, vec![concurrent, new_head]);
        Ok(())
    }

    #[test]
    fn redb_entry_cas_does_not_depend_on_unrelated_snapshot_changes() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = RedbMessagePackBackingStore::new(temp.path().join("independent.cc"))?;
        let current = test_envelope("model", "current", b"one");
        assert!(store.insert_entry_if_absent(current.clone())?);
        let unrelated = test_envelope("event", "later", b"noise");
        assert!(store.insert_entry_if_absent(unrelated.clone())?);
        let replacement = test_envelope("model", "current", b"two");
        assert!(store.compare_and_swap_entry(&current, replacement.clone())?);
        assert_eq!(store.pull_all()?, vec![unrelated, replacement]);
        Ok(())
    }

    #[test]
    fn redb_fresh_handles_share_one_database_for_the_same_cultcache_path() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("shared.cc");
        let mut first = RedbMessagePackBackingStore::new(&path)?;
        let second = RedbMessagePackBackingStore::new(&path)?;
        let initial = test_envelope("model", "current", b"one");
        first.push(&initial)?;
        assert_eq!(second.pull_all()?, vec![initial.clone()]);
        let replacement = test_envelope("model", "current", b"two");
        assert!(second.compare_and_swap_entry(&initial, replacement.clone())?);
        assert_eq!(first.pull_all()?, vec![replacement]);
        Ok(())
    }

    #[test]
    fn owned_redb_clones_share_authority_and_fresh_owner_is_refused() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("owned.cc");
        let owner = OwnedRedbMessagePackBackingStore::new(&path)?;
        let clone = owner.clone();
        assert_eq!(owner.file_identity(), clone.file_identity());
        assert!(OwnedRedbMessagePackBackingStore::new(&path).is_err());
        drop(owner);
        assert!(OwnedRedbMessagePackBackingStore::new(&path).is_err());
        drop(clone);
        assert!(OwnedRedbMessagePackBackingStore::new(&path).is_ok());
        Ok(())
    }

    #[test]
    fn owned_redb_file_identity_is_stable_across_writes_and_reopen() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("identity.cc");
        let identity = {
            let mut owner = OwnedRedbMessagePackBackingStore::new(&path)?;
            let identity = owner.file_identity().to_string();
            owner.push(&test_envelope("model", "current", b"one"))?;
            owner.validate_path_identity()?;
            owner.require_file_identity(&identity)?;
            identity
        };
        let reopened = OwnedRedbMessagePackBackingStore::new(&path)?;
        assert_eq!(reopened.file_identity(), identity);
        assert_eq!(reopened.pull_all()?.len(), 1);
        Ok(())
    }

    #[test]
    fn owned_redb_identity_helper_refuses_mismatch() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let owner = OwnedRedbMessagePackBackingStore::new(temp.path().join("mismatch.cc"))?;
        assert!(owner.require_file_identity("not-the-owned-file").is_err());
        owner.require_file_identity(owner.file_identity())?;
        Ok(())
    }

    #[test]
    fn owned_redb_batch_refuses_stale_member_and_commits_success_atomically() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let mut owner = OwnedRedbMessagePackBackingStore::new(temp.path().join("batch-owned.cc"))?;
        let first = test_envelope("model", "first", b"one");
        let second = test_envelope("model", "second", b"one");
        owner.push(&first)?;
        owner.push(&second)?;
        let first_two = test_envelope("model", "first", b"two");
        let second_two = test_envelope("model", "second", b"two");
        let companion = test_envelope("receipt", "batch", b"committed");
        let stale_second = test_envelope("model", "second", b"stale");
        assert!(!owner.compare_and_swap_batch(
            &[first.clone(), stale_second],
            vec![first_two.clone(), second_two.clone(), companion.clone()],
        )?);
        assert_eq!(owner.pull_all()?, vec![first.clone(), second.clone()]);
        assert!(owner.compare_and_swap_batch(
            &[first, second],
            vec![first_two.clone(), second_two.clone(), companion.clone()],
        )?);
        assert_eq!(owner.pull_all()?, vec![first_two, second_two, companion]);
        Ok(())
    }

    #[test]
    fn owned_redb_compaction_refuses_stale_snapshot_and_atomically_replaces_and_deletes()
    -> Result<()> {
        let temp = tempfile::tempdir()?;
        let mut owner =
            OwnedRedbMessagePackBackingStore::new(temp.path().join("compact-owned.cc"))?;
        let old_head = test_envelope("retention", "current", b"one");
        let retired_a = test_envelope("claim", "a", b"terminal");
        let retired_b = test_envelope("receipt", "a", b"terminal");
        owner.push(&old_head)?;
        owner.push(&retired_a)?;
        owner.push(&retired_b)?;
        let stale = owner.pull_all()?;
        let concurrent = test_envelope("claim", "concurrent", b"running");
        assert!(owner.insert_entry_if_absent(concurrent.clone())?);
        let new_head = test_envelope("retention", "current", b"two");
        assert!(!owner.replace_and_delete_if_snapshot_unchanged(
            &stale,
            vec![new_head.clone()],
            &[retired_a.clone(), retired_b.clone()],
        )?);
        assert!(owner.pull_all()?.contains(&retired_a));
        let exact = owner.pull_all()?;
        assert!(owner.replace_and_delete_if_snapshot_unchanged(
            &exact,
            vec![new_head.clone()],
            &[retired_a, retired_b],
        )?);
        assert_eq!(owner.pull_all()?, vec![concurrent, new_head]);
        Ok(())
    }

    #[test]
    fn owned_redb_path_replacement_cannot_redirect_pinned_authority() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("pinned.cc");
        let displaced = temp.path().join("displaced.cc");
        let mut owner = OwnedRedbMessagePackBackingStore::new(&path)?;
        let identity = owner.file_identity().to_string();
        owner.push(&test_envelope("model", "before", b"one"))?;
        fs::rename(&path, &displaced)?;
        File::create(&path)?;
        assert!(owner.validate_path_identity().is_err());
        assert_eq!(owner.file_identity(), identity);
        owner.push(&test_envelope("model", "after", b"two"))?;
        assert_eq!(owner.pull_all()?.len(), 2);
        assert_ne!(
            file_identity_from_file(&File::open(&path)?)?,
            owner.file_identity()
        );
        Ok(())
    }

    #[test]
    fn redb_overlapping_fresh_handles_serialize_the_database_open_interval() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("contended.cc");
        let first = RedbMessagePackBackingStore::new(&path)?;
        let second = RedbMessagePackBackingStore::new(&path)?;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let first_barrier = barrier.clone();
        let first_thread = std::thread::spawn(move || -> Result<()> {
            first_barrier.wait();
            for index in 0..32 {
                first.insert_entry_if_absent(test_envelope(
                    "event",
                    &format!("first-{index}"),
                    b"first",
                ))?;
            }
            Ok(())
        });
        let second_barrier = barrier.clone();
        let second_thread = std::thread::spawn(move || -> Result<()> {
            second_barrier.wait();
            for index in 0..32 {
                second.insert_entry_if_absent(test_envelope(
                    "event",
                    &format!("second-{index}"),
                    b"second",
                ))?;
            }
            Ok(())
        });
        barrier.wait();
        first_thread.join().expect("first redb writer")?;
        second_thread.join().expect("second redb writer")?;
        assert_eq!(
            RedbMessagePackBackingStore::new(path)?.pull_all()?.len(),
            64
        );
        Ok(())
    }

    #[test]
    fn redb_accepts_a_bare_relative_cultcache_path() -> Result<()> {
        let name = format!("cultcache-redb-relative-{}.cc", uuid::Uuid::new_v4());
        let path = PathBuf::from(&name);
        let lock_path = PathBuf::from(format!("{name}.lock"));
        let store = RedbMessagePackBackingStore::new(&path)?;
        assert!(store.insert_entry_if_absent(test_envelope("model", "current", b"one"))?);
        assert_eq!(store.pull_all()?.len(), 1);
        fs::remove_file(path)?;
        fs::remove_file(lock_path)?;
        Ok(())
    }

    #[test]
    fn conditional_batch_replaces_expected_and_inserts_companion_atomically() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("conditional-batch.cc");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        let current = test_envelope("model", "current", b"revision-1");
        store.push(&current)?;
        let replacement = test_envelope("model", "current", b"revision-2");
        let companion = test_envelope("receipt", "migration-1", b"applied");

        assert!(store.compare_and_swap_batch(
            std::slice::from_ref(&current),
            vec![replacement.clone(), companion.clone()],
        )?);
        assert_eq!(store.pull_all()?, vec![replacement, companion]);
        Ok(())
    }

    #[test]
    fn conditional_batch_allows_atomic_first_import_when_identities_are_absent() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("conditional-import.cc");
        let store = SingleFileMessagePackBackingStore::new(&store_path);
        let model = test_envelope("model", "current", b"revision-1");
        let receipt = test_envelope("receipt", "migration-1", b"applied");

        assert!(store.compare_and_swap_batch(&[], vec![model.clone(), receipt.clone()])?);
        assert_eq!(store.pull_all()?, vec![model, receipt]);
        Ok(())
    }

    #[test]
    fn conditional_batch_stale_expected_and_companion_collision_preserve_exact_bytes() -> Result<()>
    {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("conditional-refusal.cc");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        let current = test_envelope("model", "current", b"revision-1");
        let occupied = test_envelope("receipt", "migration-1", b"someone-else");
        store.push(&current)?;
        store.push(&occupied)?;
        let before = fs::read(&store_path)?;

        let stale = test_envelope("model", "current", b"stale-copy");
        let replacement = test_envelope("model", "current", b"revision-2");
        let fresh_companion = test_envelope("receipt", "migration-2", b"applied");
        assert!(
            !store.compare_and_swap_batch(&[stale], vec![replacement.clone(), fresh_companion],)?
        );
        assert_eq!(fs::read(&store_path)?, before);

        assert!(!store.compare_and_swap_batch(
            &[current],
            vec![
                replacement,
                test_envelope("receipt", "migration-1", b"collision")
            ],
        )?);
        assert_eq!(fs::read(&store_path)?, before);
        Ok(())
    }

    #[test]
    fn conditional_batch_rejects_empty_replacements_and_duplicate_identities() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store = SingleFileMessagePackBackingStore::new(temp.path().join("invalid-batch.cc"));
        let entry = test_envelope("model", "current", b"one");

        assert!(
            store
                .compare_and_swap_batch(&[entry.clone()], Vec::new())
                .is_err()
        );
        assert!(
            store
                .compare_and_swap_batch(&[], vec![entry.clone(), entry.clone()])
                .is_err()
        );
        assert!(
            store
                .compare_and_swap_batch(
                    &[entry.clone(), entry.clone()],
                    vec![test_envelope("model", "current", b"two")],
                )
                .is_err()
        );
        assert!(!store.path().exists());
        Ok(())
    }

    #[test]
    fn full_snapshot_replacement_and_append_refuses_concurrent_rows() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let snapshot_path = temp.path().join("snapshot-replace-append.cc");
        let mut snapshot_store = SingleFileMessagePackBackingStore::new(&snapshot_path);
        let current = test_envelope("session", "current", b"active");
        snapshot_store.push(&current)?;
        let stale_snapshot = snapshot_store.pull_all()?;
        let concurrent = test_envelope("event", "concurrent", b"live");
        snapshot_store.push(&concurrent)?;
        let completed = test_envelope("session", "current", b"completed");
        let receipt = test_envelope("receipt", "terminal", b"done");
        assert!(!snapshot_store.replace_and_append_if_snapshot_unchanged(
            &stale_snapshot,
            vec![completed.clone(), receipt.clone()],
        )?);
        assert_eq!(
            snapshot_store.pull_all()?,
            vec![concurrent.clone(), current.clone()]
        );
        let exact_snapshot = snapshot_store.pull_all()?;
        assert!(snapshot_store.replace_and_append_if_snapshot_unchanged(
            &exact_snapshot,
            vec![completed.clone(), receipt.clone()],
        )?);
        assert_eq!(
            snapshot_store.pull_all()?,
            vec![concurrent.clone(), receipt.clone(), completed.clone()]
        );

        let keyed_path = temp.path().join("keyed-replace-append.redb");
        let mut keyed_store = RedbMessagePackBackingStore::new(&keyed_path)?;
        keyed_store.push(&current)?;
        let stale_snapshot = keyed_store.pull_all()?;
        keyed_store.push(&concurrent)?;
        assert!(!keyed_store.replace_and_append_if_snapshot_unchanged(
            &stale_snapshot,
            vec![completed.clone(), receipt.clone()],
        )?);
        let exact_snapshot = keyed_store.pull_all()?;
        assert!(keyed_store.replace_and_append_if_snapshot_unchanged(
            &exact_snapshot,
            vec![completed.clone(), receipt.clone()],
        )?);
        assert_eq!(
            keyed_store.pull_all()?,
            vec![concurrent, receipt, completed]
        );
        Ok(())
    }

    #[test]
    fn single_file_compaction_atomically_replaces_summary_and_deletes_history() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("conditional-compaction.cc");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        let history = test_envelope("history", "old", b"closed");
        let head = test_envelope("retention", "head", b"revision-1");
        store.push(&history)?;
        store.push(&head)?;
        let snapshot = store.pull_all()?;
        let next_head = test_envelope("retention", "head", b"revision-2");

        assert!(store.replace_and_delete_if_snapshot_unchanged(
            &snapshot,
            vec![next_head.clone()],
            std::slice::from_ref(&history),
        )?);
        assert_eq!(store.pull_all()?, vec![next_head]);

        let before = fs::read(&store_path)?;
        assert!(!store.replace_and_delete_if_snapshot_unchanged(
            &snapshot,
            vec![test_envelope("retention", "head", b"revision-3")],
            std::slice::from_ref(&history),
        )?);
        assert_eq!(fs::read(&store_path)?, before);
        Ok(())
    }

    #[test]
    fn snapshot_append_fences_concurrent_unknown_identity() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("snapshot-append.cc");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        let model = test_envelope("model", "current", b"revision-1");
        store.push(&model)?;
        let expected = store.pull_all()?;
        let duplicate_admission = test_envelope("admission", "unexpected", b"also-current");
        store.push(&duplicate_admission)?;
        let before = fs::read(&store_path)?;

        assert!(!store.append_if_snapshot_unchanged(
            &expected,
            vec![test_envelope("readiness", "proof", b"ready")],
        )?);
        assert_eq!(fs::read(&store_path)?, before);
        Ok(())
    }

    #[test]
    fn snapshot_append_is_order_insensitive_and_refuses_collisions() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("snapshot-order.cc");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        let a = test_envelope("a", "one", b"1");
        let b = test_envelope("b", "two", b"2");
        store.push(&a)?;
        store.push(&b)?;
        assert!(store.append_if_snapshot_unchanged(
            &[b.clone(), a.clone()],
            vec![test_envelope("proof", "three", b"3")],
        )?);
        let current = store.pull_all()?;
        assert!(!store.append_if_snapshot_unchanged(
            &current,
            vec![test_envelope("a", "one", b"collision")],
        )?);
        assert!(
            store
                .append_if_snapshot_unchanged(&current, Vec::new())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn type_and_key_identity_is_not_delimiter_ambiguous() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("delimiter-identity.cc");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        let left = test_envelope("a::b", "c", b"left");
        let right = test_envelope("a", "b::c", b"right");
        store.push(&left)?;
        store.push(&right)?;
        let rows = store.pull_all()?;
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&left));
        assert!(rows.contains(&right));
        Ok(())
    }

    #[test]
    fn refused_batch_preserves_prior_snapshot() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("refused-batch.cc");
        let refusal = Arc::new(AtomicBool::new(false));
        let mut cache = CultCache::new();
        cache.register_registry(TestEntries)?;
        cache.add_generic_backing_store(RefusingBatchStore {
            inner: SingleFileMessagePackBackingStore::new(&store_path),
            refuse_batch: refusal.clone(),
        });
        cache.put(
            "app",
            &Settings {
                theme: "before".to_string(),
                retries: 1,
            },
        )?;
        let (note, _) = cache.prepare_entry(
            "receipt",
            &Note {
                title: "after".to_string(),
                body: "must not land".to_string(),
            },
        )?;
        refusal.store(true, Ordering::SeqCst);
        assert!(cache.put_prepared_batch(vec![note]).is_err());
        assert!(cache.get::<Note>("receipt")?.is_none());

        let mut reloaded = CultCache::new();
        reloaded.register_registry(TestEntries)?;
        reloaded.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));
        reloaded.pull_all_backing_stores()?;
        assert_eq!(reloaded.get_required::<Settings>("app")?.theme, "before");
        assert!(reloaded.get::<Note>("receipt")?.is_none());
        Ok(())
    }

    #[test]
    fn type_specific_store_routes_before_generic_store() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let generic_path = temp.path().join("generic.cc");
        let settings_path = temp.path().join("settings.cc");
        let mut cache = CultCache::new();
        cache.register_entry_type::<Settings>()?;
        cache.register_entry_type::<Note>()?;
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&generic_path));
        cache.add_backing_store(
            SingleFileMessagePackBackingStore::new(&settings_path),
            ["settings"],
        );

        cache.put(
            "app",
            &Settings {
                theme: "ash".to_string(),
                retries: 3,
            },
        )?;
        cache.put(
            "memo",
            &Note {
                title: "hello".to_string(),
                body: "world".to_string(),
            },
        )?;

        let generic_entries = SingleFileMessagePackBackingStore::new(&generic_path).pull_all()?;
        let settings_entries = SingleFileMessagePackBackingStore::new(&settings_path).pull_all()?;
        assert_eq!(generic_entries[0].r#type, "note");
        assert_eq!(settings_entries[0].r#type, "settings");
        Ok(())
    }

    #[test]
    fn update_and_delete_follow_the_cache_api() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("cache.cc");
        let mut cache = CultCache::new();
        cache.register_entry_type::<Settings>()?;
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));

        let updated = cache.update::<Settings, _>("app", |current| {
            let mut current = current.unwrap_or(Settings {
                theme: "ash".to_string(),
                retries: 0,
            });
            current.retries += 1;
            current
        })?;
        assert_eq!(updated.retries, 1);
        assert!(cache.delete::<Settings>("app")?);
        assert!(cache.get::<Settings>("app")?.is_none());
        Ok(())
    }

    #[test]
    fn pull_rejects_unregistered_persisted_entry_type() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("cache.cc");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        store.push(&CultCacheEnvelope {
            key: "unknown".to_string(),
            r#type: "unregistered".to_string(),
            payload: rmp_serde::to_vec(&1_u8)?,
            stored_at: now_utc_second(),
            schema_id: Some("unregistered".to_string()),
        })?;

        let mut cache = CultCache::new();
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));
        let error = cache.pull_all_backing_stores().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("No schema is registered for persisted entry type")
        );
        Ok(())
    }

    #[test]
    fn payload_is_binary_messagepack_not_json_value() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("cache.cc");
        let mut cache = CultCache::new();
        cache.register_entry_type::<Settings>()?;
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));
        cache.put(
            "app",
            &Settings {
                theme: "ash".to_string(),
                retries: 3,
            },
        )?;

        let entry = cache.snapshot().remove(0);
        let decoded: Settings = rmp_serde::from_slice(&entry.payload)?;
        assert_eq!(decoded.theme, "ash");
        assert!(!entry.payload.is_empty());
        Ok(())
    }

    #[test]
    fn byte_slots_use_messagepack_binary_and_round_trip() -> Result<()> {
        let record = BinaryRecord {
            required: vec![1, 2, 3],
            optional: Vec::new(),
        };

        let payload = rmp_serde::to_vec(&record)?;
        assert_eq!(payload, vec![0x92, 0xc4, 3, 1, 2, 3, 0xc4, 0]);
        assert_eq!(rmp_serde::from_slice::<BinaryRecord>(&payload)?, record);
        Ok(())
    }

    #[test]
    fn corrupted_payload_fails_during_typed_retrieval() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("cache.cc");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        store.push(&CultCacheEnvelope {
            key: "app".to_string(),
            r#type: "settings".to_string(),
            payload: vec![0xc1],
            stored_at: now_utc_second(),
            schema_id: Some("settings".to_string()),
        })?;

        let mut cache = CultCache::new();
        cache.register_entry_type::<Settings>()?;
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));
        cache.pull_all_backing_stores()?;
        let error = cache.get_required::<Settings>("app").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed to decode CultCache entry")
        );
        Ok(())
    }

    #[test]
    fn put_envelope_reuses_existing_messagepack_payload() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let origin_store = temp.path().join("origin.cc");
        let target_store = temp.path().join("target.cc");

        let mut origin = CultCache::new();
        origin.register_entry_type::<Settings>()?;
        origin.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&origin_store));
        origin.put(
            "app",
            &Settings {
                theme: "ash".to_string(),
                retries: 3,
            },
        )?;

        let envelope = origin.get_required_envelope::<Settings>("app")?;

        let mut target = CultCache::new();
        target.register_entry_type::<Settings>()?;
        target.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&target_store));
        let applied = target.put_envelope::<Settings>(envelope.clone())?;

        assert_eq!(
            applied,
            Settings {
                theme: "ash".to_string(),
                retries: 3,
            }
        );
        assert_eq!(target.get_required::<Settings>("app")?, applied);
        assert_eq!(
            target.get_required_envelope::<Settings>("app")?.payload,
            envelope.payload
        );
        Ok(())
    }

    #[test]
    fn raw_envelope_path_preserves_a_registered_dynamic_payload() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("dynamic.cc");
        let envelope = CultCacheEnvelope {
            key: "app".to_string(),
            r#type: Settings::TYPE.to_string(),
            payload: vec![0xc1],
            stored_at: now_utc_second(),
            schema_id: Some("dynamic.settings.v1".to_string()),
        };

        let mut cache = CultCache::new();
        cache.register_entry_type::<Settings>()?;
        cache.register_entry_type::<Note>()?;
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));
        cache.put_raw_envelope(envelope.clone())?;

        assert_eq!(cache.snapshot(), vec![envelope.clone()]);
        assert_eq!(
            SingleFileMessagePackBackingStore::new(&store_path).pull_all()?,
            vec![envelope]
        );
        assert!(cache.get_required::<Settings>("app").is_err());
        let before_collision = fs::read(&store_path)?;
        assert!(
            cache
                .put_raw_envelope(CultCacheEnvelope {
                    key: "note".to_string(),
                    r#type: Note::TYPE.to_string(),
                    payload: Vec::new(),
                    stored_at: now_utc_second(),
                    schema_id: Some("dynamic.settings.v1".to_string()),
                })
                .is_err()
        );
        assert_eq!(fs::read(&store_path)?, before_collision);
        assert!(
            cache
                .put_raw_envelope(CultCacheEnvelope {
                    key: "foreign".to_string(),
                    r#type: "unregistered".to_string(),
                    payload: Vec::new(),
                    stored_at: now_utc_second(),
                    schema_id: None,
                })
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn soa_snapshot_round_trips_registered_entries() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("cache.cc");
        let mut cache = CultCache::new();
        cache.register_registry(TestEntries)?;
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));
        cache.put(
            "app",
            &Settings {
                theme: "ash".to_string(),
                retries: 3,
            },
        )?;
        cache.put(
            "memo",
            &Note {
                title: "machine".to_string(),
                body: "awake".to_string(),
            },
        )?;

        let soa = cache.snapshot_soa();
        assert_eq!(soa.len(), 2);
        assert_eq!(soa.keys.len(), soa.payloads.len());

        let mut restored = CultCache::new();
        restored.register_registry(TestEntries)?;
        restored.add_generic_backing_store(SingleFileMessagePackBackingStore::new(
            temp.path().join("restored.cc"),
        ));
        restored.load_soa(soa)?;
        assert_eq!(restored.get_required::<Settings>("app")?.theme, "ash");
        assert_eq!(restored.get_required::<Note>("memo")?.body, "awake");
        Ok(())
    }

    #[test]
    fn typed_soa_columns_match_csharp_cache_ergonomics() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("cache.cc");
        let mut cache = CultCache::new();
        cache.register_registry(TestEntries)?;
        cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new(&store_path));
        cache.put(
            "settings:a",
            &Settings {
                theme: "ash".to_string(),
                retries: 3,
            },
        )?;
        cache.put(
            "settings:b",
            &Settings {
                theme: "bone".to_string(),
                retries: 5,
            },
        )?;

        let table = cache.soa::<Settings>()?;
        assert_eq!(
            table.keys(),
            &["settings:a".to_string(), "settings:b".to_string()]
        );
        assert_eq!(
            table.column::<String>("theme")?.values(),
            &["ash".to_string(), "bone".to_string()]
        );
        assert_eq!(table.column::<u32>("retries")?.values(), &[3, 5]);

        let error = table.column::<String>("retries").unwrap_err();
        assert!(error.to_string().contains("not alloc::string::String"));
        Ok(())
    }

    #[test]
    fn soa_rejects_column_length_drift() {
        let table = CultCacheSoaTable {
            keys: vec!["app".to_string()],
            types: Vec::new(),
            payloads: Vec::new(),
            stored_ats: Vec::new(),
            schema_ids: Vec::new(),
        };
        let error = table.validate().unwrap_err();
        assert!(error.to_string().contains("column types has length 0"));
    }
}
