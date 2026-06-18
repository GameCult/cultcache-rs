use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
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

    fn write_all_unlocked(&self, entries: &[CultCacheEnvelope]) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let bytes = rmp_serde::to_vec(&encode_store_snapshot(entries))
            .context("failed to encode MessagePack")?;
        let tmp_path = temporary_path_for(&self.path);
        fs::write(&tmp_path, bytes)
            .with_context(|| format!("failed to write {}", tmp_path.display()))?;
        if self.path.exists() {
            fs::remove_file(&self.path)
                .with_context(|| format!("failed to replace {}", self.path.display()))?;
        }
        fs::rename(&tmp_path, &self.path).with_context(|| {
            format!(
                "failed to move {} to {}",
                tmp_path.display(),
                self.path.display()
            )
        })?;
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

impl CacheBackingStore for SingleFileMessagePackBackingStore {
    fn pull_all(&self) -> Result<Vec<CultCacheEnvelope>> {
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

struct CultCacheStoreRegistration {
    store: Box<dyn CacheBackingStore>,
    types: BTreeSet<String>,
}

pub struct CultCache {
    definitions: BTreeMap<String, &'static str>,
    entries: BTreeMap<String, CultCacheEnvelope>,
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

    pub fn put_envelope<T: DatabaseEntry>(&mut self, entry: CultCacheEnvelope) -> Result<T> {
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

        let parsed: T = rmp_serde::from_slice(&entry.payload).with_context(|| {
            format!(
                "failed to validate CultCache envelope {:?} at key {:?} as {}",
                T::TYPE,
                entry.key,
                T::SCHEMA_NAME
            )
        })?;
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

fn entry_id(entry: &CultCacheEnvelope) -> String {
    entry_id_parts(&entry.r#type, &entry.key)
}

fn entry_id_parts(r#type: &str, key: &str) -> String {
    format!("{type}::{key}", type = r#type)
}

fn now_utc_second() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn encode_store_snapshot(entries: &[CultCacheEnvelope]) -> PersistedStoreSnapshot {
    let mut schema_names = BTreeSet::<String>::new();
    for entry in entries {
        schema_names.insert(
            entry
                .schema_id
                .clone()
                .unwrap_or_else(|| entry.r#type.clone()),
        );
    }

    let catalog = schema_names
        .into_iter()
        .map(|schema_id| {
            PersistedSchemaCatalogEntry(
                schema_id.clone(),
                schema_id.clone(),
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

    PersistedStoreSnapshot("cultcache.store.v1".to_string(), catalog, records)
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

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

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

    cultcache_soa!(Settings {
        "theme" => |row: &Settings| row.theme.clone(),
        "retries" => |row: &Settings| row.retries,
    });

    cultcache_registry!(TestEntries { Settings, Note });

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
