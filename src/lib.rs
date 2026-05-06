use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

pub trait DatabaseEntry: Serialize + DeserializeOwned + Clone + Send + 'static {
    const TYPE: &'static str;
    const SCHEMA_NAME: &'static str = "DatabaseEntry";
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CultCacheEnvelope {
    pub key: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub payload: Value,
    pub stored_at: String,
}

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

    fn write_all(&self, entries: &[CultCacheEnvelope]) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let bytes = rmp_serde::to_vec_named(entries).context("failed to encode MessagePack")?;
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
}

impl CacheBackingStore for SingleFileMessagePackBackingStore {
    fn pull_all(&self) -> Result<Vec<CultCacheEnvelope>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(&self.path)
            .with_context(|| format!("failed to read {}", self.path.display()))?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        rmp_serde::from_slice(&bytes)
            .with_context(|| format!("failed to decode MessagePack {}", self.path.display()))
    }

    fn push(&mut self, entry: &CultCacheEnvelope) -> Result<()> {
        let mut entries = self.pull_all()?;
        entries.retain(|candidate| entry_id(candidate) != entry_id(entry));
        entries.push(entry.clone());
        entries.sort_by_key(entry_id);
        self.write_all(&entries)
    }

    fn delete(&mut self, entry: &CultCacheEnvelope) -> Result<()> {
        let mut entries = self.pull_all()?;
        entries.retain(|candidate| entry_id(candidate) != entry_id(entry));
        self.write_all(&entries)
    }

    fn push_all(&mut self, entries: &[CultCacheEnvelope], _options: PushAllOptions) -> Result<()> {
        let mut entries = entries.to_vec();
        entries.sort_by_key(entry_id);
        self.write_all(&entries)
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
        self.require_document_type::<T>()?;
        let Some(entry) = self.entries.get(&entry_id_parts(T::TYPE, key)) else {
            return Ok(None);
        };
        let payload = serde_json::from_value(entry.payload.clone()).with_context(|| {
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

    pub fn get_all<T: DatabaseEntry>(&self) -> Result<Vec<T>> {
        self.require_document_type::<T>()?;
        let mut values = Vec::new();
        for entry in self.entries.values() {
            if entry.r#type != T::TYPE {
                continue;
            }
            values.push(
                serde_json::from_value(entry.payload.clone()).with_context(|| {
                    format!(
                        "failed to decode CultCache entry {:?} at key {:?} as {}",
                        T::TYPE,
                        entry.key,
                        T::SCHEMA_NAME
                    )
                })?,
            );
        }
        Ok(values)
    }

    pub fn put<T: DatabaseEntry>(&mut self, key: impl Into<String>, value: &T) -> Result<T> {
        self.require_document_type::<T>()?;
        let key = key.into();
        let parsed: T = serde_json::from_value(serde_json::to_value(value).with_context(|| {
            format!(
                "failed to encode CultCache entry {:?} at key {:?} as {}",
                T::TYPE,
                key,
                T::SCHEMA_NAME
            )
        })?)
        .with_context(|| {
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
            payload: serde_json::to_value(&parsed)?,
            stored_at: now_utc_second(),
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

    pub fn update<T, F>(&mut self, key: &str, updater: F) -> Result<T>
    where
        T: DatabaseEntry,
        F: FnOnce(Option<T>) -> T,
    {
        let current = self.get::<T>(key)?;
        self.put::<T>(key.to_string(), &updater(current))
    }

    pub fn delete<T: DatabaseEntry>(&mut self, key: &str) -> Result<bool> {
        self.require_document_type::<T>()?;
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

    fn require_document_type<T: DatabaseEntry>(&self) -> Result<()> {
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

fn temporary_path_for(path: &Path) -> PathBuf {
    let mut file_name = path
        .file_name()
        .map(|value| value.to_os_string())
        .unwrap_or_else(|| "cultcache.msgpack".into());
    file_name.push(format!(".{}.tmp", uuid::Uuid::new_v4()));
    path.with_file_name(file_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct Settings {
        theme: String,
        retries: u32,
    }

    impl DatabaseEntry for Settings {
        const TYPE: &'static str = "settings";
    }

    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct Note {
        title: String,
        body: String,
    }

    impl DatabaseEntry for Note {
        const TYPE: &'static str = "note";
    }

    #[test]
    fn familiar_cultcache_flow_persists_and_reloads_typed_documents() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let store_path = temp.path().join("cache.msgpack");
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
        let store_path = temp.path().join("cache.msgpack");
        let mut cache = CultCache::new();
        cache.register_entry_type::<Settings>()?;
        cache.register_entry_type::<Note>()?;
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
        let generic_path = temp.path().join("generic.msgpack");
        let settings_path = temp.path().join("settings.msgpack");
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
        let store_path = temp.path().join("cache.msgpack");
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
        let store_path = temp.path().join("cache.msgpack");
        let mut store = SingleFileMessagePackBackingStore::new(&store_path);
        store.push(&CultCacheEnvelope {
            key: "unknown".to_string(),
            r#type: "unregistered".to_string(),
            payload: serde_json::json!({"value": 1}),
            stored_at: now_utc_second(),
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
}
