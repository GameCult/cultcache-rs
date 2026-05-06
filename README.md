# CultCache Rust

`cultcache-rs` is the Rust port of the useful part of GameCult's CultCache
idea: consumer code talks to domain types in one polymorphism-aware in-memory
cache, while backing stores handle persistence. The cache should feel like it
never forgets. The store is how it remembers after the process dies.

This is not an ORM, not a database, and not a tiny cathedral built because JSON
files looked at us funny.

## Shape

- `CultCache` is the query and mutation surface.
- Domain structs implement `CultCacheDocument`.
- Entries are keyed by `type::key`, so different document types can share a key.
- Backing stores are persistence adapters.
- Writes persist to the resolved backing store before the in-memory cache is
  updated.
- Type-specific backing stores beat generic backing stores.
- `SingleFileMessagePackBackingStore` is the first concrete store.

## Example

```rust
use cultcache_rs::{
    CultCache,
    CultCacheDocument,
    SingleFileMessagePackBackingStore,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Settings {
    theme: String,
    retries: u32,
}

impl CultCacheDocument for Settings {
    const TYPE: &'static str = "settings";
}

let mut cache = CultCache::new();
cache.register_document_type::<Settings>()?;
cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new("cache.msgpack"));
cache.pull_all_backing_stores()?;

cache.put("app", &Settings {
    theme: "ash".to_string(),
    retries: 3,
})?;

let settings = cache.get_required::<Settings>("app")?;
# Ok::<(), anyhow::Error>(())
```

## Current Scope

- typed heterogeneous document cache
- MessagePack single-file backing store
- generic and type-specific store routing
- `get`, `get_required`, `get_all`, `put`, `update`, `delete`, and `snapshot`

If multiple processes write the same backing file, use an external lock or a
coordinator. The single-file store is atomic for replacement, not a distributed
consensus machine in a fake mustache.
