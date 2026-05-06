# CultCache Rust

`cultcache-rs` is the Rust port of the useful part of GameCult's CultCache:
a polymorphism-aware in-memory cache that remembers through pluggable backing
stores.

Consumer code should think in domain types:

```rust
let player = cache.get_required::<PlayerData>("player:meta")?;
cache.put("player:meta", &player)?;
```

The persistence layer should deal with envelopes, routing, MessagePack payload
bytes, and schema identity. Application code should not paw at loose JSON files
like a sad little bureaucrat with a clipboard.

## What It Is

CultCache is a domain cache with persistence adapters.

- `CultCache` is the query and mutation surface.
- Domain structs implement `DatabaseEntry`.
- Entries are stored behind a `type::key` identity, so multiple entry types
  can share a logical key without colliding.
- Each payload is encoded directly from the known `DatabaseEntry` type into
  MessagePack bytes.
- Backing stores are adapters, not the public data model.
- Writes persist to the resolved backing store before the in-memory cache is
  updated.
- Type-specific backing stores beat generic backing stores.
- `SingleFileMessagePackBackingStore` is the first concrete store and guards
  file access with a sidecar lock file.

This is not an ORM, not a database, and not distributed consensus in a novelty
hat. If multiple processes write the same backing file, use an external lock or
a coordinator.

## Current API

```rust
use cultcache_rs::{CultCache, DatabaseEntry, SingleFileMessagePackBackingStore};

#[derive(Clone, Debug, PartialEq, Eq, DatabaseEntry)]
#[cultcache(type = "settings")]
struct Settings {
    #[cultcache(key = 0)]
    theme: String,

    #[cultcache(key = 1, default)]
    retries: u32,
}

let mut cache = CultCache::new();
cache.register_entry_type::<Settings>()?;
cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new("cache.msgpack"));
cache.pull_all_backing_stores()?;

cache.put("app", &Settings {
    theme: "ash".to_string(),
    retries: 3,
})?;

let settings = cache.get_required::<Settings>("app")?;
# Ok::<(), anyhow::Error>(())
```

For a closed set of entries, generate a registry instead of repeating yourself:

```rust
use cultcache_rs::cultcache_registry;

cultcache_registry!(GameCultEntries {
    Settings,
    PlayerData,
});

let mut cache = CultCache::new();
cache.register_registry(GameCultEntries)?;
```

The important part is that callers retrieve domain values directly:

```rust
let settings: Settings = cache.get_required("app")?;
let all_settings: Vec<Settings> = cache.get_all()?;
```

The cache is the ergonomic surface. The backing store is the memory prosthetic.

## How This Maps From Original CultCache

The original C# CultCache relies on runtime reflection:

- cacheable models inherit from `DatabaseEntry`
- the cache scans child classes at startup
- `Get<T>(id)` returns a `T`
- optional `INamedEntry` enables name lookup
- optional field/property indexes are registered by member name
- backing stores pull/push/delete `DatabaseEntry` values

Rust does not have C#-style assembly scanning or runtime subclass discovery.
The closest honest Rust translation is:

- domain structs implement a marker trait, `DatabaseEntry`
- the derive macro generates a positional array formatter from explicit integer
  field slots
- the envelope carries the polymorphic type discriminator
- unknown persisted type ids fail closed instead of constructing arbitrary
  runtime types
- a generated registry should eventually replace repetitive manual
  registration

Manual `register_entry_type::<T>()` is still accepted, but entry identity no
longer needs a hand-written impl: `#[derive(DatabaseEntry)]` declares it beside
the domain type. `cultcache_registry!` provides the current generated resolver
surface for a closed entry set.

## Intended Rust Ergonomics

The target user-facing shape should be:

```rust
#[derive(Clone, DatabaseEntry)]
#[cultcache(type = "player")]
pub struct PlayerData {
    #[cultcache(key = 0)]
    pub id: String,

    #[cultcache(key = 1)]
    pub name: String,

    #[cultcache(key = 2)]
    pub faction: String,
}

let mut cache = CultCache::builder()
    .with_generated_entries(game_entries())
    .with_generic_store(SingleFileMessagePackBackingStore::new("cache.msgpack"))
    .build()?;

cache.pull_all_backing_stores()?;
let player = cache.get_required::<PlayerData>("player:ari")?;
```

The current `cultcache_registry!` macro is deliberately simple. The remaining
ergonomic endpoint is automatic inventory or build-script generation so users
do not have to maintain the registry list by hand.

The registry is the Rust equivalent of C# reflection and MessagePack resolver
setup. It should know:

- entry type id
- generated MessagePack/serde formatter path
- Rust type name / schema name
- optional store domain route
- optional name key extractor
- optional secondary indexes
- optional global singleton marker

In other words: code generation should restore the original CultCache feeling
without pretending Rust has runtime reflection hiding under the floorboards.

## Current Surface

- `CultCache::new`
- `register_entry_type::<T>`
- `add_generic_backing_store`
- `add_backing_store`
- `pull_all_backing_stores`
- `get::<T>`
- `get_required::<T>`
- `get_envelope::<T>`
- `get_required_envelope::<T>`
- `get_all::<T>`
- `put::<T>`
- `put_envelope::<T>`
- `update::<T>`
- `delete::<T>`
- `snapshot`
- `SingleFileMessagePackBackingStore`

## Backing Store Routing

Generic store:

```rust
cache.add_generic_backing_store(SingleFileMessagePackBackingStore::new("cache.msgpack"));
```

Type-specific store:

```rust
cache.add_backing_store(
    SingleFileMessagePackBackingStore::new("players.msgpack"),
    ["player"],
);
```

When writing a `PlayerData`, the cache checks type-specific stores first. If none
match, it writes to the first generic store. Later matching stores are mirrors.

This mirrors the C# behavior:

- specific domain stores own their domain
- the first generic store is the primary generic write target
- later generic stores mirror writes
- this is not multi-master

## Persistence Semantics

`put` persists before mutating the in-memory cache. If persistence fails, the
cache does not pretend the write succeeded.

The envelope is MessagePack, and the payload inside each envelope is also raw
MessagePack bytes encoded from the registered concrete `DatabaseEntry` type.
That avoids the old bootstrap path where payloads were normalized through
`serde_json::Value`.

For bit-compatible neighbors, that also enables a real fast lane:

- `get_envelope::<T>()` exports the canonical persisted bytes for a typed entry
- `put_envelope::<T>()` ingests the same envelope into another cache instance
  without re-encoding the payload first

It still decodes once for validation and typed reads. We are not pretending one
`Vec<u8>` became metaphysically zero-copy because we believed in it harder. But
the stupid decode/re-encode loop is gone.

The `DatabaseEntry` derive does not trust Rust source field order as the durable
schema. Every persisted member must declare a stable integer slot:

```rust
#[derive(Clone, DatabaseEntry)]
#[cultcache(type = "player")]
pub struct PlayerData {
    #[cultcache(key = 0)]
    pub id: String,

    #[cultcache(key = 1)]
    pub name: String,

    #[cultcache(key = 2, default)]
    pub level: u32,
}
```

The derive emits a tuple/array formatter. Gaps serialize as nil. Missing fields
marked `default` use `Default::default()` when older payloads are read. Deleted
field slots should stay reserved until an explicit store migration rewrites the
data.

The single-file MessagePack store rewrites an atomic snapshot. That is a sane
starting point for small typed state surfaces, settings, Epiphany agent memory,
heartbeat state, and other compact control-plane data. Large corpora should use
a sharded store or a real database instead of asking one file to become a
warehouse and then acting wounded when physics invoices us.

The single-file store uses a sidecar lock file for shared reads and exclusive
writes. That protects ordinary multi-process access to the same file. It is
still not a multi-master replication protocol, and it is not a substitute for a
coordinator when higher-level write ordering matters.

## Near-Term Ergonomic Improvements

1. **Derive macro**
   - `#[derive(DatabaseEntry)]`
   - `#[cultcache(type = "...")]`
   - `#[cultcache(key = N)]` on every persisted field
   - tuple/array formatter generation landed
   - optional `#[cultcache(name)]`, `#[cultcache(index)]`, and
     `#[cultcache(global)]` are still future work

2. **Generated registry**
   - a `CultCacheRegistry` trait
   - `cache.register_registry(GameCultEntries)`
   - basic macro registry landed
   - automatic inventory/build-script generation and store routes are future work

3. **Name and index lookups**
   - `cache.get_by_name::<T>("Potion")`
   - `cache.get_by_index::<T>("faction", "Lucent")`
   - generated extractors instead of reflection strings

4. **Schema/version metadata**
   - schema version in the envelope or payload metadata
   - explicit migration hooks
   - refusal on unknown persisted entry types unless a migration/resolver is
     installed

5. **Projection helpers**
   - JSON projection for review and git diffs
   - vector stripping / large-field elision for agent memory surfaces

## Why Not Skip Registration Immediately?

Rust can only deserialize polymorphic data into concrete types if something
maps the persisted `type` discriminator to the Rust type. In C#, reflection and
MessagePack resolvers can discover a closed `DatabaseEntry` inheritance tree at
runtime and route it through known formatters. In Rust, that map has to come from
somewhere:

- explicit registration
- a generated registry
- a macro inventory crate
- a hand-written resolver

Explicit registration is the simplest honest implementation. Generated registry
is the ergonomic destination. Hand-written resolver tables are the punishment
we give ourselves if we get lazy.

## Security Model

CultCache should never parse arbitrary structured data into arbitrary runtime
types. It only accepts persisted envelopes whose `type` discriminator resolves
to a known `DatabaseEntry` type registered in the cache or generated registry.

That is the Rust version of the original trick:

- C# refuses to store objects outside the `DatabaseEntry` root type.
- Runtime-visible `DatabaseEntry` subclasses get dedicated formatters.
- Unknown or unregistered types are rejected.
- MessagePack payload bytes deserialize through known concrete formatters, not
  an open-world type loader or generic structured-value bridge.

The derive/registry work should preserve that shape. Its job is to remove
manual registration ceremony, not to loosen the closed-world boundary.

## License

Private GameCult infrastructure for now. Public packaging can wait until the
API grows enough teeth to deserve strangers.
