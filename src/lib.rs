//http://sled.rs/

#![allow(dead_code)]
mod iface;
mod sled_config;
mod sled_storage;
mod test;
mod test_kv;
mod test_list;
mod test_map;

use async_trait::async_trait;
use core::fmt;
use iface::*;
pub use iface::{List, Map};
use serde::Serialize;
use serde::de::DeserializeOwned;
pub use sled_config::Config;
use sled_storage::{SledStorageDB, SledStorageList, SledStorageMap};

type TimestampMillis = i64;
type Result<T> = anyhow::Result<T>;
#[inline]
fn timestamp_millis() -> TimestampMillis {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_millis() as TimestampMillis)
        .unwrap_or_else(|_| {
            let now = chrono::Local::now();
            now.timestamp_millis() as TimestampMillis
        })
}

#[allow(unused)]
const SEPARATOR: &[u8] = b"@";
#[allow(unused)]
const KEY_PREFIX: &[u8] = b"__sled@";
#[allow(unused)]
const KEY_PREFIX_LEN: &[u8] = b"__sled_len@";
#[allow(unused)]
const MAP_NAME_PREFIX: &[u8] = b"__sled_map@";
#[allow(unused)]
const LIST_NAME_PREFIX: &[u8] = b"__sled_list@";

/// Type alias for storage keys
type Key = Vec<u8>;
/// Result type for iteration items (key-value pair)
type IterItem<V> = Result<(Key, V)>;

pub async fn init_db(cfg: &Config) -> Result<StorageDB> {
    let db = SledStorageDB::new(cfg.clone()).await?;
    let db = StorageDB::Sled(db);
    Ok(db)
}

#[derive(Clone)]
pub enum StorageDB {
    Sled(SledStorageDB),
}

impl StorageDB {
    /// Accesses a named map
    #[inline]
    pub async fn map<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Result<StorageMap> {
        Ok(match self {
            StorageDB::Sled(db) => StorageMap::Sled(db.map(name, expire).await?),
        })
    }

    /// Removes a named map
    #[inline]
    pub async fn map_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.map_remove(name).await,
        }
    }

    /// Checks if map exists
    #[inline]
    pub async fn map_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self {
            StorageDB::Sled(db) => db.map_contains_key(key).await,
        }
    }

    /// Accesses a named list
    #[inline]
    pub async fn list<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Result<StorageList> {
        Ok(match self {
            StorageDB::Sled(db) => StorageList::Sled(db.list(name, expire).await?),
        })
    }

    /// Removes a named list
    #[inline]
    pub async fn list_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.list_remove(name).await,
        }
    }

    /// Checks if list exists
    #[inline]
    pub async fn list_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self {
            StorageDB::Sled(db) => db.list_contains_key(key).await,
        }
    }

    /// Inserts a key-value pair
    #[inline]
    pub async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.insert(key, val).await,
        }
    }

    /// Retrieves a value by key
    #[inline]
    pub async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.get(key).await,
        }
    }

    /// Removes a key-value pair
    #[inline]
    pub async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.remove(key).await,
        }
    }

    /// Batch insert of key-value pairs
    #[inline]
    pub async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.batch_insert(key_vals).await,
        }
    }

    /// Batch removal of keys
    #[inline]
    pub async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        match self {
            StorageDB::Sled(db) => db.batch_remove(keys).await,
        }
    }

    /// Increments a counter
    #[inline]
    pub async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.counter_incr(key, increment).await,
        }
    }

    /// Decrements a counter
    #[inline]
    pub async fn counter_decr<K>(&self, key: K, decrement: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.counter_decr(key, decrement).await,
        }
    }

    /// Gets counter value
    #[inline]
    pub async fn counter_get<K>(&self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.counter_get(key).await,
        }
    }

    /// Sets counter value
    #[inline]
    pub async fn counter_set<K>(&self, key: K, val: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.counter_set(key, val).await,
        }
    }

    /// Gets number of items (requires "len" feature)
    #[inline]
    #[cfg(feature = "len")]
    pub async fn len(&self) -> Result<usize> {
        match self {
            StorageDB::Sled(db) => db.len().await,
        }
    }

    /// Gets total storage size in bytes
    #[inline]
    pub async fn db_size(&self) -> Result<usize> {
        match self {
            StorageDB::Sled(db) => db.db_size().await,
        }
    }

    /// Checks if key exists
    #[inline]
    pub async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self {
            StorageDB::Sled(db) => db.contains_key(key).await,
        }
    }

    /// Sets expiration timestamp (requires "ttl" feature)
    #[inline]
    #[cfg(feature = "ttl")]
    pub async fn expire_at<K>(&self, key: K, at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.expire_at(key, at).await,
        }
    }

    /// Sets expiration duration (requires "ttl" feature)
    #[inline]
    #[cfg(feature = "ttl")]
    pub async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.expire(key, dur).await,
        }
    }

    /// Gets time-to-live (requires "ttl" feature)
    #[inline]
    #[cfg(feature = "ttl")]
    pub async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageDB::Sled(db) => db.ttl(key).await,
        }
    }

    /// Iterates over maps
    #[inline]
    pub async fn map_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageMap>> + Send + 'a>> {
        match self {
            StorageDB::Sled(db) => db.map_iter().await,
        }
    }

    /// Iterates over lists
    #[inline]
    pub async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>> {
        match self {
            StorageDB::Sled(db) => db.list_iter().await,
        }
    }

    /// Scans keys matching pattern
    #[inline]
    pub async fn scan<'a, P>(
        &'a mut self,
        pattern: P,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
    {
        match self {
            StorageDB::Sled(db) => db.scan(pattern).await,
        }
    }

    /// Gets storage information
    #[inline]
    pub async fn info(&self) -> Result<serde_json::Value> {
        match self {
            StorageDB::Sled(db) => db.info().await,
        }
    }
}

#[derive(Clone)]
pub enum StorageMap {
    /// Sled map implementation
    Sled(SledStorageMap),
}

#[async_trait]
impl Map for StorageMap {
    fn name(&self) -> &[u8] {
        match self {
            StorageMap::Sled(m) => m.name(),
        }
    }

    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        match self {
            StorageMap::Sled(m) => m.insert(key, val).await,
        }
    }

    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageMap::Sled(m) => m.get(key).await,
        }
    }

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageMap::Sled(m) => m.remove(key).await,
        }
    }

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self {
            StorageMap::Sled(m) => m.contains_key(key).await,
        }
    }

    #[cfg(feature = "map_len")]
    async fn len(&self) -> Result<usize> {
        match self {
            StorageMap::Sled(m) => m.len().await,
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        match self {
            StorageMap::Sled(m) => m.is_empty().await,
        }
    }

    async fn clear(&self) -> Result<()> {
        match self {
            StorageMap::Sled(m) => m.clear().await,
        }
    }

    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageMap::Sled(m) => m.remove_and_fetch(key).await,
        }
    }

    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageMap::Sled(m) => m.remove_with_prefix(prefix).await,
        }
    }

    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        match self {
            StorageMap::Sled(m) => m.batch_insert(key_vals).await,
        }
    }

    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        match self {
            StorageMap::Sled(m) => m.batch_remove(keys).await,
        }
    }

    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        match self {
            StorageMap::Sled(m) => m.iter().await,
        }
    }

    async fn key_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>> {
        match self {
            StorageMap::Sled(m) => m.key_iter().await,
        }
    }

    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        match self {
            StorageMap::Sled(m) => m.prefix_iter(prefix).await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        match self {
            StorageMap::Sled(m) => m.expire_at(at).await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        match self {
            StorageMap::Sled(m) => m.expire(dur).await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        match self {
            StorageMap::Sled(m) => m.ttl().await,
        }
    }
}

#[derive(Clone)]
pub enum StorageList {
    /// Sled list implementation
    Sled(SledStorageList),
}

impl fmt::Debug for StorageList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            StorageList::Sled(list) => list.name(),
        };
        f.debug_tuple(&format!("StorageList({:?})", String::from_utf8_lossy(name)))
            .finish()
    }
}

#[async_trait]
impl List for StorageList {
    fn name(&self) -> &[u8] {
        match self {
            StorageList::Sled(m) => m.name(),
        }
    }

    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        match self {
            StorageList::Sled(list) => list.push(val).await,
        }
    }

    async fn pushs<V>(&self, vals: Vec<V>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        match self {
            StorageList::Sled(list) => list.pushs(vals).await,
        }
    }

    async fn push_limit<V>(
        &self,
        val: &V,
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<V>>
    where
        V: Serialize + Sync + Send,
        V: DeserializeOwned,
    {
        match self {
            StorageList::Sled(list) => list.push_limit(val, limit, pop_front_if_limited).await,
        }
    }

    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageList::Sled(list) => list.pop().await,
        }
    }

    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageList::Sled(list) => list.all().await,
        }
    }

    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageList::Sled(list) => list.get_index(idx).await,
        }
    }

    async fn len(&self) -> Result<usize> {
        match self {
            StorageList::Sled(list) => list.len().await,
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        match self {
            StorageList::Sled(list) => list.is_empty().await,
        }
    }

    async fn clear(&self) -> Result<()> {
        match self {
            StorageList::Sled(list) => list.clear().await,
        }
    }

    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        match self {
            StorageList::Sled(list) => list.iter().await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        match self {
            StorageList::Sled(l) => l.expire_at(at).await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        match self {
            StorageList::Sled(l) => l.expire(dur).await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        match self {
            StorageList::Sled(l) => l.ttl().await,
        }
    }
}
