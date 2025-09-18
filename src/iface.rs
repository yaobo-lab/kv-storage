use super::{IterItem, Key, Result, StorageList, StorageMap, TimestampMillis};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
/// Asynchronous iterator trait for storage operations
#[async_trait]
pub trait AsyncIterator {
    type Item;
    /// Fetches the next item from the iterator
    async fn next(&mut self) -> Option<Self::Item>;
}

/// Trait for splitting byte slices (used in sled backend)
pub trait SplitSubslice {
    /// Splits slice at the first occurrence of given subslice
    fn split_subslice(&self, subslice: &[u8]) -> Option<(&[u8], &[u8])>;
}

impl SplitSubslice for [u8] {
    fn split_subslice(&self, subslice: &[u8]) -> Option<(&[u8], &[u8])> {
        self.windows(subslice.len())
            .position(|window| window == subslice)
            .map(|index| self.split_at(index + subslice.len()))
    }
}

/// Core storage database operations
#[async_trait]
#[allow(clippy::len_without_is_empty)]
pub trait IStorageDB: Send + Sync {
    /// Concrete Map type for this storage
    type MapType: Map;

    /// Concrete List type for this storage
    type ListType: List;

    /// Creates or accesses a named map
    async fn map<N: AsRef<[u8]> + Sync + Send>(
        &self,
        name: N,
        expire: Option<TimestampMillis>,
    ) -> Result<Self::MapType>;

    /// Removes an entire map
    async fn map_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Checks if a map exists
    async fn map_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool>;

    /// Creates or accesses a named list
    async fn list<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Result<Self::ListType>;

    /// Removes an entire list
    async fn list_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Checks if a list exists
    async fn list_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool>;

    /// Inserts a key-value pair
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send;

    /// Retrieves a value by key
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send;

    /// Removes a key-value pair
    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Batch insert of multiple key-value pairs
    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send;

    /// Batch removal of keys
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()>;

    /// Increments a counter value
    async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Decrements a counter value
    async fn counter_decr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Gets current counter value
    async fn counter_get<K>(&self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Sets counter to specific value
    async fn counter_set<K>(&self, key: K, val: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Checks if key exists
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool>;

    /// Gets number of items in storage (requires "len" feature)
    #[cfg(feature = "len")]
    async fn len(&self) -> Result<usize>;

    /// Gets total storage size in bytes
    async fn db_size(&self) -> Result<usize>;

    /// Sets expiration timestamp for a key (requires "ttl" feature)
    #[cfg(feature = "ttl")]
    async fn expire_at<K>(&self, key: K, at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Sets expiration duration for a key (requires "ttl" feature)
    #[cfg(feature = "ttl")]
    async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Gets remaining time-to-live for a key (requires "ttl" feature)
    #[cfg(feature = "ttl")]
    async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Iterates over all maps in storage
    async fn map_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageMap>> + Send + 'a>>;

    /// Iterates over all lists in storage
    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>>;

    /// Scans keys matching pattern (supports * and ? wildcards)
    async fn scan<'a, P>(
        &'a mut self,
        pattern: P,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync;

    /// Gets storage backend information
    async fn info(&self) -> Result<serde_json::Value>;
}

/// Map (dictionary) storage operations
#[async_trait]
pub trait Map: Sync + Send {
    /// Gets the name of this map
    fn name(&self) -> &[u8];

    /// Inserts a key-value pair into the map
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send + ?Sized;

    /// Retrieves a value from the map
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send;

    /// Removes a key from the map
    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Checks if key exists in the map
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool>;

    /// Gets number of items in map (requires "map_len" feature)
    #[cfg(feature = "map_len")]
    async fn len(&self) -> Result<usize>;

    /// Checks if map is empty
    async fn is_empty(&self) -> Result<bool>;

    /// Clears all entries in the map
    async fn clear(&self) -> Result<()>;

    /// Removes a key and returns its value
    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send;

    /// Removes all keys with given prefix
    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    /// Batch insert of key-value pairs
    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send;

    /// Batch removal of keys
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()>;

    /// Iterates over all key-value pairs
    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static;

    /// Iterates over all keys
    async fn key_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>>;

    /// Iterates over key-value pairs with given prefix
    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
        V: DeserializeOwned + Sync + Send + 'a + 'static;

    /// Sets expiration timestamp for the entire map (requires "ttl" feature)
    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool>;

    /// Sets expiration duration for the entire map (requires "ttl" feature)
    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool>;

    /// Gets remaining time-to-live for the map (requires "ttl" feature)
    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>>;
}

/// List storage operations
#[async_trait]
pub trait List: Sync + Send {
    /// Gets the name of this list
    fn name(&self) -> &[u8];

    /// Appends a value to the end of the list
    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send;

    /// Appends multiple values to the list
    async fn pushs<V>(&self, vals: Vec<V>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send;

    /// Pushes with size limit and optional pop-front behavior
    async fn push_limit<V>(
        &self,
        val: &V,
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<V>>
    where
        V: serde::ser::Serialize + Sync + Send,
        V: DeserializeOwned;

    /// Removes and returns the first value in the list
    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send;

    /// Retrieves all values in the list
    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send;

    /// Gets value by index
    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send;

    /// Gets number of items in the list
    async fn len(&self) -> Result<usize>;

    /// Checks if list is empty
    async fn is_empty(&self) -> Result<bool>;

    /// Clears all items from the list
    async fn clear(&self) -> Result<()>;

    /// Iterates over all values
    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static;

    /// Sets expiration timestamp for the entire list (requires "ttl" feature)
    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool>;

    /// Sets expiration duration for the entire list (requires "ttl" feature)
    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool>;

    /// Gets remaining time-to-live for the list (requires "ttl" feature)
    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>>;
}
