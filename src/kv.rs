use std::collections::HashMap;

/// KvStore is a struct that store Key Value pairs
/// /// Example:
/// ```rust
/// use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
#[derive(Debug)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Return a new instance of KvStore
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// Given a `String` key, return the `String` value of the key.
    ///
    /// if key not in KvStore, return None.
    pub fn get(&mut self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    /// Given a `String` key and a `String` value, store the `String` value in the KvStore.
    /// if key already in KvStore, override it.
    ///
    /// Return None if the key was not in the KvStore.
    /// Return the previously value at the key if the key was previously in the KvStore.
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Given a `String` key , remove the `String` key in the KvStore.
    ///
    /// Return the value at the key if the key was previously in the KvStore.
    /// Reruen None if the key was not in the KvStore.
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
