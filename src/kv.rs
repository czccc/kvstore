use std::collections::HashMap;

#[derive(Debug)]
pub struct KvStore {
    map: HashMap<String, String>
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new()
        }
    }
    pub fn get(&mut self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}