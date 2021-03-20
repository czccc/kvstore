use std::collections::HashMap;

#[derive(Default, Debug, Clone)]
pub struct ReadOnly {
    req_status: HashMap<u64, Vec<Vec<u8>>>,
}

impl ReadOnly {
    pub fn new() -> ReadOnly {
        ReadOnly {
            req_status: HashMap::default(),
        }
    }

    pub fn reset(&mut self) {
        self.req_status.clear();
    }

    pub fn add_request(&mut self, index: u64, req: Vec<u8>) {
        if !self.req_status.contains_key(&index) {
            self.req_status.insert(index, Vec::default());
        }
        self.req_status.get_mut(&index).unwrap().push(req);
    }

    pub fn pop_requests(&mut self, index: u64) -> Vec<Vec<u8>> {
        self.req_status.remove(&index).unwrap_or_default()
    }
}
