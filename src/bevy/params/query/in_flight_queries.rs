use bevy::prelude::Resource;
use std::collections::HashSet;
use std::sync::Mutex;

#[derive(Resource, Default)]
pub struct InFlightQueries {
    hashes: Mutex<HashSet<u64>>,
}

impl InFlightQueries {
    pub fn insert_if_absent(&self, hash: u64) -> bool {
        let mut guard = self.hashes.lock().unwrap();
        if guard.contains(&hash) {
            false
        } else {
            guard.insert(hash);
            true
        }
    }

    pub fn remove(&self, hash: u64) {
        self.hashes.lock().unwrap().remove(&hash);
    }
}
