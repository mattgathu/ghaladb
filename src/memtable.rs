use serde::{Deserialize, Serialize};

use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

pub trait MemTable {
    fn contains(&self, key: String) -> bool;
    fn delete(&mut self, key: String);
    fn get(&self, key: &str) -> Option<String>;
    fn insert(&mut self, key: String, val: String);
    fn len(&self) -> usize;
    fn size(&self) -> usize;
    fn iter(&self) -> MemTableIter;
    fn is_empty(&self) -> bool {
        self.size() == 0
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum OpType {
    Put = 0,
    Delete = 1,
}

#[derive(Debug, PartialEq)]
pub struct BTreeMemTable {
    pub map: BTreeMap<String, Option<String>>,
    size: usize,
}

impl BTreeMemTable {
    pub fn new() -> BTreeMemTable {
        BTreeMemTable {
            map: BTreeMap::new(),
            size: 0usize,
        }
    }

    fn mem_size(&self) -> usize {
        self.size
    }

    fn iter(&self) -> Iter<String, Option<String>> {
        self.map.iter()
    }
}

impl MemTable for BTreeMemTable {
    fn contains(&self, key: String) -> bool {
        self.map.contains_key(&key)
    }

    fn delete(&mut self, key: String) {
        self.map.insert(key, None);
    }

    fn get(&self, key: &str) -> Option<String> {
        if let Some(v) = self.map.get(key) {
            v.as_deref().map(|s| s.to_string())
        } else {
            None
        }
    }

    fn insert(&mut self, key: String, val: String) {
        self.size += key.len() + val.len();
        self.map.insert(key, Some(val));
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn size(&self) -> usize {
        self.mem_size()
    }

    fn iter(&self) -> MemTableIter {
        MemTableIter { iter: self.iter() }
    }
}

pub struct MemTableIter<'a> {
    iter: Iter<'a, String, Option<String>>,
}

impl<'a> Iterator for MemTableIter<'a> {
    type Item = (String, Option<String>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(k, v)| (k.to_string(), v.as_ref().map(|s| s.to_string())))
    }
}

/// Merge two mem tables. Incase of key collisions the values in the newer (right) table
/// are considered the latest.
#[allow(dead_code)]
pub fn merge_mem_tables(mut old: BTreeMemTable, new: BTreeMemTable) -> BTreeMemTable {
    old.map.extend(new.map);
    old
}
#[cfg(test)]
mod tests {
    //use super::*;
}
