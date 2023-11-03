use crate::core::{Bytes, KeyRef, MemTable, MemTableIter, ValueEntry};

use std::{
    collections::{btree_map::IntoIter, BTreeMap},
    ops::Add,
};

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct BTreeMemTable {
    pub map: BTreeMap<Bytes, ValueEntry>,
    mem_size: usize,
}

impl BTreeMemTable {
    pub fn new() -> BTreeMemTable {
        BTreeMemTable {
            map: BTreeMap::new(),
            mem_size: 0usize,
        }
    }

    fn mem_size(&self) -> usize {
        self.mem_size
    }

    fn into_iter(self) -> IntoIter<Bytes, ValueEntry> {
        self.map.into_iter()
    }

    fn iter(&self) -> IntoIter<Bytes, ValueEntry> {
        //TODO: avoid cloning
        self.map.clone().into_iter()
    }

    fn update_memsize(&mut self) {
        self.mem_size = self
            .map
            .iter()
            .map(|(k, v)| {
                k.len()
                    + match v {
                        ValueEntry::Val(bytes) => bytes.len(),
                        ValueEntry::Tombstone => 0usize,
                    }
            })
            .sum();
    }
}

impl Default for BTreeMemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl Add for BTreeMemTable {
    type Output = Self;
    /// Merge two mem tables. Incase of key collisions the values in the newer (right) table
    /// are considered as the latest.
    fn add(mut self, rhs: Self) -> Self::Output {
        self.map.extend(rhs.map);
        self.update_memsize();
        self
    }
}

impl MemTable for BTreeMemTable {
    fn contains(&self, key: KeyRef) -> bool {
        self.map.contains_key(key)
    }

    fn delete(&mut self, key: Bytes) {
        if let Some(val) = self.map.insert(key, ValueEntry::Tombstone) {
            match val {
                ValueEntry::Val(bytes) => self.mem_size -= bytes.len(),
                ValueEntry::Tombstone => {}
            }
        }
    }

    fn get(&self, key: KeyRef) -> Option<&ValueEntry> {
        self.map.get(key)
    }

    fn insert(&mut self, key: Bytes, val: Bytes) {
        if let Some(prev_val) = self.get(&key) {
            let prev_val_size = match prev_val {
                ValueEntry::Tombstone => 0usize,
                ValueEntry::Val(bytes) => bytes.len(),
            };
            self.mem_size += val.len();
            self.mem_size -= prev_val_size;
        } else {
            self.mem_size += key.len() + val.len();
        }

        self.map.insert(key, ValueEntry::Val(val));
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn mem_size(&self) -> usize {
        self.mem_size()
    }

    fn into_iter(self) -> MemTableIter {
        MemTableIter {
            iter: Box::new(self.into_iter()),
        }
    }

    fn iter(&self) -> MemTableIter {
        MemTableIter {
            iter: Box::new(self.iter()),
        }
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use rand::{
        distributions::{Alphanumeric, DistString},
        thread_rng,
    };

    use super::*;

    fn gen_strings(len: usize, num: usize) -> Vec<String> {
        let mut rng = thread_rng();
        let mut strings = vec![];
        for _ in 0..num {
            strings.push(Alphanumeric {}.sample_string(&mut rng, len))
        }
        strings
    }

    #[test]
    fn test_mem_size() {
        let mut mem_table = BTreeMemTable::new();

        assert_eq!(mem_table.mem_size(), 0usize);

        let entries = gen_strings(32, 500);
        for entry in &entries {
            mem_table.insert(entry.clone().into_bytes(), entry.clone().into_bytes());
        }

        assert_eq!(mem_table.mem_size(), 32 * 1000usize);

        for entry in entries {
            mem_table.delete(entry.into_bytes());
        }

        assert_eq!(mem_table.mem_size(), 32 * 500usize);
    }
}
