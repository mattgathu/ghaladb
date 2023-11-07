use crate::{
    core::{Bytes, DataPtr, KeyRef, ValueEntry},
    error::GhalaDbResult,
};

use std::{
    collections::{btree_map::IntoIter, BTreeMap},
    ops::Add,
};
pub(crate) trait MemTable {
    fn contains(&self, key: KeyRef) -> bool;
    fn delete(&mut self, key: Bytes);
    fn get(&self, key: KeyRef) -> Option<ValueEntry>;
    fn insert(&mut self, key: Bytes, val: DataPtr);
    fn len(&self) -> usize;
    fn mem_size(&self) -> usize;
    fn iter(&self) -> MemTableIter;
    fn into_iter(self) -> MemTableIter;
    fn is_empty(&self) -> bool;
}
pub(crate) struct MemTableIter {
    pub iter: Box<dyn Iterator<Item = (Bytes, ValueEntry)>>,
}

impl Iterator for MemTableIter {
    type Item = GhalaDbResult<(Bytes, ValueEntry)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(Ok)
    }
}
//TODO make table generic over key and val
// to make refactor easier
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
        self.mem_size = self.map.iter().map(|(k, v)| k.len() + v.mem_sz()).sum();
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
                ValueEntry::Val(dp) => self.mem_size -= dp.mem_sz(),
                ValueEntry::Tombstone => {}
            }
        }
    }

    fn get(&self, key: KeyRef) -> Option<ValueEntry> {
        self.map.get(key).cloned()
    }

    fn insert(&mut self, key: Bytes, val: DataPtr) {
        if let Some(prev_val) = self.get(&key) {
            let prev_val_size = match prev_val {
                ValueEntry::Tombstone => 0usize,
                ValueEntry::Val(dp) => dp.mem_sz(),
            };
            self.mem_size += val.mem_sz();
            self.mem_size -= prev_val_size;
        } else {
            self.mem_size += key.len() + val.mem_sz();
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
    //TODO
}
