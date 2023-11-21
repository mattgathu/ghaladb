use crate::core::MemSize;
use crate::error::GhalaDbResult;
use std::collections::{btree_map::IntoIter, BTreeMap};

pub(crate) trait MemTable<K, V> {
    fn contains(&self, key: &K) -> bool;
    fn delete(&mut self, key: K);
    fn get(&self, key: impl AsRef<K>) -> Option<V>;
    fn insert(&mut self, key: K, val: V);
    fn len(&self) -> usize;
    fn mem_size(&self) -> usize;
    fn iter(&self) -> Box<dyn Iterator<Item = GhalaDbResult<(K, V)>>>;
    fn into_iter(self) -> Box<dyn Iterator<Item = GhalaDbResult<(K, V)>>>;
    fn is_empty(&self) -> bool;
}
pub(crate) struct MemTableIter<K, V> {
    pub iter: Box<dyn Iterator<Item = (K, V)>>,
}

impl<K, V> Iterator for MemTableIter<K, V> {
    type Item = GhalaDbResult<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(Ok)
    }
}
//TODO make table generic over key and val
// to make refactor easier
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct BTreeMemTable<K, V>
where
    K: Clone,
    V: Clone,
{
    //pub map: BTreeMap<Bytes, ValueEntry>,
    pub map: BTreeMap<K, V>,
    mem_size: usize,
}

impl<K, V> BTreeMemTable<K, V>
where
    K: Clone,
    V: Clone,
{
    pub fn new() -> BTreeMemTable<K, V> {
        BTreeMemTable {
            map: BTreeMap::new(),
            mem_size: 0usize,
        }
    }

    fn mem_size(&self) -> usize {
        self.mem_size
    }

    fn into_iter(self) -> IntoIter<K, V> {
        self.map.into_iter()
    }

    fn iter(&self) -> IntoIter<K, V> {
        //TODO: avoid cloning
        self.map.clone().into_iter()
    }
}

impl<
        K: Clone + Ord + MemSize + AsRef<K> + 'static,
        V: Clone + Default + MemSize + 'static,
    > MemTable<K, V> for BTreeMemTable<K, V>
{
    fn contains(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    fn delete(&mut self, key: K) {
        if let Some(val) = self.map.insert(key, V::default()) {
            let mem_size = val.mem_sz();
            self.mem_size -= mem_size;
        }
    }

    fn get(&self, key: impl AsRef<K>) -> Option<V> {
        self.map.get(key.as_ref()).cloned()
    }

    fn insert(&mut self, key: K, val: V) {
        if let Some(prev_val) = self.get(&key) {
            let prev_val_size = prev_val.mem_sz();
            self.mem_size += val.mem_sz();
            self.mem_size -= prev_val_size;
        } else {
            self.mem_size += key.mem_sz() + val.mem_sz();
        }

        self.map.insert(key, val);
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn mem_size(&self) -> usize {
        self.mem_size()
    }

    fn into_iter(self) -> Box<dyn Iterator<Item = GhalaDbResult<(K, V)>>> {
        let it = MemTableIter {
            iter: Box::new(self.into_iter()),
        };
        Box::new(it)
    }

    fn iter(&self) -> Box<dyn Iterator<Item = GhalaDbResult<(K, V)>>> {
        let it = MemTableIter {
            iter: Box::new(self.iter()),
        };
        Box::new(it)
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    //TODO
}
