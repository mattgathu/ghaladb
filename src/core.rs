use crate::error::GhalaDbResult;

pub type Bytes = Vec<u8>;
pub type KeyRef<'a> = &'a [u8];

pub(crate) trait MemTable {
    fn contains(&self, key: KeyRef) -> bool;
    fn delete(&mut self, key: Bytes);
    fn get(&self, key: KeyRef) -> Option<&ValueEntry>;
    fn insert(&mut self, key: Bytes, val: Bytes);
    fn len(&self) -> usize;
    fn mem_size(&self) -> usize;
    fn iter(&self) -> MemTableIter;
    fn into_iter(self) -> MemTableIter;
    fn is_empty(&self) -> bool;
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ValueEntry {
    Tombstone,
    Val(Bytes),
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
