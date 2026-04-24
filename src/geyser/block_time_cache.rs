use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// Caches slot → block_time_ms from BlockMeta messages.
/// Uses BTreeMap for ordered iteration during eviction.
/// Kept small — Solana produces ~216k slots/day; at 1000 slots
/// this is <8KB of memory.
pub struct BlockTimeCache {
    inner: RwLock<BTreeMap<u64, i64>>,
    max_slots: usize,
}

impl BlockTimeCache {
    pub fn new(max_slots: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(BTreeMap::new()),
            max_slots,
        })
    }

    pub fn insert(&self, slot: u64, block_time_ms: i64) {
        let mut map = self.inner.write().unwrap();
        map.insert(slot, block_time_ms);
        // Evict oldest entries beyond capacity
        while map.len() > self.max_slots {
            // BTreeMap is sorted ascending — pop_first removes oldest slot
            map.pop_first();
        }
    }

    pub fn get(&self, slot: u64) -> Option<i64> {
        self.inner.read().unwrap().get(&slot).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_evicts_oldest_entries() {
        let cache = BlockTimeCache::new(3);

        cache.insert(1, 1000);
        cache.insert(2, 2000);
        cache.insert(3, 3000);

        assert_eq!(cache.get(1), Some(1000));
        assert_eq!(cache.get(2), Some(2000));
        assert_eq!(cache.get(3), Some(3000));

        // Insert 4th entry - should evict slot 1 (oldest)
        cache.insert(4, 4000);

        assert_eq!(cache.get(1), None); // Evicted
        assert_eq!(cache.get(2), Some(2000));
        assert_eq!(cache.get(3), Some(3000));
        assert_eq!(cache.get(4), Some(4000));
    }

    #[test]
    fn cache_returns_none_for_missing_slot() {
        let cache = BlockTimeCache::new(100);
        assert_eq!(cache.get(999), None);
    }

    #[test]
    fn cache_overwrites_existing_slot() {
        let cache = BlockTimeCache::new(10);

        cache.insert(5, 1000);
        cache.insert(5, 2000);

        assert_eq!(cache.get(5), Some(2000));
    }
}
