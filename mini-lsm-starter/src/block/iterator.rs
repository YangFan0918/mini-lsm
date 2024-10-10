use std::{cmp::Ordering, sync::Arc};

use crate::key::{KeySlice, KeyVec};
use bytes::Buf;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut block_iterator = BlockIterator::new(block);
        block_iterator.seek_to_first();
        block_iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut block_iterator = BlockIterator::new(block);
        block_iterator.seek_to_first();
        block_iterator.seek_to_key(key);
        block_iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let end = {
            if self.block.offsets.len() == 1 {
                self.block.data.len() as u16
            } else {
                self.block.offsets[1]
            }
        };
        let key_remain_len = u16::from_le_bytes([self.block.data[2], self.block.data[3]]);
        let key_vec = self.block.data[4..4 + key_remain_len as usize].to_vec();
        let ts =
            (&self.block.data[4 + key_remain_len as usize..12 + key_remain_len as usize]).get_u64();
        self.key = KeyVec::from_vec_with_ts(key_vec, ts);
        self.first_key = self.key.clone();
        self.idx = 0;
        self.value_range = ((key_remain_len + 14) as usize, end as usize);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        let now_idx = self.idx;
        if now_idx + 1 >= self.block.offsets.len() {
            self.idx = now_idx + 1;
            self.key = KeyVec::new();
        } else {
            let start = self.block.offsets[now_idx + 1];
            let end = {
                if now_idx + 1 == self.block.offsets.len() - 1 {
                    self.block.data.len() as u16
                } else {
                    self.block.offsets[now_idx + 2]
                }
            };
            self.idx = now_idx + 1;
            let entry = self.block.data[start as usize..end as usize].to_vec();
            let key_overlap_len = u16::from_le_bytes([entry[0], entry[1]]);
            let key_remain_len = u16::from_le_bytes([entry[2], entry[3]]);
            let mut key = self.first_key.key_ref()[..key_overlap_len as usize].to_vec();
            key.extend(&entry[4..4 + key_remain_len as usize]);
            let ts =
                (&entry[4 + key_remain_len as usize..(4 + key_remain_len + 8) as usize]).get_u64();
            self.key = KeyVec::from_vec_with_ts(key, ts);
            self.value_range = ((key_remain_len + 14 + start) as usize, end as usize);
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.is_valid() && self.key().cmp(&key) == Ordering::Less {
            self.next();
        }
    }
}
