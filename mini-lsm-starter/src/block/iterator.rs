#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{cmp::Ordering, sync::Arc};

use crate::key::{KeySlice, KeyVec};

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
        block_iterator.seek_to_key(key);
        block_iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        let key_len = u16::from_le_bytes([self.key.raw_ref()[0], self.key.raw_ref()[1]]) as usize;
        KeySlice::from_slice(&self.key.raw_ref()[2..2 + key_len])
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let key_len = u16::from_le_bytes([self.key.raw_ref()[0], self.key.raw_ref()[1]]) as usize;
        let value_len = u16::from_le_bytes([
            self.key.raw_ref()[2 + key_len],
            self.key.raw_ref()[3 + key_len],
        ]) as usize;
        &self.key.raw_ref()[4 + key_len..]
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
        self.key = KeyVec::from_vec(self.block.data[0..end as usize].to_vec());
        self.idx = 0;
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
            self.key = KeyVec::from_vec(self.block.data[start as usize..end as usize].to_vec());
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
