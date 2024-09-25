#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::Block;
use crate::key::{KeySlice, KeyVec};
use std::cmp::min;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn get_common_prefix(a: &[u8], b: &[u8]) -> u16 {
    let len = min(a.len(), b.len());
    for i in 0..len {
        if a[i] != b[i] {
            return i as u16;
        }
    }
    len as u16
}
impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let mut keykey_overlap_len: u16 = 0;
        let mut rest_key_len: u16 = key.len() as u16;
        if !self.first_key.is_empty() {
            keykey_overlap_len = get_common_prefix(self.first_key.raw_ref(), &key.raw_ref());
            rest_key_len = key.len() as u16 - keykey_overlap_len;
        }
        if !self.data.is_empty()
            && (self.offsets.len() + 1) * 2
                + self.data.len()
                + 4
                + rest_key_len as usize
                + value.len()
                + 2
                > self.block_size
        {
            return false;
        }
        // add to offset
        self.offsets.push(self.data.len() as u16);
        // encode keysize
        let key_overlap_len_bytes = (keykey_overlap_len).to_le_bytes();
        self.data.push(key_overlap_len_bytes[0]);
        self.data.push(key_overlap_len_bytes[1]);

        let rest_key_len_bytes = rest_key_len.to_le_bytes();
        self.data.push(rest_key_len_bytes[0]);
        self.data.push(rest_key_len_bytes[1]);

        //add key
        self.data
            .extend_from_slice(&key.raw_ref()[keykey_overlap_len as usize..]);
        //encode valuesize
        let offset_bytes = (value.len() as u16).to_le_bytes();
        self.data.push(offset_bytes[0]);
        self.data.push(offset_bytes[1]);
        //add value
        self.data.extend_from_slice(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn block_size(&self) -> usize {
        self.data.len() + 2 * self.offsets.len() + 2
    }
}
