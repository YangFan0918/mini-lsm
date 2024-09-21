#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{Ok, Result};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn new(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        let sst_iterator = Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(block),
            blk_idx: 0,
        };
        Ok(sst_iterator)
    }

    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        SsTableIterator::new(table)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block_cached(0)?;
        self.blk_idx = 0;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut sst_iterator = SsTableIterator::new(table)?;
        sst_iterator.seek_to_key(key)?;
        Ok(sst_iterator)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let block_idx = self.table.find_block_idx(key);
        let block = self.table.read_block_cached(block_idx)?;
        self.blk_idx = block_idx;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        if !self.blk_iter.is_valid() {
            self.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            let nowblk_idx = self.blk_idx;
            if nowblk_idx + 1 >= self.table.block_meta.len() {
                return Ok(());
            }
            self.blk_idx += 1;
            self.blk_iter = BlockIterator::create_and_seek_to_first(
                self.table.read_block_cached(self.blk_idx)?,
            );
        }
        Ok(())
    }
}
