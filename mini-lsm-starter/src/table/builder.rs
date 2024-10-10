#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;
use bytes::BufMut;
use std::path::Path;
use std::sync::Arc;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hash: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hash: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            self.finish_block();
            if !self.builder.add(key, value) {
                panic!("error!!!");
            }
        }
        self.key_hash.push(farmhash::fingerprint32(&key.key_ref()));
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        self.last_key = key.to_key_vec();
    }

    pub fn finish_block(&mut self) {
        let block_builder =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = block_builder.build();
        let block_mate = BlockMeta {
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
            offset: self.data.len(),
        };
        self.meta.push(block_mate);
        self.first_key.clear();
        self.last_key.clear();
        self.data.extend_from_slice(&block.encode());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.builder.block_size()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if self.builder.block_size() > 0 {
            self.finish_block();
        }
        let mut buf = self.data;
        let meta_block_offset = buf.len();
        BlockMeta::encode_block_meta(self.meta.as_slice(), &mut buf);
        buf.put_u32(meta_block_offset as u32);
        let bloom_filter_offset = buf.len();
        let bloom_filter = Bloom::build_from_key_hashes(
            &self.key_hash,
            Bloom::bloom_bits_per_key(self.key_hash.len(), 0.01),
        );
        bloom_filter.encode(&mut buf);
        buf.put_u32(bloom_filter_offset as u32);
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            id,
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_meta_offset: meta_block_offset,
            block_cache,
            bloom: Some(bloom_filter),
            max_ts: 0, // will be changed to latest ts in week 2
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
