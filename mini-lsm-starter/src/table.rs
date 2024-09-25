#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;
use anyhow::anyhow;
use std::cmp::Ordering;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use self::bloom::Bloom;
use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        buf.put_u32(block_meta.len() as u32);
        for mate in block_meta {
            buf.put_u32(mate.offset as u32);
            buf.put_u16(mate.first_key.len() as u16);
            buf.put_slice(mate.first_key.raw_ref());
            buf.put_u16(mate.last_key.len() as u16);
            buf.put_slice(mate.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        let meta_number = buf.get_u32();
        for _ in 0..meta_number {
            let offset = buf.get_u32() as usize;
            let first_key_size = buf.get_u16();
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_size as usize));
            let last_key_size = buf.get_u16();
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_size as usize));
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let bloom_filter_offset_bytes = file.read(file.1 - 4, 4)?;
        let mut buf = &bloom_filter_offset_bytes[..];
        let bloom_filter_offset = buf.get_u32() as usize;
        let bloom_filter_bytes = file.read(
            bloom_filter_offset as u64,
            file.1 - 4 - bloom_filter_offset as u64,
        )?;

        let bloom_filter = Bloom::decode(&bloom_filter_bytes)?;

        let block_meta_offset_bytes = file.read((bloom_filter_offset as u64) - 4, 4)?;
        let mut buf = &block_meta_offset_bytes[..];
        let block_meta_offset = buf.get_u32() as usize;
        let block_meta_bytes = file.read(
            block_meta_offset as u64,
            (bloom_filter_offset as u64) - 4 - block_meta_offset as u64,
        )?;
        let block_meta = BlockMeta::decode_block_meta(&block_meta_bytes[..]);
        Ok(SsTable {
            id,
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset,
            block_cache,
            bloom: Some(bloom_filter),
            max_ts: 0, // will be changed to latest ts in week 2
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            panic!("error!");
        }
        let begin = self.block_meta[block_idx].offset;
        let end = {
            if block_idx + 1 >= self.block_meta.len() {
                self.block_meta_offset
            } else {
                self.block_meta[block_idx + 1].offset
            }
        };
        Ok(Arc::new(Block::decode(
            &self.file.read(begin as u64, (end - begin) as u64)?,
        )))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let mut l = 0;
        let mut r = self.block_meta.len() - 1;
        while l < r {
            let mid = (l + r + 1) / 2;
            if self.block_meta[mid].first_key.as_key_slice().cmp(&key) != Ordering::Greater {
                l = mid;
            } else {
                r = mid - 1;
            }
        }
        l
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
