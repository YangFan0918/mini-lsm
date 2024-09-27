#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn new(sstables: Vec<Arc<SsTable>>) -> Self {
        Self {
            current: {
                if sstables.len() > 0 {
                    Some(SsTableIterator::create_and_seek_to_first(sstables[0].clone()).unwrap())
                } else {
                    None
                }
            },
            next_sst_idx: 1,
            sstables,
        }
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let iterator = SstConcatIterator::new(sstables);
        Ok(iterator)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let len = sstables.len();
        if len > 0 {
            let mut l = 0;
            let mut r = len - 1;
            while l < r {
                let mid = (l + r) / 2;
                if sstables[mid].last_key().raw_ref() < key.raw_ref() {
                    l = mid + 1;
                } else {
                    r = mid;
                }
            }
            let iterator = Self {
                current: Some(
                    SsTableIterator::create_and_seek_to_key(sstables[l].clone(), key).unwrap(),
                ),
                next_sst_idx: l + 1,
                sstables,
            };
            return Ok(iterator);
        }
        Ok(Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(v) = &self.current {
            return v.is_valid();
        }
        false
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        if !self.is_valid() {
            if self.next_sst_idx < self.sstables.len() {
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
                self.next_sst_idx += 1;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
