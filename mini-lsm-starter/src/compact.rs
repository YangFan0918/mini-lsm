#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;
use crate::key::KeySlice;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut new_sst = Vec::new();
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        while iter.is_valid() {
            if iter.value().is_empty() {
                iter.next()?;
                continue;
            }
            sst_builder.add(iter.key(), iter.value());
            if sst_builder.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                new_sst.push(Arc::new(sst_builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?));
                sst_builder = SsTableBuilder::new(self.options.block_size);
            }
            iter.next()?;
        }

        if sst_builder.estimated_size() > 5 {
            let sst_id = self.next_sst_id();
            new_sst.push(Arc::new(sst_builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?));
        }
        Ok(new_sst)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_iterator = Vec::new();
                for x in l0_sstables {
                    l0_iterator.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(x).unwrap().clone(),
                    )?));
                }
                let mut l1_iterator = Vec::new();
                for x in l1_sstables {
                    l1_iterator.push(snapshot.sstables.get(x).unwrap().clone());
                }
                let two_merge_iterator = TwoMergeIterator::create(
                    MergeIterator::create(l0_iterator),
                    SstConcatIterator::create_and_seek_to_first(l1_iterator)?,
                )?;
                self.compact_generate_sst_from_iter(two_merge_iterator)
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                lower_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            }) => match upper_level {
                Some(_) => {
                    let mut upper_ierator = Vec::with_capacity(upper_level_sst_ids.len());
                    for x in upper_level_sst_ids.iter() {
                        upper_ierator.push(snapshot.sstables.get(x).unwrap().clone());
                    }
                    let mut lower_iterator = Vec::with_capacity(lower_level_sst_ids.len());
                    for x in lower_level_sst_ids.iter() {
                        lower_iterator.push(snapshot.sstables.get(x).unwrap().clone());
                    }
                    let two_merge_iterator = TwoMergeIterator::create(
                        SstConcatIterator::create_and_seek_to_first(upper_ierator)?,
                        SstConcatIterator::create_and_seek_to_first(lower_iterator)?,
                    )?;
                    self.compact_generate_sst_from_iter(two_merge_iterator)
                }
                None => {
                    let mut upper_ierator = Vec::with_capacity(upper_level_sst_ids.len());
                    for x in upper_level_sst_ids.iter() {
                        upper_ierator.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(x).unwrap().clone(),
                        )?));
                    }
                    let mut lower_iterator = Vec::with_capacity(lower_level_sst_ids.len());
                    for x in lower_level_sst_ids.iter() {
                        lower_iterator.push(snapshot.sstables.get(x).unwrap().clone());
                    }
                    let two_merge_iterator = TwoMergeIterator::create(
                        MergeIterator::create(upper_ierator),
                        SstConcatIterator::create_and_seek_to_first(lower_iterator)?,
                    )?;
                    self.compact_generate_sst_from_iter(two_merge_iterator)
                }
            },
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included,
            }) => {
                let mut iterator = Vec::new();
                for (_, vec) in tiers {
                    let mut sst_id = Vec::new();
                    for id in vec {
                        sst_id.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    iterator.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        sst_id,
                    )?));
                }
                let merge_iterator = MergeIterator::create(iterator);
                self.compact_generate_sst_from_iter(merge_iterator)
            }
            CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            }) => match upper_level {
                Some(_) => {
                    let mut upper_ierator = Vec::with_capacity(upper_level_sst_ids.len());
                    for x in upper_level_sst_ids.iter() {
                        upper_ierator.push(snapshot.sstables.get(x).unwrap().clone());
                    }
                    let mut lower_iterator = Vec::with_capacity(lower_level_sst_ids.len());
                    for x in lower_level_sst_ids.iter() {
                        lower_iterator.push(snapshot.sstables.get(x).unwrap().clone());
                    }
                    let two_merge_iterator = TwoMergeIterator::create(
                        SstConcatIterator::create_and_seek_to_first(upper_ierator)?,
                        SstConcatIterator::create_and_seek_to_first(lower_iterator)?,
                    )?;
                    self.compact_generate_sst_from_iter(two_merge_iterator)
                }
                None => {
                    let mut upper_ierator = Vec::with_capacity(upper_level_sst_ids.len());
                    for x in upper_level_sst_ids.iter() {
                        upper_ierator.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(x).unwrap().clone(),
                        )?));
                    }
                    let mut lower_iterator = Vec::with_capacity(lower_level_sst_ids.len());
                    for x in lower_level_sst_ids.iter() {
                        lower_iterator.push(snapshot.sstables.get(x).unwrap().clone());
                    }
                    let two_merge_iterator = TwoMergeIterator::create(
                        MergeIterator::create(upper_ierator),
                        SstConcatIterator::create_and_seek_to_first(lower_iterator)?,
                    )?;
                    self.compact_generate_sst_from_iter(two_merge_iterator)
                }
            },
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let sst_to_compact = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let sst_to_compact_copy: (Vec<usize>, Vec<usize>) = sst_to_compact.clone();
        let new_sst = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: (sst_to_compact.0),
            l1_sstables: (sst_to_compact.1),
        })?;
        let mut new_sst_id = Vec::new();
        for sst in &new_sst {
            new_sst_id.push(sst.sst_id());
        }
        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();

            for _ in 0..sst_to_compact_copy.0.len() {
                snapshot.l0_sstables.pop();
            }
            snapshot.levels[0].1 = new_sst_id;
            for sst_id in &sst_to_compact_copy.0 {
                let entry = snapshot.sstables.get(sst_id);
                if let Some(value) = entry {
                    std::fs::remove_file(self.path_of_sst(*sst_id))?;
                }
                snapshot.sstables.remove(sst_id);
            }
            for sst_id in &sst_to_compact_copy.1 {
                let entry = snapshot.sstables.get(sst_id);
                if let Some(value) = entry {
                    std::fs::remove_file(self.path_of_sst(*sst_id))?;
                }
                snapshot.sstables.remove(sst_id);
            }
            for sst in &new_sst {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            }

            *state = Arc::new(snapshot);
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        if let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        {
            println!("running compaction task: {:?}", task);
            let new_sst = self.compact(&task)?;
            let new_sst_id = new_sst.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();

            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            for sst in new_sst {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            }
            let (mut snapshot, file_need_to_move) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &new_sst_id, false);
            for x in file_need_to_move {
                std::fs::remove_file(self.path_of_sst(x))?;
                snapshot.sstables.remove(&x);
            }
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
        }
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let guard = self.state.read();
            guard.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
