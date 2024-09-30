use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;
use std::collections::HashMap;
#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        let mut size = 0;
        let len = snapshot.levels.len();
        for i in 0..(len - 1) {
            size += snapshot.levels[i].1.len();
        }
        let space_amp_ratio =
            (size as f64) / (snapshot.levels.last().unwrap().1.len() as f64) * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        size = 0;
        for i in 0..(len - 1) {
            size += snapshot.levels[i].1.len();
            let this_tier = snapshot.levels[i + 1].1.len();
            let current_ratio = (size as f64) / (this_tier as f64);
            if current_ratio >= size_ratio_trigger && i + 2 >= self.options.min_merge_width {
                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(i + 2)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: (i + 2 >= len),
                });
            }
        }
        let num_tiers_to_take = snapshot.levels.len() - self.options.num_tiers + 2;
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: snapshot.levels.len() >= num_tiers_to_take,
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut file_need_to_remove = Vec::new();
        let mut tier_to_remove = _task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut flag = false;
        for (tier_id, tier_vec) in &snapshot.levels {
            if let Some(v) = tier_to_remove.remove(tier_id) {
                file_need_to_remove.extend(v);
            } else {
                levels.push((*tier_id, tier_vec.clone()));
            }

            if tier_to_remove.is_empty() && !flag {
                flag = true;
                levels.push((_output[0], _output.to_vec()));
            }
        }
        snapshot.levels = levels;
        (snapshot, file_need_to_remove)
    }
}
