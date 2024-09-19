#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;
pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buffer = self.data.clone();
        let nums = self.offsets.len();
        for &x in &self.offsets {
            buffer.put_u16(x);
        }
        buffer.put_u16(nums as u16);
        buffer.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let lensum = data.len();
        let nums = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 - SIZEOF_U16 * nums;
        let offsets_row = &data[data_end..data.len() - SIZEOF_U16];
        let offsets = offsets_row
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        let data = data[0..data_end].to_vec();

        Self { data, offsets }
    }
}
