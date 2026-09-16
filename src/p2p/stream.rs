//! Implementation of a buffered stream mainly used for reconstructing data coming from GossipSub.

use std::num::NonZeroU32;

pub struct BufferBlock {
    data: Vec<u8>,
}

pub struct BufferedStream {
    /// The size of q `BufferBlock.data` in B
    block_size: u64,
    /// The max amount of cached `BufferBlock`
    blocks_n: NonZeroU32,
}

impl BufferedStream {
    pub fn new(block_size: u64, blocks_n: u32) -> BufferedStream {
        todo!()
    }

    pub fn buffer(&mut self, block: BufferBlock) {
        // TODO
    }
}