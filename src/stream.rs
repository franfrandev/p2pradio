/// Implementation of a buffered stream

use std::num::NonZeroU32;

pub struct BufferWindow {
    data: Vec<u8>,
}

pub struct BufferedStream {
    /// The size of q `BufferWindow.data` in B
    window_size: u64,
    /// The max amount of cached `BufferWindow`
    windows_cached: NonZeroU32,
}

impl BufferedStream {
    pub fn new(window_size: u64, windows_cached: u32) -> BufferedStream {
        todo!()
    }
    
    pub fn buffer(&mut self, window: BufferWindow) {
        // TODO
    }
}