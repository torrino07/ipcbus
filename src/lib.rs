use core::sync::atomic::{AtomicU64, Ordering};
use shared_memory::{Shmem, ShmemConf};
use std::ffi::CString;

pub const NUM_EXCHANGES: usize = 5;
pub const NUM_MARKETS: usize = 2;
pub const NUM_SYMBOLS: usize = 30;
pub const NUM_CHANNELS: usize = 2;
pub const NUM_TOPICS: usize = NUM_EXCHANGES * NUM_MARKETS * NUM_SYMBOLS * NUM_CHANNELS; // 600
pub const SLOTS_PER_TOPIC: usize = 10;
pub const JOURNAL_SIZE: usize = NUM_TOPICS * SLOTS_PER_TOPIC;
pub const DATA_SIZE: usize = 1024;

pub const BITWORDS: usize = (NUM_TOPICS + 63) / 64; // 600 -> 10 u64 words

#[repr(C)]
#[derive(Clone, Debug, Copy)]
pub struct Message {
    pub seq: u64,
    pub data: [u8; DATA_SIZE],
    pub data_len: u32,
}

impl Message {
    #[inline]
    pub fn get_text(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.data[..self.data_len as usize]) }
    }
}

impl Default for Message {
    fn default() -> Self {
        Message { seq: 0, data: [0u8; DATA_SIZE], data_len: 0 }
    }
}

#[repr(C)]
pub struct Journal {
    pub slots: [Message; JOURNAL_SIZE],
    /// Atomic bitmap split into BITWORDS 64-bit words.
    /// Each bit i corresponds to topic_id i (0..NUM_TOPICS-1).
    /// NOTE: We store as raw u64s to keep repr(C) predictable; we use AtomicU64 via pointer casts.
    pub pending_bits: [u64; BITWORDS],
}

unsafe impl Send for Bus {}

pub struct Bus {
    pub shmem: Option<Shmem>,
    pub journal: *mut Journal,

    // POSIX named semaphore for notifications
    sem: *mut libc::sem_t,
}

impl Bus {
    pub fn open_or_create(name: &str) -> Self {
        const MEM_SIZE: usize = core::mem::size_of::<Journal>();

        let (shmem, created) = match ShmemConf::new().size(MEM_SIZE).os_id(name).create() {
            Ok(s) => (s, true),
            Err(_) => (
                ShmemConf::new().size(MEM_SIZE).os_id(name).open()
                    .expect("Cannot create/open shared memory"),
                false
            ),
        };

        let ptr = shmem.as_ptr() as *mut Journal;

        // Only on first creation, zero the region to ensure clean seq/pending bits.
        if created {
            unsafe { core::ptr::write_bytes(ptr as *mut u8, 0, MEM_SIZE); }
        }

        // POSIX named semaphore (works on macOS & Linux). Name MUST start with '/'.
        let sem_name = CString::new(format!("/{}-notify", name)).unwrap();
        let sem = unsafe { libc::sem_open(sem_name.as_ptr(), libc::O_CREAT, 0o666, 0) };
        if sem == libc::SEM_FAILED {
            panic!("sem_open failed: {}", std::io::Error::last_os_error());
        }

        Self { shmem: Some(shmem), journal: ptr, sem }
    }

    #[inline]
    pub fn write(&self, topic_id: usize, seq: u64, data: &[u8]) {
        unsafe {
            let journal = &mut *self.journal;
            let topic_offset = topic_id * SLOTS_PER_TOPIC;
            let idx = topic_offset + (seq as usize % SLOTS_PER_TOPIC);

            let mut msg = Message::default();
            msg.seq = seq;
            let len = data.len().min(DATA_SIZE);
            msg.data[..len].copy_from_slice(&data[..len]);
            msg.data_len = len as u32;

            // Store the payload before publishing
            journal.slots[idx] = msg;
        }
    }

    #[inline]
    pub fn read(&self, topic_id: usize, seq: u64) -> Option<Message> {
        unsafe {
            let journal = &*self.journal;
            let topic_offset = topic_id * SLOTS_PER_TOPIC;
            let idx = topic_offset + (seq as usize % SLOTS_PER_TOPIC);
            let msg = journal.slots[idx];
            if msg.seq == seq { Some(msg) } else { None }
        }
    }

    #[inline]
    pub fn get_latest_seq(&self, topic_id: usize) -> u64 {
        unsafe {
            let journal = &*self.journal;
            let topic_offset = topic_id * SLOTS_PER_TOPIC;
            let mut latest = 0u64;
            for i in 0..SLOTS_PER_TOPIC {
                let msg = journal.slots[topic_offset + i];
                if msg.seq > latest { latest = msg.seq; }
            }
            latest
        }
    }

    /// Producer: mark topic as pending and (maybe) post the semaphore.
    #[inline]
    pub fn notify(&self, topic_id: usize) {
        debug_assert!(topic_id < NUM_TOPICS);
        let word_idx = topic_id / 64;
        let bit = 1u64 << (topic_id % 64);

        unsafe {
            // Interpret the pending_bits word as AtomicU64
            let journal = &*self.journal;
            let word_ptr = journal.pending_bits.as_ptr().add(word_idx) as *const AtomicU64;
            let prev = (*word_ptr).fetch_or(bit, Ordering::Release);

            // Only post when transitioning 0 -> 1 for this bit (prevents semaphore overshoot).
            if (prev & bit) == 0 {
                libc::sem_post(self.sem);
            }
        }
    }

    /// Consumer: block until any topic is pending.
    #[inline]
    pub fn wait(&self) {
        unsafe {
            // Will block until someone sem_post()s.
            libc::sem_wait(self.sem);
        }
    }

    /// Consumer: non-blocking try-wait; returns true if it consumed a token.
    #[inline]
    pub fn try_wait(&self) -> bool {
        let rc = unsafe { libc::sem_trywait(self.sem) };
        rc == 0
    }

    #[inline]
    pub fn drain_pending_mask<F: FnMut(usize)>(&self, mask: &[u64; BITWORDS], mut on_topic: F) {
        unsafe {
            let journal = &*self.journal;
            for w in 0..BITWORDS {
                // take pending
                let word_atomic = &*(journal.pending_bits.as_ptr().add(w) as *const AtomicU64);
                let mut bits = word_atomic.swap(0, Ordering::Acquire);
                // keep only subscribed bits
                bits &= mask[w];

                while bits != 0 {
                    let tz = bits.trailing_zeros() as usize;
                    let topic_id = w * 64 + tz;
                    on_topic(topic_id);
                    bits &= bits - 1;
                }
            }
        }
    }

    pub fn wait_and_drain_mask<F: FnMut(usize)>(&self, mask: &[u64; BITWORDS], mut on_topic: F) {
        self.wait();
        self.drain_pending_mask(mask, &mut on_topic);
        while self.try_wait() {
            self.drain_pending_mask(mask, &mut on_topic);
        }
    }
}

impl Drop for Bus {
    fn drop(&mut self) {
        unsafe {
            if !self.sem.is_null() {
                libc::sem_close(self.sem);
                // Optional: libc::sem_unlink(c_name.as_ptr()) if you manage lifecycle externally.
            }
        }
    }
}