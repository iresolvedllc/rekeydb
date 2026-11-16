//! MVCC timestamps.
//!
//! KeyDB packs milliseconds and a logical counter into one `u64`:
//! `(unix_ms << 20) | counter`. The counter keeps timestamps strictly
//! monotonic when multiple commands execute in the same millisecond, and the
//! timestamp is persisted in RDB files and used by active replication to
//! reject stale updates.

use std::sync::atomic::{AtomicU64, Ordering};

/// Number of low bits reserved for the logical counter.
pub const MVCC_MS_SHIFT: u32 = 20;

/// A process-wide MVCC clock.
#[derive(Debug)]
pub struct MvccClock {
    value: AtomicU64,
}

impl Default for MvccClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl MvccClock {
    pub fn new(now_ms: i64) -> Self {
        let now_ms = now_ms.max(0) as u64;
        Self {
            value: AtomicU64::new(now_ms << MVCC_MS_SHIFT),
        }
    }

    /// Return the current timestamp without advancing the clock.
    pub fn current(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    /// Advance the clock and return a strictly increasing timestamp.
    pub fn next(&self, now_ms: i64) -> u64 {
        let now_ms = now_ms.max(0) as u64;
        loop {
            let previous = self.value.load(Ordering::Acquire);
            let previous_ms = previous >> MVCC_MS_SHIFT;
            let next = if previous_ms >= now_ms {
                previous.saturating_add(1)
            } else {
                now_ms << MVCC_MS_SHIFT
            };
            match self.value.compare_exchange_weak(
                previous,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return next,
                Err(_) => continue,
            }
        }
    }

    /// Extract the millisecond component of a timestamp.
    pub fn timestamp_ms(timestamp: u64) -> u64 {
        timestamp >> MVCC_MS_SHIFT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advances_with_time() {
        let clock = MvccClock::new(1000);
        assert_eq!(MvccClock::timestamp_ms(clock.next(1001)), 1001);
        assert_eq!(MvccClock::timestamp_ms(clock.next(2000)), 2000);
    }

    #[test]
    fn monotonic_within_same_ms() {
        let clock = MvccClock::new(1000);
        let a = clock.next(1000);
        let b = clock.next(1000);
        let c = clock.next(1000);
        assert!(a < b && b < c);
        assert_eq!(MvccClock::timestamp_ms(a), 1000);
        assert_eq!(MvccClock::timestamp_ms(c), 1000);
    }

    #[test]
    fn ignores_regressing_wall_clock() {
        let clock = MvccClock::new(2000);
        let previous = clock.next(2000);
        let next = clock.next(1000);
        assert!(next > previous);
        assert_eq!(MvccClock::timestamp_ms(next), 2000);
    }
}
