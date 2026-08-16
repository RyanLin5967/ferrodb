//! The self-describing page header.
//!
//! Design authority: DESIGN.md section 1 ("Page header").
//!
//! `{ birth_epoch, arena_id, page_type, checksum }` lives **in the page**. A side table mapping
//! page -> birth would itself be COW'd metadata needing its own reclamation, which is the
//! problem it was supposed to solve.
//!
//! On-disk layout, big-endian to match the rest of ferrodb:
//!
//! ```text
//! 0..8    birth_epoch  u64
//! 8..12   arena_id     u32
//! 12..16  checksum     u32   (crc32 over the whole page with these 4 bytes zeroed)
//! 16      page_type    u8
//! 17      flags        u8
//! 18..24  reserved     6 bytes
//! 24..    payload
//! ```

use crate::branch::types::{ArenaId, Epoch};
use crate::error::FerroError;
use crate::storage::disk_manager::PAGE_SIZE;
use crate::wal::log::crc32;

/// Bytes reserved at the front of every COW page. Payload starts here, 8-byte aligned.
pub const PAGE_HEADER_SIZE: usize = 24;

const OFF_BIRTH: usize = 0;
const OFF_ARENA: usize = 8;
const OFF_CHECKSUM: usize = 12;
const OFF_TYPE: usize = 16;
const OFF_FLAGS: usize = 17;

/// What the page holds. Stored as a single byte so a scavenger can classify any page without a
/// catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PageType {
    /// Store-level metadata root.
    Meta,
    BTreeInternal,
    BTreeLeaf,
    Heap,
    Overflow,
    /// Durable `BranchRecord`s.
    BranchCatalog,
    /// The pending-free log.
    FreeLog,
    /// Interned `RunEntity` dictionary.
    Provenance,
    /// Allocated but not yet typed.
    Free,
}

impl PageType {
    pub fn as_u8(&self) -> u8 {
        match self {
            PageType::Meta => 0,
            PageType::BTreeInternal => 1,
            PageType::BTreeLeaf => 2,
            PageType::Heap => 3,
            PageType::Overflow => 4,
            PageType::BranchCatalog => 5,
            PageType::FreeLog => 6,
            PageType::Provenance => 7,
            PageType::Free => 8,
        }
    }

    pub fn from_u8(v: u8) -> Result<Self, FerroError> {
        Ok(match v {
            0 => PageType::Meta,
            1 => PageType::BTreeInternal,
            2 => PageType::BTreeLeaf,
            3 => PageType::Heap,
            4 => PageType::Overflow,
            5 => PageType::BranchCatalog,
            6 => PageType::FreeLog,
            7 => PageType::Provenance,
            8 => PageType::Free,
            other => {
                return Err(FerroError::Cow(format!("unknown page type {}", other)));
            }
        })
    }
}

/// Flag bits in byte 17.
pub mod flags {
    /// The page has never been shared: it was born in this branch's arena after the last fork,
    /// so it may be mutated in place with no copy.
    pub const PRIVATE: u8 = 0b0000_0001;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    /// The epoch at which this page came into existence. Half of every reclamation decision.
    pub birth_epoch: Epoch,
    /// The extent this page was allocated from, hence which branch owns it.
    pub arena_id: ArenaId,
    pub page_type: PageType,
    /// crc32 over the whole page with the checksum field zeroed. Recomputed by
    /// [`stamp_checksum`] at write time, checked by [`verify_checksum`] at read time.
    pub checksum: u32,
    pub flags: u8,
}

impl PageHeader {
    pub fn new(birth_epoch: Epoch, arena_id: ArenaId, page_type: PageType) -> Self {
        PageHeader { birth_epoch, arena_id, page_type, checksum: 0, flags: 0 }
    }

    /// Write the header fields into `page`, zeroing the checksum. Call [`stamp_checksum`] after
    /// the payload is final.
    pub fn write_to(&self, page: &mut [u8; PAGE_SIZE]) {
        page[OFF_BIRTH..OFF_BIRTH + 8].copy_from_slice(&self.birth_epoch.0.to_be_bytes());
        page[OFF_ARENA..OFF_ARENA + 4].copy_from_slice(&self.arena_id.0.to_be_bytes());
        page[OFF_CHECKSUM..OFF_CHECKSUM + 4].copy_from_slice(&0u32.to_be_bytes());
        page[OFF_TYPE] = self.page_type.as_u8();
        page[OFF_FLAGS] = self.flags;
        for b in page.iter_mut().take(PAGE_HEADER_SIZE).skip(OFF_FLAGS + 1) {
            *b = 0;
        }
    }

    /// Parse a header. Does **not** verify the checksum — call [`verify_checksum`] separately so
    /// callers can decide whether a torn page is fatal or repairable.
    pub fn read_from(page: &[u8; PAGE_SIZE]) -> Result<Self, FerroError> {
        Ok(PageHeader {
            birth_epoch: Epoch(u64::from_be_bytes(
                page[OFF_BIRTH..OFF_BIRTH + 8].try_into().unwrap(),
            )),
            arena_id: ArenaId(u32::from_be_bytes(
                page[OFF_ARENA..OFF_ARENA + 4].try_into().unwrap(),
            )),
            checksum: u32::from_be_bytes(page[OFF_CHECKSUM..OFF_CHECKSUM + 4].try_into().unwrap()),
            page_type: PageType::from_u8(page[OFF_TYPE])?,
            flags: page[OFF_FLAGS],
        })
    }

    /// True iff this page may be mutated in place by a writer holding `arena` whose branch last
    /// forked at `fork_epoch`.
    ///
    /// The page must belong to the writer's own arena **and** have been born at or after the
    /// writer's fork epoch. Anything older may be visible to the parent or to a sibling and must
    /// be shadowed instead.
    pub fn is_private_to(&self, arena: ArenaId, fork_epoch: Epoch) -> bool {
        self.arena_id == arena && self.birth_epoch >= fork_epoch
    }
}

/// Recompute and store the page checksum. Call immediately before handing the page to the disk
/// manager.
pub fn stamp_checksum(page: &mut [u8; PAGE_SIZE]) {
    page[OFF_CHECKSUM..OFF_CHECKSUM + 4].copy_from_slice(&0u32.to_be_bytes());
    let crc = crc32(page);
    page[OFF_CHECKSUM..OFF_CHECKSUM + 4].copy_from_slice(&crc.to_be_bytes());
}

/// True iff the stored checksum matches the page contents.
pub fn verify_checksum(page: &[u8; PAGE_SIZE]) -> bool {
    let stored = u32::from_be_bytes(page[OFF_CHECKSUM..OFF_CHECKSUM + 4].try_into().unwrap());
    let mut scratch = *page;
    scratch[OFF_CHECKSUM..OFF_CHECKSUM + 4].copy_from_slice(&0u32.to_be_bytes());
    crc32(&scratch) == stored
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrips_and_leaves_payload_alone() {
        let mut page = [0u8; PAGE_SIZE];
        page[PAGE_HEADER_SIZE] = 0xAB;
        let h = PageHeader::new(Epoch(1234), ArenaId(7), PageType::BTreeLeaf);
        h.write_to(&mut page);
        stamp_checksum(&mut page);
        let back = PageHeader::read_from(&page).unwrap();
        assert_eq!(back.birth_epoch, Epoch(1234));
        assert_eq!(back.arena_id, ArenaId(7));
        assert_eq!(back.page_type, PageType::BTreeLeaf);
        assert_eq!(page[PAGE_HEADER_SIZE], 0xAB);
        assert!(verify_checksum(&page));
    }

    #[test]
    fn checksum_catches_a_flipped_payload_byte() {
        let mut page = [0u8; PAGE_SIZE];
        PageHeader::new(Epoch(1), ArenaId(1), PageType::Heap).write_to(&mut page);
        stamp_checksum(&mut page);
        assert!(verify_checksum(&page));
        page[PAGE_SIZE - 1] ^= 0x01;
        assert!(!verify_checksum(&page));
    }

    #[test]
    fn privacy_needs_both_own_arena_and_post_fork_birth() {
        let h = PageHeader::new(Epoch(100), ArenaId(3), PageType::Heap);
        assert!(h.is_private_to(ArenaId(3), Epoch(100)));
        assert!(h.is_private_to(ArenaId(3), Epoch(50)));
        // born before this branch forked -> may be visible elsewhere, must be shadowed
        assert!(!h.is_private_to(ArenaId(3), Epoch(101)));
        // someone else's arena
        assert!(!h.is_private_to(ArenaId(4), Epoch(1)));
    }

    #[test]
    fn unknown_page_type_is_rejected() {
        let mut page = [0u8; PAGE_SIZE];
        page[OFF_TYPE] = 200;
        assert!(PageHeader::read_from(&page).is_err());
    }
}
