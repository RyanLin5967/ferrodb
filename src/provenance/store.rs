//! The interning store behind [`ProvId`], and the page-local dictionary that makes it cheap.
//!
//! Design authority: DESIGN.md section 2, exit criterion 9.
//!
//! The whole point of the design is that a version does **not** carry the actor tuple. The actor
//! tuple has *run-level* cardinality: every row a run writes carries the same
//! `{agent, run, model, model_version, prompt_hash, started_at, parent_branch}`. Storing it
//! literally per version is a large density loss for zero information.
//!
//! So attribution is two indirections, both small:
//!
//! 1. a **page-local dictionary**: each page holds a short `Vec<ProvId>` and each stamped slot
//!    holds one `u8` index into it. That is the per-version cost — one byte.
//! 2. the **global intern table**: `ProvId -> RunEntity`, one entry per run, not per row.
//!
//! Two guards are enforced rather than documented:
//!
//! - a page dictionary is capped at 255 entries and **refuses** a 256th rather than silently
//!   widening the per-version slot;
//! - re-interning the same `(agent_id, run_id)` with a *different* actor tuple is a hard error.
//!   A run's model cannot change mid-run, so a disagreement means the caller is attributing two
//!   different things to one run id, and quietly picking one of them would corrupt every
//!   provenance answer downstream.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::error::FerroError;
use crate::provenance::{ProvId, ProvenanceStore, RunEntity};
use crate::storage::heap_file_manager::RecordId;

/// Maximum distinct runs whose versions may live on one page. Beyond this the per-version slot
/// would need to grow past a byte, so the store refuses instead.
pub const MAX_PAGE_DICT_ENTRIES: usize = 255;

/// Bytes a stamped version spends on provenance under this design: one dictionary index.
pub const PROV_SLOT_BYTES: usize = 1;

/// A page's provenance dictionary: a handful of `ProvId`s, plus one byte per stamped slot.
#[derive(Debug, Clone, Default)]
pub struct PageProvDict {
    /// Local index -> global slot. Short by construction: it holds runs, not rows.
    entries: Vec<ProvId>,
    /// Slot number -> local index into `entries`.
    slots: HashMap<u16, u8>,
}

impl PageProvDict {
    pub fn new() -> Self {
        PageProvDict::default()
    }

    /// Intern `id` into this page's dictionary, returning the local index stored per version.
    pub fn intern_local(&mut self, id: ProvId) -> Result<u8, FerroError> {
        if let Some(i) = self.entries.iter().position(|e| *e == id) {
            return Ok(i as u8);
        }
        if self.entries.len() >= MAX_PAGE_DICT_ENTRIES {
            return Err(FerroError::Provenance(format!(
                "page provenance dictionary full ({} entries); refusing to widen the per-version slot",
                MAX_PAGE_DICT_ENTRIES
            )));
        }
        self.entries.push(id);
        Ok((self.entries.len() - 1) as u8)
    }

    pub fn stamp(&mut self, slot_num: u16, id: ProvId) -> Result<(), FerroError> {
        let local = self.intern_local(id)?;
        self.slots.insert(slot_num, local);
        Ok(())
    }

    /// The local index stored with the version, or `None` if the slot was never stamped.
    pub fn local_of(&self, slot_num: u16) -> Option<u8> {
        self.slots.get(&slot_num).copied()
    }

    pub fn prov_of(&self, slot_num: u16) -> ProvId {
        match self.local_of(slot_num) {
            Some(l) => self.entries.get(l as usize).copied().unwrap_or(ProvId::NONE),
            None => ProvId::NONE,
        }
    }

    /// Number of distinct runs referenced from this page.
    pub fn dictionary_len(&self) -> usize {
        self.entries.len()
    }

    /// Number of versions on this page carrying attribution.
    pub fn stamped_versions(&self) -> usize {
        self.slots.len()
    }

    /// Bytes this page spends on provenance: the dictionary plus one byte per stamped version.
    pub fn footprint_bytes(&self) -> usize {
        self.entries.len() * std::mem::size_of::<ProvId>() + self.slots.len() * PROV_SLOT_BYTES
    }
}

#[derive(Debug, Default)]
struct Inner {
    /// `ProvId(n)` is `runs[n - 1]`; `ProvId(0)` is reserved for "unattributed".
    runs: Vec<RunEntity>,
    by_run_key: HashMap<(String, String), ProvId>,
    pages: HashMap<u32, PageProvDict>,
}

/// In-memory [`ProvenanceStore`]. Durability of the dictionary belongs to the page format; this
/// holds the same shape so the layers above it are exercised for real.
#[derive(Debug, Default)]
pub struct MemProvenanceStore {
    inner: RwLock<Inner>,
}

impl MemProvenanceStore {
    pub fn new() -> Self {
        MemProvenanceStore::default()
    }

    fn poisoned() -> FerroError {
        FerroError::Provenance("provenance store lock poisoned".into())
    }

    /// The exit-criterion-9 answer in one call: which agent + run + model wrote this version.
    pub fn who_wrote(&self, rid: RecordId) -> Result<RunEntity, FerroError> {
        let id = self.attribute(rid)?;
        if id.is_none() {
            return Err(FerroError::Provenance(format!(
                "no provenance recorded for page {} slot {}",
                rid.page_id, rid.slot_num
            )));
        }
        self.lookup(id)
    }

    /// One-line human answer, or an explicit "unattributed".
    pub fn describe_row(&self, rid: RecordId) -> String {
        match self.who_wrote(rid) {
            Ok(r) => r.describe(),
            Err(_) => "unattributed".to_string(),
        }
    }

    /// Every version this run wrote, ordered. The reverse direction of criterion 9: not "who
    /// wrote this row" but "what did this run touch".
    pub fn rows_written_by(&self, id: ProvId) -> Result<Vec<RecordId>, FerroError> {
        let inner = self.inner.read().map_err(|_| Self::poisoned())?;
        let mut out = Vec::new();
        for (page_id, dict) in inner.pages.iter() {
            for slot in dict.slots.keys() {
                if dict.prov_of(*slot) == id {
                    out.push(RecordId { page_id: *page_id, slot_num: *slot });
                }
            }
        }
        out.sort_by_key(|r| (r.page_id, r.slot_num));
        Ok(out)
    }

    /// Distinct runs interned so far.
    pub fn run_count(&self) -> usize {
        self.inner.read().map(|i| i.runs.len()).unwrap_or(0)
    }

    pub fn runs(&self) -> Result<Vec<RunEntity>, FerroError> {
        let inner = self.inner.read().map_err(|_| Self::poisoned())?;
        Ok(inner.runs.clone())
    }

    /// How many distinct runs a page's dictionary references.
    pub fn page_dictionary_len(&self, page_id: u32) -> usize {
        self.inner
            .read()
            .ok()
            .and_then(|i| i.pages.get(&page_id).map(|d| d.dictionary_len()))
            .unwrap_or(0)
    }

    /// Bytes actually spent on provenance across every page.
    pub fn footprint_bytes(&self) -> usize {
        self.inner
            .read()
            .map(|i| i.pages.values().map(|d| d.footprint_bytes()).sum())
            .unwrap_or(0)
    }

    /// What the same attribution would have cost stored literally in every version header.
    pub fn literal_footprint_bytes(&self) -> Result<usize, FerroError> {
        let inner = self.inner.read().map_err(|_| Self::poisoned())?;
        let mut total = 0usize;
        for dict in inner.pages.values() {
            for slot in dict.slots.keys() {
                let id = dict.prov_of(*slot);
                if let Some(run) = inner.runs.get(id.0 as usize - 1) {
                    total += run.literal_footprint();
                }
            }
        }
        Ok(total)
    }
}

impl ProvenanceStore for MemProvenanceStore {
    fn intern(&self, run: &RunEntity) -> Result<ProvId, FerroError> {
        let key = (run.agent_id.clone(), run.run_id.clone());
        let mut inner = self.inner.write().map_err(|_| Self::poisoned())?;
        if let Some(existing) = inner.by_run_key.get(&key).copied() {
            let stored = &inner.runs[existing.0 as usize - 1];
            if !stored.same_actor(run) {
                return Err(FerroError::Provenance(format!(
                    "run {}/{} already interned as {} — refusing to re-intern with a different actor tuple",
                    run.agent_id,
                    run.run_id,
                    stored.describe()
                )));
            }
            return Ok(existing);
        }
        let id = ProvId(inner.runs.len() as u32 + 1);
        let mut stored = run.clone();
        stored.prov_id = id;
        inner.runs.push(stored);
        inner.by_run_key.insert(key, id);
        Ok(id)
    }

    fn lookup(&self, id: ProvId) -> Result<RunEntity, FerroError> {
        if id.is_none() {
            return Err(FerroError::Provenance("ProvId::NONE names no run".into()));
        }
        let inner = self.inner.read().map_err(|_| Self::poisoned())?;
        inner
            .runs
            .get(id.0 as usize - 1)
            .cloned()
            .ok_or_else(|| FerroError::Provenance(format!("unknown provenance slot {}", id)))
    }

    fn attribute(&self, rid: RecordId) -> Result<ProvId, FerroError> {
        let inner = self.inner.read().map_err(|_| Self::poisoned())?;
        Ok(inner
            .pages
            .get(&rid.page_id)
            .map(|d| d.prov_of(rid.slot_num))
            .unwrap_or(ProvId::NONE))
    }

    fn stamp(&self, rid: RecordId, id: ProvId) -> Result<(), FerroError> {
        if id.is_none() {
            return Err(FerroError::Provenance(
                "refusing to stamp a version with ProvId::NONE; omit the stamp instead".into(),
            ));
        }
        let mut inner = self.inner.write().map_err(|_| Self::poisoned())?;
        if inner.runs.get(id.0 as usize - 1).is_none() {
            return Err(FerroError::Provenance(format!(
                "cannot stamp with {}: not interned",
                id
            )));
        }
        inner.pages.entry(rid.page_id).or_default().stamp(rid.slot_num, id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::types::BranchId;

    fn run(agent: &str, run_id: &str) -> RunEntity {
        RunEntity::new(
            ProvId::NONE,
            agent,
            run_id,
            "claude-opus",
            "2026-05",
            [7u8; 32],
            1_700_000_000_000,
            BranchId::new(4, 0),
        )
    }

    fn rid(page: u32, slot: u16) -> RecordId {
        RecordId { page_id: page, slot_num: slot }
    }

    #[test]
    fn interning_the_same_run_twice_is_a_lookup() {
        let s = MemProvenanceStore::new();
        let a = s.intern(&run("restock", "run-1")).unwrap();
        let b = s.intern(&run("restock", "run-1")).unwrap();
        assert_eq!(a, b);
        assert_eq!(s.run_count(), 1);
    }

    #[test]
    fn distinct_runs_get_distinct_slots() {
        let s = MemProvenanceStore::new();
        let a = s.intern(&run("restock", "run-1")).unwrap();
        let b = s.intern(&run("restock", "run-2")).unwrap();
        let c = s.intern(&run("auditor", "run-1")).unwrap();
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_eq!(s.run_count(), 3);
    }

    #[test]
    fn re_interning_a_run_with_a_different_model_is_refused() {
        let s = MemProvenanceStore::new();
        s.intern(&run("restock", "run-1")).unwrap();
        let mut lying = run("restock", "run-1");
        lying.model = "some-other-model".into();
        let err = s.intern(&lying).unwrap_err();
        assert!(format!("{}", err).contains("different actor tuple"));
    }

    #[test]
    fn criterion_nine_which_agent_run_model_wrote_this_row() {
        let s = MemProvenanceStore::new();
        let id = s.intern(&run("restock-agent", "run-42")).unwrap();
        s.stamp(rid(9, 3), id).unwrap();
        let who = s.who_wrote(rid(9, 3)).unwrap();
        assert_eq!(who.agent_id, "restock-agent");
        assert_eq!(who.run_id, "run-42");
        assert_eq!(who.model, "claude-opus");
        assert_eq!(who.model_version, "2026-05");
        assert_eq!(who.prov_id, id);
    }

    #[test]
    fn an_unstamped_version_is_unattributed_not_wrong() {
        let s = MemProvenanceStore::new();
        assert_eq!(s.attribute(rid(1, 1)).unwrap(), ProvId::NONE);
        assert!(s.who_wrote(rid(1, 1)).is_err());
        assert_eq!(s.describe_row(rid(1, 1)), "unattributed");
    }

    #[test]
    fn stamping_with_an_uninterned_slot_is_refused() {
        let s = MemProvenanceStore::new();
        assert!(s.stamp(rid(1, 1), ProvId(9)).is_err());
        assert!(s.stamp(rid(1, 1), ProvId::NONE).is_err());
    }

    #[test]
    fn a_run_can_be_asked_what_it_wrote() {
        let s = MemProvenanceStore::new();
        let a = s.intern(&run("restock", "run-1")).unwrap();
        let b = s.intern(&run("auditor", "run-1")).unwrap();
        s.stamp(rid(1, 0), a).unwrap();
        s.stamp(rid(1, 1), b).unwrap();
        s.stamp(rid(2, 0), a).unwrap();
        assert_eq!(s.rows_written_by(a).unwrap(), vec![rid(1, 0), rid(2, 0)]);
        assert_eq!(s.rows_written_by(b).unwrap(), vec![rid(1, 1)]);
    }

    #[test]
    fn attribution_is_run_level_so_a_page_holds_one_dictionary_entry_per_run() {
        let s = MemProvenanceStore::new();
        let id = s.intern(&run("restock", "run-1")).unwrap();
        for slot in 0..200u16 {
            s.stamp(rid(1, slot), id).unwrap();
        }
        // 200 versions, one run: the page dictionary holds exactly one entry.
        assert_eq!(s.page_dictionary_len(1), 1);
    }

    /// **The numbers the docs quote, computed here so they cannot drift into folklore.**
    ///
    /// `~3.4x density loss` was asserted in three places — this module's header, the README and the
    /// demo — and measured in none. It is not wrong, but it is the *row-inflation* figure and it
    /// silently depends on a row size nobody stated: the same tuple inflates a 24-byte version by
    /// 5.2x and a 100-byte one by 2.0x. A reader could not have checked it.
    ///
    /// So both quantities are pinned. Exact equality on the tuple size is deliberate: it is a
    /// number the prose quotes, so changing `RunEntity` should fail here and force the prose to be
    /// updated rather than silently becoming false.
    /// Stamping a version with `ProvId::NONE` is refused rather than stored.
    ///
    /// `NONE` is the value `prov_of` returns for a version that carries no attribution, so storing
    /// it would make "this row was written by nobody" and "this row has no record of who wrote it"
    /// the same bytes — and criterion 9 is the ability to tell who wrote a given row.
    #[test]
    fn stamping_a_version_with_no_provenance_is_refused() {
        let s = MemProvenanceStore::new();
        let err = s
            .stamp(rid(1, 0), ProvId::NONE)
            .expect_err("a version was stamped with ProvId::NONE");
        assert!(
            format!("{err}").contains("refusing to stamp"),
            "refused, but not by this guard: {err}"
        );

        // Anti-vacuity: a real id stamps fine, so the refusal is about NONE and not about stamping.
        let id = s.intern(&run("a", "r")).unwrap();
        s.stamp(rid(1, 0), id).expect("a real provenance id was refused");
        assert_eq!(s.attribute(rid(1, 0)).unwrap(), id, "the stamp did not land");
    }

    #[test]
    fn the_density_numbers_the_docs_quote_are_the_numbers_this_computes() {
        let e = run("restock-agent", "run-42");
        assert_eq!(
            e.literal_footprint(),
            101,
            "the actor tuple's literal size changed. The README, this module's header and the demo \
             all quote it - update them together with this number."
        );

        let s = MemProvenanceStore::new();
        let id = s.intern(&e).unwrap();
        for slot in 0..200u16 {
            s.stamp(rid(1, slot), id).unwrap();
        }
        let interned = s.footprint_bytes();
        let literal = s.literal_footprint_bytes().unwrap();
        assert_eq!(interned, 204, "1 byte per version plus a one-entry dictionary");
        assert_eq!(literal, 20_200, "101 bytes repeated 200 times");
        assert_eq!(literal / interned, 99, "the ratio the docs quote for provenance bytes");

        // The row-inflation figure, with the row size it depends on made explicit.
        //
        // Bounded rather than pinned to two decimals: the exact value is 141/40 = 3.525, and the
        // first version of this assertion disagreed with the probe that produced it because Python
        // rounds half-to-even (3.52) and Rust half-up (3.53). A test that encodes a rounding
        // convention tests the convention. The prose says ~3.5x, which both agree on.
        let inflate = |row: usize| (row + e.literal_footprint()) as f64 / row as f64;
        assert!(
            (inflate(40) - 3.525).abs() < 1e-9,
            "the ~3.5x figure is a 40-byte version, and only that: {}",
            inflate(40)
        );
        assert!(
            inflate(24) > inflate(100),
            "inflation must fall as rows grow, or the figure is not about row size at all"
        );
    }

    #[test]
    fn the_interned_slot_beats_a_literal_header_by_a_wide_margin() {
        let s = MemProvenanceStore::new();
        let id = s.intern(&run("restock-agent", "run-42")).unwrap();
        for slot in 0..200u16 {
            s.stamp(rid(1, slot), id).unwrap();
        }
        let interned = s.footprint_bytes();
        let literal = s.literal_footprint_bytes().unwrap();
        // Measured here, not quoted: one u8 per version plus a 1-entry dictionary, against the
        // full actor tuple repeated 200 times.
        assert_eq!(interned, 200 * PROV_SLOT_BYTES + std::mem::size_of::<ProvId>());
        assert!(
            literal > interned * 20,
            "literal {} vs interned {}",
            literal,
            interned
        );
    }

    #[test]
    fn a_page_dictionary_refuses_to_widen_past_a_byte() {
        let mut d = PageProvDict::new();
        for i in 1..=MAX_PAGE_DICT_ENTRIES as u32 {
            d.intern_local(ProvId(i)).unwrap();
        }
        assert_eq!(d.dictionary_len(), MAX_PAGE_DICT_ENTRIES);
        let err = d.intern_local(ProvId(9999)).unwrap_err();
        assert!(format!("{}", err).contains("dictionary full"));
        // An already-present entry still resolves; the cap refuses growth, not use.
        assert_eq!(d.intern_local(ProvId(1)).unwrap(), 0);
    }

    #[test]
    fn the_local_slot_round_trips_through_the_dictionary() {
        let mut d = PageProvDict::new();
        d.stamp(5, ProvId(2)).unwrap();
        d.stamp(6, ProvId(3)).unwrap();
        d.stamp(7, ProvId(2)).unwrap();
        assert_eq!(d.local_of(5), Some(0));
        assert_eq!(d.local_of(6), Some(1));
        assert_eq!(d.local_of(7), Some(0));
        assert_eq!(d.prov_of(7), ProvId(2));
        assert_eq!(d.prov_of(99), ProvId::NONE);
        assert_eq!(d.dictionary_len(), 2);
        assert_eq!(d.stamped_versions(), 3);
    }
}
