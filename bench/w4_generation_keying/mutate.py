#!/usr/bin/env python3
"""Apply one named mutant to src/agent_sql/runtime.rs, or refuse.

Every mutant reverts the file to the slot-blind behaviour of ONE call site, which is what the
tree did before D158 item 1. A mutant whose anchor is not found EXACTLY ONCE aborts non-zero:
running an unmutated tree and reading the green is the failure this guard exists to prevent.
"""
import sys, pathlib

WT = pathlib.Path("/Users/idide/wt/ferrodb-agent-acf966798a3979f57")
SRC = WT / "src/agent_sql/runtime.rs"


def slot_range(var):
    return (f"BranchId::new({var}.id, 0)..=BranchId::new({var}.id, u32::MAX)")


MUTANTS = {
    # The eviction that replaces the BTreeMap key collision the slot-keyed map got free.
    "evict": (
        """        let displaced: Vec<BranchId> = self
            .workspaces
            .range(BranchId::new(branch.id, 0)..=BranchId::new(branch.id, u32::MAX))
            .map(|(b, _)| *b)
            .collect();
        let olds: Vec<Workspace> =
            displaced.into_iter().filter_map(|dead| self.workspaces.remove(&dead)).collect();
        self.workspaces.insert(branch, ws);
        for old in olds {
            self.drop_txn_refs(&old);
            forget_captures_unless_published(self, &old);
        }""",
        """        self.workspaces.insert(branch, ws);""",
    ),
    # visible_rows_where -- the staged-row overlay a SELECT reads.
    "overlay": (
        "                state.workspaces.get(&b).map(|ws| (ws.rows.clone(), ws.unprobeable_rows))",
        "                state.workspaces.range("
        + slot_range("b")
        + ").next().map(|(_, ws)| (ws.rows.clone(), ws.unprobeable_rows))",
    ),
    # record_read -- which task a read is retained against.
    "record_read": (
        "        let (txn, prov) = match state.workspaces.get(&reader) {",
        "        let (txn, prov) = match state.workspaces.range("
        + slot_range("reader")
        + ").next().map(|(_, w)| w) {",
    ),
    # blind_writes -- the shortest statement that needs a workspace.
    "blind_writes": (
        """    pub fn blind_writes(&self, branch: BranchId) -> Result<Vec<(TableId, RowId)>, FerroError> {
        let state = self.state.lock().unwrap();
        let ws = state.workspaces.get(&branch).ok_or_else(|| {""",
        """    pub fn blind_writes(&self, branch: BranchId) -> Result<Vec<(TableId, RowId)>, FerroError> {
        let state = self.state.lock().unwrap();
        let ws = state.workspaces.range("""
        + slot_range("branch")
        + """).next().map(|(_, w)| w).ok_or_else(|| {""",
    ),
    # seal -- ABANDON / MERGE retiring a branch.
    "seal": (
        "            if let Some(ws) = state.remove_workspace(&branch) {",
        "            let mutant_key = state.workspaces.range("
        + slot_range("branch")
        + ").next().map(|(k, _)| *k);\n"
        "            if let Some(ws) = mutant_key.and_then(|k| state.remove_workspace(&k)) {",
    ),
    # forget_one_branch -- the lease sweep's own removal.
    "forget": (
        "    let Some(ws) = state.remove_workspace(&bid) else {",
        "    let mutant_key = state.workspaces.range("
        + slot_range("bid")
        + ").next().map(|(k, _)| *k);\n"
        "    let Some(ws) = mutant_key.and_then(|k| state.remove_workspace(&k)) else {",
    ),
    # The eviction ORDER: release the displaced workspace before installing the new one, which is
    # what the slot-keyed map could not get wrong because BTreeMap::insert did both in one call.
    "evict_order": (
        """        let olds: Vec<Workspace> =
            displaced.into_iter().filter_map(|dead| self.workspaces.remove(&dead)).collect();
        self.workspaces.insert(branch, ws);
        for old in olds {
            self.drop_txn_refs(&old);
            forget_captures_unless_published(self, &old);
        }""",
        """        let olds: Vec<Workspace> =
            displaced.into_iter().filter_map(|dead| self.workspaces.remove(&dead)).collect();
        for old in olds {
            self.drop_txn_refs(&old);
            forget_captures_unless_published(self, &old);
        }
        self.workspaces.insert(branch, ws);""",
    ),
    # The chunked reconciliation's RESUME. D158 item 1 changed the cursor from a u64 slot to a
    # BranchId with an excluded bound; a resume that does not resume forgets one chunk's worth and
    # leaves the rest, which is the unbounded growth the function exists to prevent.
    "chunk_stop": (
        """            match last_seen {
                Some(last) => cursor = Some(last),""",
        """            match last_seen {
                Some(_last) => return forgotten,""",
    ),
}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in MUTANTS:
        print(f"usage: mutate.py <{'|'.join(MUTANTS)}>", file=sys.stderr)
        return 2
    old, new = MUTANTS[sys.argv[1]]
    s = SRC.read_text()
    n = s.count(old)
    if n != 1:
        print(f"REFUSING: anchor for '{sys.argv[1]}' found {n} times, expected exactly 1",
              file=sys.stderr)
        return 1
    SRC.write_text(s.replace(old, new))
    print(f"APPLIED {sys.argv[1]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
