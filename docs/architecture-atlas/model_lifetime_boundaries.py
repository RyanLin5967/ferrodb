# Executed by make_model.py with its diagram helpers.
add('lifetime','Who owns a page, and when it can be freed',
    'The cleanup rule preserves descendants that might still reach old pages. Epoch, lease and generation are different clocks/identities.',[
    N('trigger',45,80,'Branch is finished',['Merge / abandon / lease expiry','Validate handle generation'],'lifecycle',w=300,detail=['The default agent lease is 15 minutes; the default background scan is every 30 seconds. Expiry makes cleanup eligible rather than implying exact-deadline deletion. The trunk cannot be reaped.']),
    N('reaping',450,80,'Persist Reaping state',['Refuse further branch reads','Choose logical free epoch'],'lifecycle',w=300,detail=['Persisting an intermediate lifecycle state allows a later startup to resume cleanup after interruption. A stale handle cannot act on a different branch occupying the same recycled ID slot.']),
    N('children',855,80,'Any live children?',['Consult authoritative catalog','Keep descendant visibility pins'],'check',w=300,detail=['The catalog indexes parent/child relationships and child fork epochs. A reaped intermediate ancestor with a live descendant retains its lineage link, preserving protection for grandparent pages.']),
    N('bulk',450,300,'Childless branch',['Free its owned arenas in bulk','Do not free inherited pages'],'lifecycle',w=300,detail=['An arena is a contiguous extent owned by one branch; extents grow geometrically from 1 to 256 pages, then stay capped. A branch can own several arenas. Bulk reclaim is safe when no descendant needs those private pages.']),
    N('interval',855,300,'Shared history remains',['Check birth / free interval','Park protected pages as pending'],'lifecycle',w=300,detail=['A page cannot be reused while a live child fork epoch falls in [page birth, logical free). Protection is conservative and indexed by lineage, not a count stored on every page.']),
    N('epoch',45,300,'Example: page lifetime',['Birth 3 → free requested at 9','Child fork at 5 still protects it'],'branch',w=300,detail=['5 lies in [3,9), so the child may still reach the old page even though its parent no longer uses it. These numbers are logical ordering epochs, not seconds or transaction IDs.']),
    N('fence',450,540,'Persist Reaped state',['Bump generation; reuse if safe','Retain needed ancestor links'],'lifecycle',w=300,detail=['Reaped slots remain unavailable while live descendants still require them. Generation fencing prevents an old BranchId handle from accidentally reading/writing a new branch after ID reuse. Parent links detach only when descendant pins can be released safely.']),
    N('retry',855,540,'Drain pending frees',['After last protecting child ends','Reuse newly safe pages / extents'],'lifecycle',w=300,detail=['When the last relevant child disappears, cleanup revisits deferred frees and can cascade upward through reaped ancestors. Startup also resumes interrupted reaps and sweeps orphaned extents.']),
    N('allocator',45,540,'Arena allocator state',['Main-file page extents','Checkpoint / tail in .arena'],'lifecycle',w=300,detail=['Page payloads are in the main database; ownership/allocation metadata is in the allocator sidecar. Persisted allocation avoids giving a page already owned by one branch to another after reopen.'])
    ],[
    E('trigger','reaping',[[345,135],[450,135]],'retire',397,123,'lifecycle'),
    E('reaping','children',[[750,135],[855,135]],'inspect',802,123,'lifecycle'),
    E('children','bulk',[[900,190],[900,240],[600,240],[600,300]],'no live child',750,227,'lifecycle'),
    E('children','interval',[[1050,190],[1050,300]],'yes',1080,245,'check'),
    E('epoch','interval',[[195,410],[195,455],[1005,455],[1005,410]],'fork 5 lies inside [3, 9): defer',610,442,'branch',True),
    E('bulk','fence',[[600,410],[600,540]],'finish record',600,510,'lifecycle'),
    E('interval','retry',[[1100,410],[1100,540]],'later release',1100,510,'lifecycle'),
    E('interval','fence',[[900,410],[900,490],[700,490],[700,540]],'mark reaped',810,479,'lifecycle'),
    E('fence','allocator',[[450,595],[345,595]],'safe frees',397,583,'lifecycle'),
    E('fence','retry',[[750,595],[855,595]],'recheck',802,583,'lifecycle')
    ],height=695,cards=[
    C('Epoch','Logical ordering of page births, forks and frees. Also participates in deciding whether a branch-owned page is private enough to edit in place.','branch'),
    C('Lease','Standalone: wall-clock deadline for abandoned work; clustered components use replicated time. A background scan and cleanup do the actual reclaim; this is separate from MVCC snapshot visibility.','lifecycle'),
    C('Generation','Identity fence for recycled branch IDs. Same numeric slot with a different generation means a different branch. Branch GC does not vacuum ordinary heap row versions.','check')],sources=[
    S('src/branch/reaper.rs',619,'Reap stages'),S('src/branch/reaper.rs',242,'Preserve descendant links'),S('src/branch/record.rs',1267,'Half-open lifetime interval'),S('src/branch/arena.rs',2132,'Pending versus immediate free'),S('src/branch/types.rs',271,'Arena growth'),S('src/branch/lease_thread.rs',120,'Background interval'),S('src/agent_sql/runtime.rs',96,'Default lease')])

add('boundaries','Extensions connect to the core at specific points',
    'These are attachments, not a second explanation of the database. Dashed arrows mean a separate/optional component connection.',[
    N('simulate',45,75,'SIMULATE / selected effects',['Try candidate workspaces','Inspect, select, recheck admission'],'branch',w=300,detail=['SIMULATE forks candidates and runs the same evaluator. Assertions determine legality; equally legal candidates tie and declaration order wins. Cherry-pick selects recorded effects into a workspace. Neither is a second storage engine.']),
    N('core',450,75,'Agent staging and merge',['Private staging → checked merge','Shared heap is the target'],'check',w=300),
    N('provenance',855,75,'Provenance / revert',['Who wrote and what was read','Dependency-aware inverse changes'],'lifecycle',w=300,detail=['Provenance captures run/model/prompt identity and read/write relationships. Revert can halt on downstream dependencies or explicitly cascade inverse operations. It is not a tree-root rewind or one all-cascade atomic transaction.']),
    N('physical',45,335,'Physical WAL shipping',['Durable log bytes → heap redo','Base backup needed for bootstrap'],'wal',w=300,detail=['The source stops at flushed_lsn. Log shipping alone is not leader election or safe automatic failover. Bare redo does not reproduce the full ordinary catalog; an existing integration test explicitly records that limitation.']),
    N('wal',450,335,'Ordinary WAL',['Row records + transaction markers','Source for replication / decoding'],'wal',w=300),
    N('cdc',855,335,'Logical CDC feed',['Decode committed row changes','Snapshot + resumable WAL handoff'],'wal',w=300,detail=['An initial MVCC snapshot captures existing rows; pinned WAL and a resume boundary bridge later committed changes. Publication filtering controls emitted rows. CDC is not the branch TEL and does not perform branch merging.']),
    N('raft',45,610,'Separate consensus components',['Raft-style terms / quorums','Ordered rounds + durable log'],'neutral',w=300,detail=['The consensus state machine and driver coordinate replicated commands and membership. Normal CLI/pgserver do not construct this coordinator. Consensus log round numbers are not local WAL byte offsets.']),
    N('cluster',450,610,'ClusterAgents coordinator',['Replicated metadata decisions','Speculative contents stay local'],'neutral',w=300,detail=['A merge proposal is based on an applied round, may require re-evaluation if the base moved, and publishes locally after its metadata decision applies. This is a library/test path, not the normal SQL server path.']),
    N('gap',855,610,'Distribution boundary',['Merge metadata and row WAL','Arrive in separate rounds'],'check',w=300,detail=['The documented failure window can leave metadata saying merged while row WAL has not arrived. There is no transparent failover for an unfinished node-local branch. No end-to-end distributed SQL atomicity is claimed here.'])
    ],[
    E('simulate','core',[[345,130],[450,130]],'reuse core paths',397,118,'branch'),
    E('core','provenance',[[750,130],[855,130]],'capture / record',802,118,'lifecycle'),
    E('core','wal',[[600,185],[600,335]],'published row transaction',600,268,'wal'),
    E('wal','physical',[[450,390],[345,390]],'ship bytes',397,378,'wal',True),
    E('wal','cdc',[[750,390],[855,390]],'decode rows',802,378,'wal',True),
    E('raft','cluster',[[345,665],[450,665]],'commands',397,653,'neutral',True),
    E('cluster','gap',[[750,665],[855,665]],'known gap',802,653,'check',True)
    ],lanes=[L(25,550,1150,205,'IMPLEMENTED SEPARATELY — NOT IN THE NORMAL CLI / PGWIRE EXECUTION PATH','neutral')],height=790,cards=[
    C('Tree utilities are distinct','Page diff skips shared page IDs during paired descent. cow::merge3 is a structural base/ours/theirs merger; SQL MERGE uses operation semantics. Auxiliary content hashes do not define production page identity.','branch'),
    C('Deliberately outside the core map','Benchmark harnesses, full-text retrieval, wire-format conveniences, unwired dedup and page-delta compression utilities are not expanded. Their omission does not imply they are core execution dependencies.','neutral')],sources=[
    S('src/agent_sql/simulate.rs',367,'Candidate workflow'),S('src/agent_sql/runtime.rs',5791,'Dependency-aware revert'),S('src/agent_sql/runtime.rs',2094,'Runtime page diff'),S('src/cow/merge3.rs',303,'Structural merge library'),S('src/replication/mod.rs',1,'Replication and CDC components'),S('src/agent_sql/cluster.rs',67,'Cluster atomicity boundary'),S('tests/integration_replication_e2e.rs',255,'Catalog replication boundary')])

GLOSSARY = [
    ('Page','A 4 KB block addressed by a page ID. Its format depends on what it stores.'),
    ('Page ID','A numeric block address, not a memory pointer or a content hash.'),
    ('Root page ID','The starting page for a tree. A branch stores this in its branch record.'),
    ('Internal / leaf','An internal tree node routes to children; a leaf holds the tree\'s key/value entries.'),
    ('Separator key','A boundary used to choose which child subtree can contain a search key.'),
    ('Header / payload','The format\'s metadata prefix versus the node or tuple data following it.'),
    ('RecordId','Physical ordinary row location: page ID plus slot number.'),
    ('Logical row ID','Identity used to match agent changes; currently derived from primary-key values.'),
    ('Heap','Pages containing ordinary table tuples, organized through page directories.'),
    ('Buffer pool','RAM cache of disk pages. Pins prevent eviction; locks protect accesses to bytes.'),
    ('Latch','Short-lived physical synchronization for page/tree operations; distinct from transaction visibility.'),
    ('MVCC','Multiple row versions plus a rule choosing which version a reader sees.'),
    ('Snapshot','Transaction-ID boundary and active set defining visibility, not a complete copied database.'),
    ('Version chain','Previous-version pointers linking a current tuple to older tuples.'),
    ('COW / shadow paging','Share existing pages; copy pages when an edit would otherwise change shared state.'),
    ('Workspace','RAM maps of private row state, before-images, effects, schema edits and related task state.'),
    ('Persistent map','Immutable, structurally shared map in RAM. Persistent here does not mean saved to disk.'),
    ('Arena','A contiguous page extent owned by a branch; a branch can own multiple extents.'),
    ('Epoch','Logical order used for sharing and reclamation. Not elapsed time or MVCC transaction identity.'),
    ('Lease','Deadline after which background cleanup can retire abandoned branch work.'),
    ('Generation','Version of a recycled branch identity slot, used to reject stale handles.'),
    ('TEL / effect frame','Typed operation intent and guards for a task. Separate from physical recovery WAL.'),
    ('WAL','Write-ahead log: recovery information is made durable before associated dirty heap pages are written.'),
    ('LSN','WAL position; page LSNs help avoid replaying the same physical update twice.'),
    ('CLR','Compensation log record describing undo progress so recovery can continue after another crash.'),
    ('Guard / assertion','Guard: precondition on the target before effects. Assertion: condition on proposed results.'),
    ('Commuting effects','Supported operations whose combination can preserve both intents; checks can still reject the result.'),
    ('Fingerprint','Evidence of the evaluated base, checked again before publishing; not a full serializability proof.'),
    ('Quarantine','Hold an agent branch for inspection after an admission gate refuses publication.'),
    ('Checkpoint','Coordinate durable state to reduce replay history; retention rules may prevent log truncation.'),
    ('CDC','Committed row-change feed decoded from WAL, optionally bootstrapped with a snapshot.'),
    ('Consensus round','Position in the replicated command order, distinct from the local WAL byte LSN.'),
]

# Surface essential ordinary-write and branch-state distinctions without another dense diagram.
next(s for s in SECTIONS if s['id']=='mvcc')['table'] = {
    'headers':['Ordinary operation','Row/version action','Index action'], 'rows':[
    ['INSERT','Create tuple with begin=my transaction','Primary upsert; secondary entries'],
    ['UPDATE','Preserve old copy; new head points to it','Maintain mapping if location/indexed value changes'],
    ['DELETE','Set ending transaction; retain historical row','Entries can remain; reads check visibility']
]}

MODEL = dict(
    title='FerroDB · The core, connected', revision='fc9556a742f61884a3011ccc28e78f160a8d57fe', date='2026-09-23',
    intro='An implementation map, from SQL to bytes and back. Read the numbered views in order; select a box for its mechanism and evidence. Solid arrows show the labeled dependency or data flow. Dashed arrows mark a separate path, optional component, or later action as labeled.',
    legend=[dict(key=k,label=v) for k,v in [('sql','Ordinary SQL / shared data'),('branch','Private branch / COW'),('wal','Log / durability'),('check','Validation / boundary'),('lifecycle','Ownership / cleanup'),('neutral','Entry point / separate component')]],
    sections=SECTIONS, glossary=[dict(term=t,meaning=m) for t,m in GLOSSARY],
    scope=[
      'Mapped to the source revision above. Four independent source reviews were reconciled; see review/ and verification.json for evidence and final checks.',
      'The atlas explains implemented mechanisms. It does not certify every combination of isolation, schema change, rollback, index access, allocation and machine power loss.',
      'Agent branch semantics, costs, server concurrency and restart boundaries are stated explicitly. No general branch-versus-transaction performance advantage is claimed.',
      'The application source was not changed. Focused existing tests support specific claims; they are not a full test-suite or exhaustive correctness proof.'
    ])
OUT.joinpath('atlas.json').write_text(json.dumps(MODEL,ensure_ascii=False,indent=2)+'\n')
print(f'Wrote {len(SECTIONS)} diagrams, {sum(len(s["diagram"]["nodes"]) for s in SECTIONS)} nodes to {OUT / "atlas.json"}')
