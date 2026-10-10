# Executed by make_model.py with its diagram helpers.
add('fork','Fork and shadow paging, with real pointer roles',
    'Illustrative IDs. This is the branch COW tree, not a frozen copy of every ordinary SQL table.',[
    N('before_rec',65,70,'Two branch records',['Parent.root = 7','Child.root = 7'],'branch',w=290,h=100,detail=['Fork creates child metadata with a new identity/generation/lease and the parent\'s root ID. Sharing this number makes the existing COW tree reachable without copying all data.']),
    N('before_root',125,245,'Page 7: root',['Internal node','child IDs: 12, 19'],'branch',w=170,h=100),
    N('before_left',45,465,'Page 12',['Leaf values'],'branch',w=150,h=90),
    N('before_right',235,465,'Page 19',['Leaf values'],'branch',w=150,h=90),
    N('parent_rec',480,70,'Parent record',['root_page_id = 7'],'branch',w=260,h=100),
    N('child_rec',900,70,'Child record',['root_page_id = 30'],'branch',w=260,h=100,detail=['Only this child\'s record receives the new COW root. The branch catalog also has its own B+ tree root; that is a different ID in a different tree.']),
    N('original_root',555,245,'Page 7: original',['IDs still 12, 19'],'branch',w=180,h=100),
    N('copied_root',935,245,'Page 30: copy',['IDs now 12, 31'],'branch',w=220,h=100,detail=['The root needs a copy because its child pointer changes. A deeper tree may need affected shared ancestors copied along the path; unchanged subtrees remain shared.']),
    N('original_leaf',745,465,'Page 19',['Original values'],'branch',w=175,h=90),
    N('shared_leaf',465,465,'Page 12',['Still shared'],'sql',w=175,h=90),
    N('changed_leaf',985,465,'Page 31',['Modified copy of 19'],'branch',w=175,h=90)
    ],[
    E('before_rec','before_root',[[210,170],[210,245]],'root ID',210,212,'branch'),
    E('before_root','before_left',[[175,345],[120,465]],'12',135,400,'branch'),
    E('before_root','before_right',[[245,345],[310,465]],'19',290,400,'branch'),
    E('parent_rec','original_root',[[610,170],[645,245]],'unchanged',610,211,'branch'),
    E('child_rec','copied_root',[[1030,170],[1045,245]],'new root ID',1050,211,'branch'),
    E('original_root','original_leaf',[[690,345],[832,465]],'19',770,400,'branch'),
    E('original_root','shared_leaf',[[615,345],[552,465]],'12',560,400,'sql'),
    E('copied_root','shared_leaf',[[980,345],[945,390],[945,590],[552,590],[552,555]],'shared page 12',750,578,'sql'),
    E('copied_root','changed_leaf',[[1080,345],[1072,465]],'31',1110,400,'branch')
    ],lanes=[L(25,15,380,580,'1 / AFTER FORK','branch'),L(435,15,745,580,'2 / AFTER CHILD EDITS RIGHT LEAF','branch')],height=650,
    texts=[T(40,630,'Assume shared starting pages; no split or boundary repair. Copy 19 and the route to it; keep 12 shared.')],cards=[
    C('RAM shares a second structure','Fork also clones roots of immutable workspace maps. These are balanced binary trees in RAM, not disk B+ tree pages. Map edits copy their path; later parent-private edits stay isolated.','branch'),
    C('Exclusively owned pages can change in place','The writer must own the page and its birth epoch must meet the privacy barrier, which includes the latest live child fork. Splits or boundary repairs can allocate extra pages.','branch'),
    C('Avoiding a full data copy is not zero cost','Fork still performs catalog, synchronization and durability work. Branches also add effect capture, workspace and page-management costs; no cheaper-than-transactions claim is made.','check')],sources=[
    S('src/branch/record.rs',244,'Child inherits page root'),S('src/agent_sql/runtime.rs',1737,'Share workspace roots'),S('src/agent_sql/persistent_map.rs',190,'Immutable map clone'),S('src/branch/arena.rs',2008,'COW decision'),S('src/cow/btree.rs',942,'Relink ancestors'),S('src/branch/table_catalog.rs',1124,'Record new root')])

exec(compile(OUT.joinpath('model_cow_engine.py').read_text(), str(OUT / 'model_cow_engine.py'), 'exec'), globals())

add('pages','Inside the pages: three layouts, three purposes',
    'Page IDs locate blocks. Root IDs choose starting blocks. Child IDs route through trees. Row locations select heap slots.',[
    N('cow_header',50,80,'COW header — 24 bytes',['birth epoch / arena ID / CRC','page type / flags / reserved'],'branch',w=320,detail=['Offsets: birth epoch 0–7; arena ID 8–11; checksum 12–15; type 16; flags 17; reserved 18–23. No branch root ID or list of child IDs lives here.']),
    N('cow_payload',50,265,'COW internal payload',['Count / offsets / leftmost child','Separator keys + child page IDs'],'branch',w=320,detail=['A slotted node holds variable-size cells. Leftmost child has its own payload field; other child IDs accompany separator keys. Root is a role: a small tree can have a leaf as its root.']),
    N('cow_leaf',50,455,'COW leaf payload',['(table ID, row ID) → row value','No leaf next / prev links'],'branch',w=320,detail=['PagedRows keys concatenate table_id:u32 and row_id:u64 in big-endian order. Values encode typed row values. Ordered scans use an ancestor stack rather than leaf sibling links.']),
    N('index_header',440,80,'Ordinary index header',['Type / own page ID / LSN','Checksum field / key count'],'sql',w=320,detail=['Ordinary internal header: 19 bytes. Ordinary leaf header: 27 bytes, including next/prev leaf IDs. The ordinary checksum field is serialized but is not verified COW-style CRC protection. This differs from the COW format.']),
    N('index_payload',440,265,'Ordinary internal payload',['Sorted separator keys','Child page IDs'],'sql',w=320,detail=['Another B+ tree, with pages updated in place under physical synchronization. Shared root cells make splits visible to other index handles.']),
    N('index_leaf',440,455,'Ordinary index leaf',['Primary: PK → (page, slot)','Secondary: (value, PK)'],'sql',w=320,detail=['Ordinary leaves have sibling links for ordered scans. They locate heap rows rather than hold branch-private encoded rows.']),
    N('heap_header',830,80,'Heap page header',['Page metadata / LSN / checksum','Slot counts and free space'],'sql',w=320,detail=['Heap pages use a 23-byte header and 4-byte slot entries. The ordinary checksum field does not imply verified COW-style CRC protection. This is neither the COW node nor the ordinary index node format.']),
    N('heap_slot',830,265,'Slot directory',['slot number → offset + length','Variable-size tuple bytes'],'sql',w=320,detail=['RecordId is physical (page_id, slot_num). Slot offsets locate tuple bytes. It is distinct from the logical row ID used in agent merging.']),
    N('tuple',830,455,'Tuple / version header',['begin / end transaction IDs','Previous version: page + slot'],'sql',w=320,detail=['Fixed 24-byte version prefix: begin 8, end 8, previous page 4, previous slot 2, reserved 2. Null bitmap and typed values follow. Current agent row identity is derived from the primary key, not an independent immutable surrogate.'])
    ],[
    E('cow_header','cow_payload',[[210,190],[210,265]],'within page',210,232,'branch'),
    E('cow_payload','cow_leaf',[[210,375],[210,455]],'child page link',210,416,'branch'),
    E('index_header','index_payload',[[600,190],[600,265]],'within page',600,232,'sql'),
    E('index_payload','index_leaf',[[600,375],[600,455]],'child page link',600,416,'sql'),
    E('heap_header','heap_slot',[[990,190],[990,265]],'then slots',990,232,'sql'),
    E('heap_slot','tuple',[[990,375],[990,455]],'slot locates',990,416,'sql')
    ],lanes=[L(30,20,360,590,'BRANCH COW TREE','branch'),L(420,20,360,590,'ORDINARY SQL INDEX','sql'),L(810,20,360,590,'ORDINARY HEAP ROWS','sql')],texts=[T(45,650,'A child link reaches a different page with its own header. Internal and leaf payloads are different node types.')],height=680,
    cards=[C('Where is the root ID?','A branch record stores its COW root ID in the branch catalog. The catalog has its own separate tree root. Every page does not store the root of its tree.','branch'),C('Where is the page itself?','A buffer-pool hit reuses a RAM frame; a miss reads the requested page from disk. Fork does not load the whole tree. A page must be in memory before it can be edited.','sql')],sources=[
    S('src/cow/page_header.rs',9,'COW header bytes'),S('src/cow/node.rs',19,'COW payload'),S('src/agent_sql/paged_rows.rs',36,'Encoded row key'),S('src/storage/index_page.rs',35,'Ordinary index headers'),S('src/storage/heap_page.rs',19,'Heap layout'),S('src/storage/tuple.rs',11,'Version prefix'),S('src/agent_sql/runtime.rs',299,'Logical row identity')])

add('agent','What an agent actually reads and writes',
    'Workspace state drives branch SQL. Its frame drives live SQL merge; the appended effect log is a separate representation.',[
    N('base',40,80,'Current committed heap',['Fresh ordinary ReadView','Index / table scan'],'sql',w=280,detail=['The base scan uses read_snapshot_cached with txn_id 0 for each call. A branch does not freeze committed tables at fork.']),
    N('overlay',450,80,'Combine by logical key',['Private row replaces base row','Deleted marker removes row'],'branch',w=300,detail=['Private inserts add rows. Predicates apply to private after-images too, allowing an edited row to enter or leave the result.']),
    N('result',860,80,'Agent-visible result',['Projection + read capture','Supported single-table reads'],'branch',w=300,detail=['Explicit reads capture point-version references or predicate summaries. This AgentRuntime SELECT path refuses general joins.']),
    N('write',40,330,'Compute proposed edit',['Read visible row; evaluate RHS','Capture operation + WHERE guard'],'branch',w=280,detail=['Recognized same-column + literal or - literal becomes Add(delta). Other expressions can become Assign(evaluated value), including some mathematically equivalent forms.']),
    N('checks',450,330,'Validate whole statement',['Capability envelope / escrow','Charge applicable budgets'],'check',w=300,detail=['Policy/escrow preflight occurs before row staging. Ordinary SQL outside the agent path bypasses these branch controls.']),
    N('state',860,330,'Stage private workspace',['After-image + first-touch before','Extend cumulative task frame'],'branch',w=300,detail=['base_rows retains the row\'s first-touch witness, rows retains latest private state, and frame collects typed effects/guards. fork_seq helps find intervening published operations.']),
    N('merge_input',860,560,'Input to SQL MERGE',['Workspace + frame + read set','Not a replay from .tel'],'check',w=300,detail=['Live merge reads runtime state. Reopening the durable effect log does not rebuild the SQL workspaces or admission history.']),
    N('tel',450,560,'Append / extend TEL',['Task identity + typed effects','CLI: append and sync .tel'],'wal',w=300,detail=['Durable TEL extends frames and deduplicates identical retries. The pgserver example instead uses a memory effect log. This is not physical recovery WAL.']),
    N('mirror',40,560,'Mirror row to COW pages',['Encode row; put / delete','Record changed root'],'branch',w=280,detail=['The order is workspace staging, TEL append, then page mirroring. A later I/O failure is not rolled back as one transaction across all stores.'])
    ],[
    E('base','overlay',[[320,135],[450,135]],'base rows',385,123,'sql'),
    E('overlay','result',[[750,135],[860,135]],'visible rows',805,123,'branch'),
    E('state','overlay',[[1010,330],[1010,250],[600,250],[600,190]],'private overrides / deletions',780,238,'branch'),
    E('write','checks',[[320,385],[450,385]],'proposed rows',385,373,'branch'),
    E('checks','state',[[750,385],[860,385]],'1. stage',805,373,'check'),
    E('state','merge_input',[[1070,440],[1070,560]],'live state',1070,503,'branch'),
    E('state','tel',[[910,440],[910,500],[600,500],[600,560]],'2. append frame',745,488,'wal'),
    E('tel','mirror',[[450,615],[320,615]],'3. mirror',385,603,'branch')
    ],height=710,cards=[
    C('A branch spans statements','A branch retains private proposals until merge or abandonment. Ordinary transaction IDs, effect-frame IDs, branch IDs and generations have different meanings.'),
    C('Read validation is partial','Exact point reads can detect later agent-published versions. Predicate summaries do not supply complete phantom validation; ordinary writes do not all update this same version map.','check'),
    C('Three representations are not one commit','Workspace, TEL and COW mirror are staged in sequence. The per-tree rollback journal does not make the full sequence atomic or restore unfinished sessions after restart.','wal')],sources=[
    S('src/agent_sql/runtime.rs',2152,'Read overlay'),S('src/agent_sql/runtime.rs',2250,'Agent SELECT scope'),S('src/agent_sql/runtime.rs',2990,'Staging sequence'),S('src/agent_sql/runtime.rs',6856,'Operation capture'),S('src/tel/log.rs',1625,'Frame extension'),S('tests/integration_durable_tel.rs',8,'Live merge versus durable log')])
