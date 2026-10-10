# Inserted after the fork example by make_model.py.
add('cow-engine','The COW engine: lookup, ownership, edit, relink',
    'The tree algorithm operates on page IDs. Arena ownership decides whether a page can change in place.',[
    N('root',40,65,'Start from branch root',['BranchRecord.root_page_id','No ancestor-branch lookup'],'branch',w=250,h=105),
    N('descent',350,65,'Descend internal nodes',['Compare key to separators','Fetch / pin selected child'],'branch',w=250,h=105,detail=['CowTree::get receives a root ID and key, not a branch ancestry chain. ArenaPageStore fetches the page through the main buffer pool and verifies its COW checksum.']),
    N('leaf',660,65,'Reach leaf',['Search sorted key slots','Read encoded value if present'],'branch',w=250,h=105),
    N('read',970,65,'Read result',['Value or absent','Unpin after use'],'branch',w=190,h=105,detail=['Pins keep a buffer frame resident during access. A cache miss loads the page; a cache hit reuses the existing frame. The branch tree read is separate from SQL\'s heap-plus-workspace overlay.']),
    N('ownership',40,365,'Need a writable page',['Writer owns the arena?','birth epoch >= privacy barrier?'],'check',w=250,h=110,detail=['Private iff the writer owns the page\'s arena and birth_epoch >= max(branch fork epoch, latest live child fork epoch). The stored private flag is not a substitute for these checks.']),
    N('inplace',355,285,'Private: reuse page',['Keep same page ID','Save before-image for rollback'],'branch',w=260,h=110),
    N('copy',355,510,'Shared: shadow page',['Allocate in writer arena','Copy payload; stamp new header'],'branch',w=260,h=110,detail=['Inherited pages are not the child\'s property to retire. Replaced pages owned by this writer are retired only when the operation commits, and descendant pins may still defer reuse.']),
    N('edit',690,395,'Edit / split leaf',['Change cell and slot layout','Split / repair boundaries if needed'],'branch',w=240,h=120,detail=['Leaf boundaries use key hashes and entry sizes with page-capacity bounds. Some updates/deletes repair chunk boundaries, so the simple two-page copy example intentionally excludes splits and boundary repair.']),
    N('relink',980,395,'Relink ancestors',['Changed child IDs','Propagate any splits'],'branch',w=180,h=120,detail=['Ancestors need edits when child pointers or separator keys change. Shared ancestors copy; private ones can update in place. Relinking can stop when nothing above changes, or create a new root after a split.']),
    N('journal',40,685,'Operation rollback journal',['Restore in-place before-images','Delay retirement until success'],'wal',w=350,h=110,detail=['On a tree error, restore the pages mutated in place. Some newly allocated unreachable pages may remain until branch reclamation. This journal handles one tree operation, not disk power loss or a multi-store SQL transaction.']),
    N('publish',840,685,'Return / record resulting root',['If changed: set branch root ID','Root unchanged for some edits'],'branch',w=320,h=110,detail=['AgentRuntime::put_row records a new root only if CowTree returned a different ID. TableBranchCatalog persists its own metadata separately; root publication alone does not imply atomic persistence of all COW data pages.'])
    ],[
    E('root','descent',[[290,117],[350,117]],'root ID',320,104,'branch'),
    E('descent','leaf',[[600,117],[660,117]],'child ID',630,104,'branch'),
    E('leaf','read',[[910,117],[970,117]],'lookup',940,104,'branch'),
    E('leaf','ownership',[[785,170],[785,230],[165,230],[165,365]],'WRITE: obtain a writable version of the selected page',475,216,'check'),
    E('ownership','inplace',[[290,400],[320,400],[320,340],[355,340]],'yes',322,326,'branch'),
    E('ownership','copy',[[290,440],[320,440],[320,565],[355,565]],'no',322,552,'branch'),
    E('inplace','edit',[[615,340],[655,340],[655,435],[690,435]],'same ID',657,328,'branch'),
    E('copy','edit',[[615,565],[655,565],[655,475],[690,475]],'new ID',653,591,'branch'),
    E('edit','relink',[[930,455],[980,455]],'upward',955,442,'branch'),
    E('relink','publish',[[1070,515],[1070,685]],'tree success',1080,608,'branch'),
    E('ownership','journal',[[165,475],[165,685]],'journal spans tree edit',165,652,'wal',True)
    ],height=835,cards=[C('Bounded copying is the benefit','Unchanged subtrees remain shared. Read cost follows tree height, not branch ancestry depth. Actual write cost also includes allocations, splits, boundary repair and metadata/durability work.','branch'),C('Read and write structures are still distinct','This is the COW page engine. Ordinary SQL indexes have their own traversal/latching implementation; branch SQL reads still use current heap rows overlaid with a private RAM workspace.','sql')],sources=[
    S('src/cow/btree.rs',214,'Tree lookup'),S('src/cow/btree.rs',466,'Tree insertion'),S('src/branch/arena.rs',2008,'Ownership/privacy decision'),S('src/cow/btree.rs',787,'Leaf boundary/split logic'),S('src/cow/btree.rs',942,'Ancestor relinking'),S('src/cow/btree.rs',147,'Operation journal'),S('src/agent_sql/runtime.rs',1486,'Publish changed root')])
