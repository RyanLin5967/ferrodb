"""Authored, source-verified diagram model. Rebuild with Python's standard library."""
from pathlib import Path
import json

OUT = Path(__file__).parent
SECTIONS = []

def S(file, line, label):
    return dict(file=file, line=line, label=label)

def N(id, x, y, title, lines, tone='neutral', detail=(), w=240, h=110, sources=()):
    return dict(id=id,x=x,y=y,w=w,h=h,title=title,lines=lines,tone=tone,detail=list(detail),sources=list(sources))

def E(a,b,points,label='',lx=None,ly=None,tone='neutral',dashed=False):
    d=dict(from_=a,to=b,points=points,label=label,tone=tone,dashed=dashed)
    d['from']=d.pop('from_')
    if lx is not None:d.update(label_x=lx,label_y=ly)
    return d

def L(x,y,w,h,label,tone='neutral'):
    return dict(x=x,y=y,w=w,h=h,label=label,tone=tone)

def T(x,y,text,size=14,tone='neutral',anchor='start'):
    return dict(x=x,y=y,text=text,size=size,tone=tone,anchor=anchor)

def C(title,text,tone='neutral'):
    return dict(title=title,text=text,tone=tone)

def add(id,title,subtitle,nodes,edges=(),lanes=(),texts=(),height=650,cards=(),sources=(),table=None):
    d=dict(id=id,number=f'{len(SECTIONS)+1:02}',title=title,subtitle=subtitle,
           diagram=dict(width=1200,height=height,nodes=nodes,edges=list(edges),lanes=list(lanes),texts=list(texts)),
           cards=list(cards),sources=list(sources))
    if table:d['table']=table
    SECTIONS.append(d)

add('system','The system, in one view',
    'Start here. Follow the blue SQL path or the purple agent path; they meet when a merge publishes rows.',[
    N('entry',440,30,'CLI or PostgreSQL client',['SQL text enters a session','Current transaction / agent'],w=320,h=90,detail=['The CLI and PostgreSQL-wire example share execution components. A session separately tracks an ordinary transaction and an agent branch. PostgreSQL wire compatibility is a protocol surface, not the PostgreSQL storage engine.']),
    N('route',440,165,'Parse and route',['Ordinary SQL or agent command'],w=320,h=85,detail=['The scanner/parser creates statements. Execution dispatch routes agent commands and branch-session DML to AgentRuntime. Ordinary SELECT is bound and planned; ordinary DML uses dedicated operators.']),
    N('sql',70,325,'Ordinary SQL execution',['Plans, operators, transactions','Heap rows + index lookups'],'sql',w=300,detail=['The ordinary path reads and writes heap rows with MVCC visibility. Its primary and secondary B+ trees locate row versions; they are separate from the branch COW tree.']),
    N('agent',830,325,'AgentRuntime',['Fork, private reads and edits','Evaluate / publish merge'],'branch',w=300,detail=['An agent session lives across statements. It stages changes in a private workspace, captures typed effects, and mirrors row state to its branch COW pages.']),
    N('heap',70,535,'Shared SQL heap',['Authoritative committed rows','Current + older row versions'],'sql',w=300,detail=['Normal SQL rows remain in the ordinary heap. The trunk COW tree is not an alternative authoritative copy of those tables.']),
    N('merge',450,535,'Merge admission',['Resolve effects; check premises','Publish accepted rows'],'check',w=300,detail=['SQL MERGE reads the workspace and its in-memory effect frame, evaluates against the current heap, then applies accepted rows in one ordinary WAL transaction. It does not replace the trunk COW root.']),
    N('workspace',830,535,'Private workspace — RAM',['Rows / tombstones / before-images','Typed effects / read captures'],'branch',w=300,detail=['Persistent maps preserve inherited private state. Actual SQL reads combine current committed heap rows with these maps. Live merge input comes from Workspace.frame; the durable TEL is not replayed into this workspace on reopen.']),
    N('data',70,735,'Main database pages',['Buffer pool → disk manager','Heap, indexes, COW arenas'],'sql',w=300,h=100,detail=['Heap/index allocation is below the persisted arena floor; branch arena allocation is above it. Both use the main database buffer pool and file. The branch catalog uses a different file and pool.']),
    N('logs',450,735,'Logs and metadata',['WAL ≠ TEL ≠ branch catalog','Different jobs and guarantees'],'wal',w=300,h=100,detail=['WAL supports ordinary row recovery. TEL stores typed intent when a durable implementation is configured. Branch records identify roots, ownership, generations and leases. Their separate durability boundaries matter.']),
    N('cow',830,735,'Branch COW pages',['Mirror staged row state','Shared pages + private copies'],'branch',w=300,h=100,detail=['These are real page-backed trees with their own roots. They support COW isolation, diff and reclamation, but normal branch SQL reads still use the workspace overlay.'])
    ],[
    E('entry','route',[[600,120],[600,165]],'statement',650,147),
    E('route','sql',[[440,210],[220,210],[220,325]],'ordinary SQL',290,197,'sql'),
    E('route','agent',[[760,210],[980,210],[980,325]],'agent path',860,197,'branch'),
    E('sql','heap',[[220,435],[220,535]],'read / write',220,487,'sql'),
    E('agent','workspace',[[980,435],[980,535]],'private state',980,487,'branch'),
    E('agent','heap',[[830,380],[790,380],[790,475],[330,475],[330,535]],'read committed base',540,463,'sql',True),
    E('workspace','merge',[[830,590],[750,590]],'submit',790,577,'branch'),
    E('merge','heap',[[450,590],[370,590]],'publish',410,577,'check'),
    E('heap','data',[[220,645],[220,735]],'page I/O',220,696,'sql'),
    E('merge','logs',[[600,645],[600,735]],'row WAL',600,696,'wal'),
    E('workspace','cow',[[980,645],[980,735]],'mirror edits',980,696,'branch')
    ],texts=[T(70,860,'Ordinary DML and accepted merge rows both use WAL; private COW staging has separate durability.')],height=895,cards=[
    C('The decisive distinction','SQL row versions, private workspace state, and COW page versions are different structures. The detailed views below follow each separately.','sql'),
    C('Actual server concurrency','Eligible reads can run together. Current pgwire statements that write take an exclusive catalog gate and drain readers; multiple agents do not imply simultaneously executing SQL writers.','check')],sources=[
    S('src/execution/session.rs',6,'Transaction and agent session are separate'),S('src/execution/executor.rs',173,'Statement routing'),S('src/agent_sql/runtime.rs',2152,'Heap + workspace read path'),S('src/pgwire/mod.rs',143,'Exclusive statement gate'),S('tests/integration_trunk_tree_authority.rs',170,'Trunk SQL rows live in heap')])

add('sql','From SQL to a physical row',
    'This is the ordinary SQL path. Primary and secondary indexes lead to heap row locations, not branch roots.',[
    N('parse',40,65,'Scanner → parser',['SQL text → statement'],w=240,h=90),
    N('bind',340,65,'Bind SELECT',['Resolve tables / columns','Construct logical plan'],'sql',detail=['Name/type binding builds expressions with column positions and a logical SELECT plan. DDL and ordinary DML are not all passed through this same logical-plan pipeline.']),
    N('plan',640,65,'Optimize → lower',['Push filters; choose scans','Choose joins; build operators'],'sql',detail=['The optimizer can choose table or index scans and hash or nested-loop joins. Inner joins can be reordered; supported left joins preserve their semantics. Lowering produces executor operators.']),
    N('next',940,65,'Execute with ReadView',['Pull rows through operators','Filter / join / project'],'sql',detail=['Operators share one read view for the statement. An ordinary explicit transaction supplies its retained snapshot; autocommit statements obtain a current snapshot.']),
    N('secondary',40,300,'Secondary B+ tree',['(column value, PK) → entry','Use PK to find row'],'sql',detail=['The secondary index stores pairs so duplicate column values remain distinguishable. It resolves the primary key through the primary index before fetching the heap row.']),
    N('primary',340,300,'Primary B+ tree',['PK → RecordId','RecordId = page + slot'],'sql',detail=['The index root cell is shared by handles so a root split is visible to later lookups. Internal pages route by separator keys; linked leaf pages support ordered scans.']),
    N('heap',640,300,'Heap page and slot',['Read current tuple header','Follow older versions if needed'],'sql',detail=['A physical RecordId identifies a slot in a heap page. The current version may not be visible to this reader; its previous-version pointer leads into the time-travel heap. Index candidates are rechecked against visible row values.']),
    N('scan',940,300,'Sequential scan',['Directory → heap pages','Visit row slots'],'sql',detail=['Without a useful index, the heap scanner walks table page directories and row slots. MVCC visibility still applies; a scan is not permission to expose uncommitted rows.']),
    N('pool',340,550,'Buffer pool — RAM',['Hit: pin cached frame','Miss: load a disk page'],'sql',detail=['Pinning prevents eviction while a caller uses a frame. Dirty pages need writeback. ARC replacement tracks recency/frequency; staged touch batching and optimistic read shadows are cache/concurrency techniques, not branch COW.']),
    N('disk',640,550,'Disk manager',['Page ID → 4 KB block','Allocate / read / write / sync'],'sql',detail=['DiskManager maps page IDs to blocks and manages allocation bitmaps. The main database reserves disjoint heap/index and branch arena regions. WAL must be durable through a dirty heap page\'s LSN before that page is written.'])
    ],[
    E('parse','bind',[[280,110],[340,110]],'SELECT',310,98,'sql'),
    E('bind','plan',[[580,120],[640,120]],'plan',610,108,'sql'),
    E('plan','next',[[880,120],[940,120]],'operators',910,108,'sql'),
    E('next','secondary',[[1060,175],[1060,240],[160,240],[160,300]],'secondary-index lookup',600,226,'sql'),
    E('next','primary',[[1035,175],[1035,270],[460,270],[460,300]],'primary-key lookup',640,258,'sql'),
    E('next','scan',[[1100,175],[1100,300]],'scan',1138,252,'sql'),
    E('secondary','primary',[[280,355],[340,355]],'PK',310,343,'sql'),
    E('primary','heap',[[580,355],[640,355]],'RID',610,343,'sql'),
    E('scan','heap',[[940,355],[880,355]],'tuple',910,343,'sql'),
    E('heap','pool',[[760,410],[760,475],[460,475],[460,550]],'fetch / pin referenced pages',602,461,'sql'),
    E('pool','disk',[[580,605],[640,605]],'I/O',610,593,'sql')
    ],texts=[T(40,205,'INSERT / UPDATE / DELETE use dedicated operators; UPDATE / DELETE can plan their row selection.'),T(45,495,'Index traversal also fetches pages through the buffer pool.')],height=700,cards=[
    C('Two kinds of synchronization','MVCC decides logical visibility. Page latches protect physical tree structure during traversal/splits. Buffer pinning protects residency. None of these substitutes for the others.'),
    C('Index maintenance is distinct','Ordinary indexes mutate in place. Deletes retain entries and scans filter stale candidates. Heap recovery is followed by index rebuilding in the CLI; this is not per-index-operation WAL redo.','check')],sources=[
    S('src/execution/executor.rs',115,'Read snapshots'),S('src/optimizer/optimizer.rs',7,'Optimization and lowering'),S('src/storage/index.rs',347,'Optimistic index descent'),S('src/storage/index_page.rs',4,'Ordinary index page layouts'),S('src/buffer/buffer_pool.rs',747,'Page fetch'),S('src/wal/recovery.rs',177,'Index rebuilding')])

add('mvcc','MVCC: time belongs to the reader',
    'A snapshot is a visibility rule over transaction IDs. It is not a copied database and not a change log.',[
    N('snap',40,70,'Reader snapshot',['high_water + active IDs','Own writes are visible'],'sql',w=300,h=110,detail=['A snapshot includes a creator transaction when its ID is below high_water and it was not active at capture. ReadView also treats its own transaction ID as visible. The begin_ts/end_ts field names contain transaction IDs, not wall-clock times.']),
    N('head',440,70,'Current row: value 15',['begin = B; end = 0','prev → older row location'],'sql',w=300,h=110,detail=['An UPDATE puts the prior row version into the time-travel heap and makes the new row the head. A larger tuple can move to a new physical location; indexes are updated accordingly.']),
    N('old',840,70,'Older row: value 10',['begin = A; end = B','May point to another version'],'sql',w=300,h=110,detail=['The old copy\'s end transaction is the updating transaction B. A reader whose snapshot excludes B can still see the old copy if it includes A. Versions live as tuples, not full snapshots of every page.']),
    N('oldreader',40,335,'Reader started before B',['B was not visible at capture','Reject head; follow prev'],'sql',w=300,detail=['For a transaction started before B, the new head is invisible. Following prev reaches the version created by A; B does not count as a visible ending transaction, so value 10 remains visible.']),
    N('rule',440,335,'Version visibility test',['Creator visible to reader?','No visible ending transaction?'],'check',w=300,detail=['ReadView first checks creation visibility, then whether a nonzero end_ts is visible. If a head fails, resolve_visibility follows the previous-version chain. A transaction also sees its own changes.']),
    N('newreader',840,335,'Reader started after B',['After B committed','Head is visible → value 15'],'sql',w=300,detail=['After commit, a new snapshot includes B. The head is visible; the old version is ended by a transaction this reader includes. An already open ordinary transaction keeps its original snapshot.'])
    ],[
    E('head','old',[[740,125],[840,125]],'prev pointer',790,111,'sql'),
    E('snap','rule',[[190,180],[190,250],[590,250],[590,335]],'visibility metadata',395,237,'sql'),
    E('head','rule',[[590,180],[590,220],[780,220],[780,285],[640,285],[640,335]],'test version',775,266,'check'),
    E('rule','oldreader',[[440,390],[340,390]],'older view',390,378,'sql'),
    E('rule','newreader',[[740,390],[840,390]],'newer view',790,378,'sql')
    ],texts=[T(40,510,'UPDATE: preserve old tuple → write new head.    DELETE: mark end transaction; keep history.'),T(40,546,'Overlapping writes can raise a write conflict. MVCC visibility alone does not prove serializable execution.')],height=590,cards=[
    C('A snapshot does not lock in agent SQL','This diagram describes ordinary transaction snapshots. Agent SQL obtains fresh committed base rows for each read and overlays private state; it does not retain this full view from branch fork.','branch'),
    C('History has a separate lifetime','The branch reaper frees branch COW pages. It is not a vacuum for old heap row versions; no general MVCC vacuum is represented here.','lifecycle')],sources=[
    S('src/wal/txn.rs',121,'Snapshot representation'),S('src/wal/txn.rs',1113,'Visibility predicate'),S('src/wal/visibility.rs',3,'Following previous versions'),S('src/execution/update.rs',73,'Creating a new version'),S('src/execution/delete.rs',47,'Ending a version'),S('src/storage/tuple.rs',11,'Version header')])

for fragment in ('model_branch.py','model_merge_recovery.py','model_lifetime_boundaries.py'):
    exec(compile(OUT.joinpath(fragment).read_text(), str(OUT / fragment), 'exec'), globals())
