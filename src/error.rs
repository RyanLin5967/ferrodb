use std::fmt::{ Display, Formatter, Result };
use std::error;
//add more types
//
// `Clone` and `PartialEq` are derived (F0): every variant is a unit or a `String`, so both are free,
// and consensus actions carry a refusal reason that tests compare structurally. An error type that
// cannot be compared forces tests to match on rendered text, which then pins the wording of a
// message rather than the fact of it.
#[derive(Debug, Clone, PartialEq)]
pub enum FerroError {
    // `Parse(String)` was here and is deliberately gone — E71.
    //
    // It had 41 construction sites and, once each was classified by who could cause it, **not one of
    // them was a parse failure**. Real SQL problems have always used `SqlParseError`. What `Parse`
    // actually held was arithmetic a user's expression could not evaluate, invariants this code is
    // supposed to maintain, on-disk corruption, and a handful of catalog lookups that belonged in
    // `Bind` (E67 moved most of those; `planner::plan` still had two).
    //
    // Removed rather than left empty. A variant meaning "one of four unrelated things" is how the
    // grab-bag formed in the first place, and leaving it available is an invitation to refill it.
    Io(String),
    NotEnoughSpace,
    SlotDeleted,
    KeyNotFound,
    EmptyList,
    PagePinned,
    SqlParseError(String),
    IndexAlreadyExists,
    Constraint(String),
    OnlyDML,
    Bind(String),
    Wal(String),
    Txn(String),
    // agent-isolation layer
    Branch(String),
    Cow(String),
    Merge(String),
    /// A cell has no value here — distinct from a failed read of one.
    ///
    /// Merge needs to tell "the LCA never had this cell" (expected for a row this merge is
    /// creating) from "reading the LCA failed" (a real fault). Collapsing both into `Merge`
    /// meant `.ok()` at the call site silently turned a disk failure into "no LCA value".
    CellAbsent(String),
    Provenance(String),

    // ---- E71: the three classes that used to share `Parse` -------------------------------------
    //
    // `Parse` had 41 construction sites covering four unrelated failure classes: real SQL problems,
    // arithmetic a user's expression could not evaluate, invariants this code is supposed to maintain,
    // and bytes on disk that are not what they should be. All four rendered as `parsing error:`, so
    // nothing reading logs by class could tell a typo from a corrupted catalog page.
    //
    // The last one is why this matters beyond tidiness: a corruption reported as a parse error is a
    // corruption nobody pages on.
    /// A user's expression could not be evaluated — division by zero, arithmetic on a non-number, an
    /// overflow. The statement is well-formed; the values are the problem.
    Eval(String),
    /// An invariant this code is supposed to maintain did not hold. Not caused by input or by disk.
    ///
    /// These are the arms that are unreachable given the caller — a `compare` reached with an operator
    /// that is not a comparison, a row shorter than its own schema. They were `Parse`, which told the
    /// reader their SQL was malformed when the fault is in ferrodb.
    Internal(String),
    /// Bytes read back from disk are not what was written: a non-UTF8 stored name, an unknown tag, a
    /// record that ends mid-field.
    ///
    /// Deliberately loud in `Display`. Every other error here describes something a caller did; this one
    /// describes damage, and it must not read like a syntax complaint.
    Corruption(String),
    /// A value the on-disk encoding's length fields cannot express — refused on the way in, before
    /// it becomes durable.
    ///
    /// **The same variant, for the same reason, as `consensus::log::LogError::Unrepresentable`**,
    /// whose doc comment states the argument in full: this is *arithmetic*, not policy, so it is
    /// not a flavour of `NotEnoughSpace` (a page-size limit) or of `Constraint` (whether data is
    /// valid). A name longer than its `u8` or `u16` length prefix is written with a **truncated
    /// prefix followed by its full bytes**, which desynchronises every reader from that point on —
    /// and nothing downstream can catch it, because everything downstream checks the bytes against
    /// a checksum of exactly the bytes intended. Refusing at the encoder is the only point at which
    /// the caller still has somewhere to put the error.
    ///
    /// `what` is a `String` and not `LogError`'s `&'static str` because the catalog encoder runs
    /// over the WHOLE catalog at once: "column name" alone does not say which table, and the
    /// operator's next action is to rename a specific object.
    ///
    /// Introduced by D141, which found `catalog::catalog_page` doing exactly this at eight sites
    /// while three sibling encoders already carried comments about the hazard.
    Unrepresentable { what: String, len: usize, limit: usize },
    /// A publication refused to let something out of the database, or its declaration is not usable
    /// — see `replication::publication`.
    ///
    /// Its own variant rather than `Constraint`, for the reason E71 gave for splitting `Parse`: a
    /// constraint is about whether data is *valid*, and this is about whether data may *leave*. They
    /// have different audiences — one is answered by fixing a row, the other by an operator deciding
    /// what a consumer is allowed to see — and a log reader filtering by class must be able to tell
    /// an egress refusal from a bad insert.
    Publication(String),

    /// This node is not the leader, so it refused rather than serving a write or a stale read.
    ///
    /// `leader` is an **address** and not a `NodeId`, because the only useful thing a refused
    /// client can do with it is reconnect, and a node number is not something it can dial. `None`
    /// means this node does not currently know who leads — which is a real and common state during
    /// an election, and is deliberately distinguishable from "the leader is elsewhere".
    NotLeader { leader: Option<String> },
}

impl Display for FerroError {
    fn fmt(&self, f: &mut Formatter<'_> ) -> Result {
        match self {
            FerroError::Eval(e) => write!(f, "evaluation error: {}", e),
            FerroError::Internal(e) => write!(f, "internal error (this is a bug in ferrodb): {}", e),
            FerroError::Corruption(e) => write!(f, "DATA CORRUPTION: {}", e),
            // Names the limit as well as the length: "too long" without a number leaves the caller
            // guessing at what would fit, and the two limits here differ by a factor of 256.
            FerroError::Unrepresentable { what, len, limit } => write!(
                f,
                "{what} is {len} bytes, which the on-disk length field cannot express (limit \
                 {limit}); refused rather than written truncated"
            ),
            FerroError::Publication(e) => write!(f, "publication refused: {}", e),
            FerroError::Io(e) => write!(f, "io error: {}", e),
            FerroError::NotEnoughSpace => write!(f, "not enough space in page"),
            FerroError::SlotDeleted => write!(f, "the slot is delted"),
            FerroError::KeyNotFound => write!(f, "key wasn't found"),
            FerroError::EmptyList => write!(f, "linked hash set is empty"),
            FerroError::PagePinned => write!(f, "page is pinned"),
            FerroError::SqlParseError(s) => write!(f, "sql parsing error: {}", s),
            FerroError::IndexAlreadyExists => write!(f, "index already exists"),
            FerroError::Constraint(s) => write!(f, "constraint error: {}", s),
            FerroError::OnlyDML => write!(f, "only supports dml"),
            FerroError::Bind(s) => write!(f, "binding error: {}", s),
            FerroError::Wal(s) => write!(f, "wal error: {}", s),
            FerroError::Txn(s) => write!(f, "txn error: {}", s),
            FerroError::Branch(s) => write!(f, "branch error: {}", s),
            FerroError::Cow(s) => write!(f, "cow page store error: {}", s),
            FerroError::Merge(s) => write!(f, "merge error: {}", s),
            FerroError::CellAbsent(s) => write!(f, "cell absent: {}", s),
            FerroError::Provenance(s) => write!(f, "provenance error: {}", s),
            // Two distinct messages, because the two states call for different actions and a
            // single message covering both would be advice that is wrong half the time. Knowing
            // the leader means reconnect *there*; not knowing it means an election is in progress
            // and the only correct action is to retry *here* shortly.
            FerroError::NotLeader { leader: Some(addr) } => write!(
                f,
                "not the leader: this node does not accept writes. The leader is at {addr}; \
                 reconnect there. Reads served here could be stale, so they are refused too."
            ),
            FerroError::NotLeader { leader: None } => write!(
                f,
                "not the leader, and this node does not currently know who is - an election is in \
                 progress or this node is partitioned from the cluster. Retry shortly; do not treat \
                 this as the leader being down."
            ),
        }
    }
}

impl error::Error for FerroError {}