//! B8 — full-text posting lists on the existing secondary-index machinery.
//!
//! A secondary index in this engine is a `BPlusTreeManager<(Value, Value), ()>` keyed
//! `(column_value, primary_key)` — see `crate::execution::index_handle`. A **posting list** is that
//! same structure with a *token* in the first component instead of the whole column value, so a
//! full-text index needs no new tree, no new page type, no new `BTreeSerialize` impl and no new WAL
//! record: a `range_scan` over one token's prefix **is** reading its posting list, and
//! `wal::recovery::rebuild_indexes` reconstructs it from the heap exactly as it reconstructs every
//! other tree.
//!
//! This module owns the storage-level half of that: the tokenizer, the posting key, and the prefix
//! bounds that turn a token into a scan. Scoring and top-k live in
//! `crate::execution::fulltext_search`.

use std::ops::Bound;
use std::sync::Arc;

use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::storage::index::BPlusTreeManager;

/// The tree a full-text index is. Identical to a secondary index's type on purpose — the whole
/// point of B8 is that a posting list needs no new structure.
pub type PostingTree = BPlusTreeManager<(Value, Value), ()>;

/// Longest token this index stores, in bytes. A longer run of alphanumerics is **not indexed** and
/// therefore cannot be found; every other token in the same value still is.
///
/// This is a structural limit, not a style choice. A leaf's payload budget is
/// `PAGE_SIZE - LEAF_HEADER_SIZE` = 4069 bytes, and `BPlusTreeLeafPage::split` cannot split a leaf
/// holding one entry: `mid = 1/2 = 0`, so `split_off(0)` moves the lone entry to the new leaf,
/// leaves the original empty, and serializing the still-oversized leaf panics. `VARCHAR(60000)` is
/// a legal column type here and one 60000-byte run of letters is one token, so without a cap an
/// ordinary `INSERT` could panic inside the B+tree rather than fail. 255 bytes is far below the
/// structural limit and far above any word.
pub const MAX_TOKEN_BYTES: usize = 255;

/// Split `text` into the tokens this index recognises, in order of appearance.
///
/// **The rule: lowercase, and break on every character that is not alphanumeric.**
///
/// What that choice costs, stated so no reader has to discover it from a surprising result:
///
/// - **No stemming and no stopwords.** `run` does not find `running`, and `the` is a term with its
///   own (very low) idf rather than being dropped.
/// - **Lowercasing is `char::to_lowercase`**, which is Unicode-aware and may expand one char into
///   several (`İ` → `i̇`). Case folding is therefore correct for Latin and Greek but is *not* full
///   Unicode caseless matching (no `ß`/`ss` folding, no locale rules).
/// - **`char::is_alphanumeric` is Unicode-aware, and that cuts both ways.** Accented letters and
///   digits from any script stay inside a token, which is what you want; but a run of CJK
///   ideographs is alphanumeric throughout and so becomes **one token**, because word segmentation
///   for unspaced scripts is a dictionary problem this tokenizer does not attempt. CJK text is
///   indexed and can be found by pasting the same run back; it cannot be searched by word.
/// - **Punctuation inside a word splits it.** `don't` → `don`, `t`; `state-of-the-art` → four
///   tokens; `user@example.com` → `user`, `example`, `com`; `3.14` → `3`, `14`. So there are no
///   phrase queries, no exact-punctuation matching, and no way to search for a token containing a
///   symbol.
/// - **Numbers are tokens**, and are compared as text: `007` and `7` are different terms.
/// - **A token over `MAX_TOKEN_BYTES` is dropped** (see that constant).
///
/// `dl` (document length) in the BM25 scorer is the length of *this* vector, i.e. the count of
/// tokens the index recognises — a dropped over-long run does not lengthen the document, because
/// for retrieval purposes it is not in it.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            // `to_lowercase` yields an iterator because one char can fold to several.
            for lc in ch.to_lowercase() {
                cur.push(lc);
            }
        } else if !cur.is_empty() {
            push_token(&mut out, std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        push_token(&mut out, cur);
    }
    out
}

fn push_token(out: &mut Vec<String>, token: String) {
    // Silently skipping is the honest option of the three: truncating would invent matches for a
    // prefix the document does not contain, and refusing the write would fail an `INSERT` over a
    // value the column type explicitly permits.
    if token.len() <= MAX_TOKEN_BYTES {
        out.push(token);
    }
}

/// The distinct tokens of `text`, sorted.
///
/// **One posting per (token, primary key), however many times the document says the word.**
/// `BPlusTreeLeafPage::insert_entry` inserts at the binary-search position rather than overwriting,
/// so a second identical key is *stored*, and every scan of that posting list then yields the row
/// twice. This is the same E66 rule the secondary-index write paths follow
/// (`execution::insert`, `execution::update`); for a full-text index it bites one level earlier,
/// because a single value can produce the same token repeatedly.
pub fn distinct_tokens(text: &str) -> Vec<String> {
    let mut tokens = tokenize(text);
    tokens.sort();
    tokens.dedup();
    tokens
}

/// The posting key for `token` under primary key `pk`.
pub fn posting_key(token: &str, pk: &Value) -> (Value, Value) {
    (Value::Varchar(token.to_string()), pk.clone())
}

/// Lower bound of one token's posting list.
///
/// `Value::Null` is the minimum of `Value`'s total order (`type_rank(Null) == 0`), so
/// `(token, Null)` sorts at or before every `(token, pk)`.
///
/// There is deliberately no matching upper bound, because `Value` has **no maximum**: the top
/// `type_rank` is `Varchar` and there is no greatest `String`. The end of a posting list is
/// therefore found by watching the first component, exactly as `SecondaryIndexScan` watches
/// `sec_upper` instead of bounding its scanner.
pub fn posting_lower_bound(token: &str) -> Bound<(Value, Value)> {
    Bound::Included((Value::Varchar(token.to_string()), Value::Null))
}

/// Every primary key posted under `token`, in tree order.
///
/// This is the whole read of a posting list: one prefix range scan over the same B+tree the
/// secondary indexes use, stopping at the first key whose token differs.
///
/// The postings returned are **candidates, not answers**. An entry outlives the row version that
/// produced it — `DELETE` leaves it in place and `UPDATE` inserts the new tokens without removing
/// the old — so the caller must resolve each primary key through the primary index, apply
/// visibility, and re-check that the token is still in the row it lands on.
pub fn postings_for_token(tree: &PostingTree, token: &str) -> Result<Vec<Value>, FerroError> {
    let want = Value::Varchar(token.to_string());
    let mut pks = Vec::new();
    for item in tree.range_scan(posting_lower_bound(token), Bound::Unbounded)? {
        let ((tok, pk), ()) = item?;
        if tok != want {
            break;
        }
        pks.push(pk);
    }
    Ok(pks)
}

/// Post every distinct token of `text` under `pk`, skipping pairs the tree already holds.
///
/// The `search`-before-`insert` probe is not an optimisation. `insert_entry` appends, so posting a
/// pair that is already there stores it twice and every lookup through it returns the row twice.
/// Both DML write paths and both build-from-heap paths go through here so that there is one copy of
/// that rule rather than four.
pub fn post_tokens(tree: &PostingTree, text: &str, pk: &Value) -> Result<(), FerroError> {
    for token in distinct_tokens(text) {
        let key = posting_key(&token, pk);
        if tree.search(&key)?.is_none() {
            tree.insert(key, ())?;
        }
    }
    Ok(())
}

/// The indexed text of a value: `Some` for `VARCHAR`, `None` for `NULL`.
///
/// `Catalog::create_fulltext_index` refuses any column that is not `VARCHAR`, so those are the only
/// two shapes a maintenance path can see; anything else is a bug in that refusal and is reported as
/// one rather than being tokenized through `Debug` or skipped quietly.
pub fn indexed_text(value: &Value) -> Result<Option<&str>, FerroError> {
    match value {
        Value::Varchar(s) => Ok(Some(s.as_str())),
        Value::Null => Ok(None),
        other => Err(FerroError::Internal(format!(
            "full-text index on a non-VARCHAR value {other:?}; CREATE FULLTEXT INDEX is supposed \
             to have refused this column"
        ))),
    }
}

/// Open a posting tree at a recorded root.
pub fn open_posting_tree(root_page_id: u32, bp: Arc<BufferPoolManager>) -> PostingTree {
    BPlusTreeManager::<(Value, Value), ()>::open(root_page_id, bp)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Breaking shape: mixed case and punctuation. Without lowercasing, `Wireless` and `wireless`
    /// are different terms; without splitting on non-alphanumerics, `charger,` keeps its comma and
    /// no query spells it that way.
    #[test]
    fn tokenize_lowercases_and_splits_on_non_alphanumerics() {
        assert_eq!(
            tokenize("Wireless Charger, 20W!"),
            vec!["wireless", "charger", "20w"]
        );
    }

    /// Breaking shape: leading, trailing and repeated separators. An off-by-one in the flush would
    /// emit an empty token, which would then be a posting under the empty string.
    #[test]
    fn tokenize_emits_no_empty_tokens() {
        assert_eq!(tokenize("  --a,,  b--  "), vec!["a", "b"]);
        assert!(tokenize("!!! ???").is_empty());
        assert!(tokenize("").is_empty());
    }

    /// Breaking shape: a document that says the same word more than once. Without the dedup, that
    /// value posts two identical `(token, pk)` keys, `insert_entry` stores both, and the lookup
    /// returns the row twice. `tokenize` must still report both occurrences, because BM25's `tf`
    /// is counted from it.
    #[test]
    fn distinct_tokens_dedups_but_tokenize_does_not() {
        assert_eq!(tokenize("cat CAT cat"), vec!["cat", "cat", "cat"]);
        assert_eq!(distinct_tokens("cat CAT cat"), vec!["cat"]);
    }

    /// The documented limits, pinned so a later "improvement" has to change this test on purpose.
    #[test]
    fn tokenize_limits_are_the_documented_ones() {
        // punctuation inside a word splits it
        assert_eq!(tokenize("don't"), vec!["don", "t"]);
        assert_eq!(tokenize("state-of-the-art"), vec!["state", "of", "the", "art"]);
        assert_eq!(tokenize("user@example.com"), vec!["user", "example", "com"]);
        assert_eq!(tokenize("3.14"), vec!["3", "14"]);
        // numbers are terms, compared as text
        assert_ne!(tokenize("007"), tokenize("7"));
        // no stemming
        assert_eq!(tokenize("running"), vec!["running"]);
        // a CJK run is one token: no word segmentation
        assert_eq!(tokenize("東京都"), vec!["東京都"]);
        // accented letters stay inside a token
        assert_eq!(tokenize("Café"), vec!["café"]);
    }

    /// Breaking shape: one alphanumeric run longer than a B+tree leaf can hold. `VARCHAR(60000)`
    /// permits it, and without the cap the `insert` panics inside `split` instead of the value
    /// simply not being indexed. The anti-vacuity half is the second assertion: the neighbouring
    /// tokens of the same value are still indexed, so the cap drops one word rather than the row.
    #[test]
    fn an_over_long_run_is_dropped_and_its_neighbours_are_not() {
        let long = "x".repeat(MAX_TOKEN_BYTES + 1);
        let text = format!("alpha {long} omega");
        assert_eq!(tokenize(&text), vec!["alpha", "omega"]);

        let ok = "y".repeat(MAX_TOKEN_BYTES);
        assert_eq!(tokenize(&ok), vec![ok.clone()], "a token AT the cap is kept");
    }

    /// Breaking shape: a multi-byte character near the cap. The cap is in bytes because the key
    /// encoding is, and a naive `chars().count()` check would let a 3-bytes-per-char token through
    /// at three times the intended size.
    #[test]
    fn the_cap_counts_bytes_not_chars() {
        let wide = "東".repeat(MAX_TOKEN_BYTES / 3 + 1); // 3 bytes each, so just over the cap
        assert!(wide.len() > MAX_TOKEN_BYTES);
        assert!(tokenize(&wide).is_empty());
    }

    /// `Value::Null` sorts below every other variant, which is what makes `(token, Null)` a usable
    /// prefix floor. Pinned here because the prefix scan is silently wrong if that stops holding.
    #[test]
    fn null_is_the_floor_of_the_second_component() {
        let lower = posting_lower_bound("cat");
        let Bound::Included(floor) = lower else {
            panic!("expected an inclusive floor")
        };
        assert!(floor < posting_key("cat", &Value::Integer(i32::MIN)));
        assert!(floor < posting_key("cat", &Value::Varchar(String::new())));
        // and it does not reach into the next token
        assert!(floor > posting_key("bat", &Value::Integer(i32::MAX)));
    }

    #[test]
    fn indexed_text_accepts_varchar_and_null_and_refuses_the_rest() {
        assert_eq!(
            indexed_text(&Value::Varchar("hi".into())).unwrap(),
            Some("hi")
        );
        assert_eq!(indexed_text(&Value::Null).unwrap(), None);
        assert!(indexed_text(&Value::Integer(1)).is_err());
    }
}
