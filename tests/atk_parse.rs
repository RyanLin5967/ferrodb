//! Attack probes on Publication::parse: can it be made WIDER than the file says?
use ferrodb::replication::publication::Publication;

fn show(tag: &str, text: &str) {
    print!("[{tag}] input = {text:?}\n        -> ");
    match Publication::parse(text) {
        Err(e) => println!("REFUSED: {e}"),
        Ok(p) => {
            let mut desc = format!("name={:?}", p.name());
            for t in p.tables() {
                desc.push_str(&format!("  table {t:?} => {:?}", p.columns_of(t).unwrap()));
            }
            println!("PARSED {desc}");
        }
    }
}

#[test]
fn parse_probes() {
    show("plain", "publication p\ncustomers: id, name\n");
    // A table name containing a colon: split_once takes the FIRST colon.
    show("colon-in-table", "publication p\npublic:customers: id, ssn\n");
    show("colon-only-line", "publication p\ncustomers: id, name\npublic:orders: id, ssn\n");
    // CRLF
    show("crlf", "publication p\r\ncustomers: id, name\r\n");
    // lone CR
    show("cr-only", "publication p\rcustomers: id, name\r");
    // BOM
    show("bom", "\u{feff}publication p\ncustomers: id, name\n");
    // trailing spaces / tabs
    show("trailing-space", "publication p   \ncustomers: id, name   \n");
    show("tab-after-publication", "publication\tp\ncustomers: id, name\n");
    show("tab-after-publication-and-colon", "publication\tp: x\ncustomers: id, name\n");
    // table twice in different case
    show("case-twice", "publication p\ncustomers: id\nCustomers: id, ssn\n");
    // a column list that mentions the denied name after a comment marker
    show("hash-in-col", "publication p\ncustomers: id, ss#n\n");
    show("hash-in-table", "publication p\ncus#tomers: id, ssn\n");
    // NBSP as separator
    show("nbsp", "publication p\ncustomers:\u{a0}id,\u{a0}ssn\n");
    // a name that is only whitespace-ish
    show("vertical-tab", "publication p\ncustomers: id,\u{b}ssn\n");
    // second header
    show("two-headers", "publication p\npublication q\ncustomers: id\n");
    // header with a colon in the name, before any table
    show("header-with-colon", "publication p:q\ncustomers: id\n");
    // A table line that *looks* like a header
    show("table-named-publication", "publication p\npublication x: id, ssn\n");
    show("publication-prefix-table", "publications: id, ssn\npublication p\n");
    // empty-ish
    show("empty", "");
    show("only-comment", "# publication p\ncustomers: id\n");
    // NUL byte
    show("nul", "publication p\ncustomers: id, ssn\u{0}\n");
    // Unicode look-alike / normalization
    show("fullwidth", "publication p\ncustomers: id, \u{ff53}sn\n");
    // trailing comma
    show("trailing-comma", "publication p\ncustomers: id,\n");
}
