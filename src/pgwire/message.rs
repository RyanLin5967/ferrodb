//! Backend messages and the framing every one of them shares.
//!
//! ## The one asymmetry worth knowing
//!
//! Every message carries `type_byte + i32 length`, and the length **includes its own four bytes
//! but excludes the type byte**. The startup packet is the exception: it has no type byte at all,
//! because at that point the server does not yet know what protocol it is speaking. Getting this
//! off by one is the classic way a hand-written implementation appears to work and then desyncs
//! a few messages in, so [`Message::encode`] is the only place that computes it.

use std::io::Read;

/// The transaction status byte that rides on every `ReadyForQuery`.
///
/// A client keys real behaviour off this: asyncpg's `is_in_transaction()` reads it, and its
/// transaction manager decides whether to send `ROLLBACK` from the answer. Sending a constant `I`
/// while a `BEGIN` is open is therefore not a harmless simplification — it tells the driver the
/// server is idle when it is not.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TxnStatus {
    /// Not in a transaction block.
    Idle,
    /// Inside a transaction block.
    InTransaction,
}

impl TxnStatus {
    pub fn byte(self) -> u8 {
        match self {
            TxnStatus::Idle => b'I',
            TxnStatus::InTransaction => b'T',
        }
    }
}

/// One column of a `RowDescription`.
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    pub name: String,
    pub type_oid: i32,
    /// The format the values of this column will be sent in: 0 text, 1 binary. It appears in
    /// `RowDescription` as well as in the rows themselves, and the two must agree — a client that
    /// is told `text` and handed eight raw bytes of `int8` reads a mojibake string, silently.
    pub format: i16,
}

impl Field {
    /// A text-format field, which is what `Describe` announces before any `Bind` has chosen a
    /// format. The formats in a `RowDescription` produced by `Describe` on a *statement* are
    /// always zero in real Postgres too, because no portal exists yet to have chosen them.
    pub fn text(name: impl Into<String>, type_oid: i32) -> Self {
        Field { name: name.into(), type_oid, format: 0 }
    }
}

/// A backend message, ready to be framed.
pub enum Message {
    AuthenticationOk,
    ParameterStatus(&'static str, String),
    BackendKeyData { pid: i32, key: i32 },
    ReadyForQuery(TxnStatus),
    RowDescription(Vec<Field>),
    /// Column values, already encoded in the format the corresponding [`Field`] announced.
    /// `None` is SQL NULL, which the protocol encodes as length -1 rather than as any byte
    /// string — a client cannot tell those apart otherwise.
    DataRow(Vec<Option<Vec<u8>>>),
    CommandComplete(String),
    EmptyQueryResponse,
    ErrorResponse { severity: &'static str, code: &'static str, message: String },
    // ---- extended query protocol -------------------------------------------------------------
    ParseComplete,
    BindComplete,
    CloseComplete,
    /// One type OID per `$n`, in order. A client uses these to decide how to *encode* the
    /// arguments it is about to send, so they are a promise about what the server will accept.
    ParameterDescription(Vec<i32>),
    /// The answer to `Describe` for a statement that returns no rows at all. Distinct from a
    /// `RowDescription` with zero fields, which means "rows, each with no columns".
    NoData,
    /// `Execute` hit its row limit and the portal still has rows. The client resumes with another
    /// `Execute` on the same portal.
    PortalSuspended,
}

impl Message {
    pub fn tag(&self) -> u8 {
        match self {
            Message::AuthenticationOk => b'R',
            Message::ParameterStatus(..) => b'S',
            Message::BackendKeyData { .. } => b'K',
            Message::ReadyForQuery(_) => b'Z',
            Message::RowDescription(_) => b'T',
            Message::DataRow(_) => b'D',
            Message::CommandComplete(_) => b'C',
            Message::EmptyQueryResponse => b'I',
            Message::ErrorResponse { .. } => b'E',
            Message::ParseComplete => b'1',
            Message::BindComplete => b'2',
            Message::CloseComplete => b'3',
            Message::ParameterDescription(_) => b't',
            Message::NoData => b'n',
            Message::PortalSuspended => b's',
        }
    }

    pub fn body(&self) -> Vec<u8> {
        let mut b = Vec::new();
        match self {
            Message::AuthenticationOk => b.extend_from_slice(&0i32.to_be_bytes()),
            Message::ParameterStatus(k, v) => {
                push_cstr(&mut b, k);
                push_cstr(&mut b, v);
            }
            Message::BackendKeyData { pid, key } => {
                b.extend_from_slice(&pid.to_be_bytes());
                b.extend_from_slice(&key.to_be_bytes());
            }
            Message::ReadyForQuery(status) => b.push(status.byte()),
            Message::RowDescription(fields) => {
                b.extend_from_slice(&(fields.len() as i16).to_be_bytes());
                for f in fields {
                    push_cstr(&mut b, &f.name);
                    b.extend_from_slice(&0i32.to_be_bytes()); // table oid: unknown
                    b.extend_from_slice(&0i16.to_be_bytes()); // column attr: unknown
                    b.extend_from_slice(&f.type_oid.to_be_bytes());
                    b.extend_from_slice(&(-1i16).to_be_bytes()); // type size: variable
                    b.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier: none
                    b.extend_from_slice(&f.format.to_be_bytes());
                }
            }
            Message::DataRow(cols) => {
                b.extend_from_slice(&(cols.len() as i16).to_be_bytes());
                for c in cols {
                    match c {
                        None => b.extend_from_slice(&(-1i32).to_be_bytes()),
                        Some(bytes) => {
                            b.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                            b.extend_from_slice(bytes);
                        }
                    }
                }
            }
            Message::CommandComplete(tag) => push_cstr(&mut b, tag),
            Message::EmptyQueryResponse => {}
            Message::ErrorResponse { severity, code, message } => {
                b.push(b'S');
                push_cstr(&mut b, severity);
                // 'V' is the non-localised severity. libpq-era clients read 'S', which a server is
                // allowed to translate; 'V' is the one a program should branch on.
                b.push(b'V');
                push_cstr(&mut b, severity);
                b.push(b'C');
                push_cstr(&mut b, code);
                b.push(b'M');
                push_cstr(&mut b, message);
                b.push(0); // terminator for the field list
            }
            Message::ParseComplete
            | Message::BindComplete
            | Message::CloseComplete
            | Message::NoData
            | Message::PortalSuspended => {}
            Message::ParameterDescription(oids) => {
                b.extend_from_slice(&(oids.len() as i16).to_be_bytes());
                for oid in oids {
                    b.extend_from_slice(&oid.to_be_bytes());
                }
            }
        }
        b
    }

    /// Frame the message. The length covers itself and the body, but **not** the type byte.
    pub fn encode(&self) -> Vec<u8> {
        let body = self.body();
        let mut out = Vec::with_capacity(body.len() + 5);
        out.push(self.tag());
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }

    /// An `ERROR`-severity response. Every refusal in this server goes through here so that the
    /// SQLSTATE is stated rather than defaulted.
    pub fn error(code: &'static str, message: impl Into<String>) -> Message {
        Message::ErrorResponse { severity: "ERROR", code, message: message.into() }
    }
}

pub fn push_cstr(b: &mut Vec<u8>, s: &str) {
    b.extend_from_slice(s.as_bytes());
    b.push(0);
}

pub fn read_exact(r: &mut impl Read, n: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn read_i32(r: &mut impl Read) -> std::io::Result<i32> {
    let b = read_exact(r, 4)?;
    Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

/// A cursor over one frontend message body.
///
/// Every accessor returns `Err` on a short read rather than panicking or reading past the end.
/// That matters more here than anywhere else in the server: the body length is chosen by the
/// client, so a truncated `Bind` is *reachable input*, not an internal invariant. Indexing a slice
/// directly is how a hand-written protocol implementation turns a malformed packet into a panic,
/// and a panicking connection thread takes the whole server down when it is holding the catalog
/// lock.
pub struct Body<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Body<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Body { buf, pos: 0 }
    }

    fn need(&self, n: usize) -> Result<(), String> {
        if self.pos + n > self.buf.len() {
            return Err(format!(
                "message body is {} bytes but {} more were needed at offset {}",
                self.buf.len(),
                n,
                self.pos
            ));
        }
        Ok(())
    }

    pub fn u8(&mut self) -> Result<u8, String> {
        self.need(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    pub fn i16(&mut self) -> Result<i16, String> {
        self.need(2)?;
        let v = i16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    pub fn i32(&mut self) -> Result<i32, String> {
        self.need(4)?;
        let v = i32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    /// A NUL-terminated string. Invalid UTF-8 is an error rather than a lossy replacement: a
    /// statement name that silently changed bytes would be looked up under a name the client never
    /// sent, and the failure would surface much later as "unknown statement".
    pub fn cstr(&mut self) -> Result<String, String> {
        let end = self.buf[self.pos..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| "unterminated string in message body".to_string())?;
        let s = std::str::from_utf8(&self.buf[self.pos..self.pos + end])
            .map_err(|e| format!("string in message body is not UTF-8: {e}"))?
            .to_string();
        self.pos += end + 1;
        Ok(s)
    }

    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8], String> {
        self.need(n)?;
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// Everything after the cursor was not consumed. Reported rather than ignored: trailing bytes
    /// mean this server and the client disagree about the shape of the message, which is exactly
    /// the desync that is otherwise invisible until a later message lands in the wrong place.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_length_covers_itself_and_the_body_but_not_the_tag() {
        // The off-by-one that makes a hand-written implementation desync a few messages in.
        let m = Message::CommandComplete("SELECT 1".into());
        let bytes = m.encode();
        assert_eq!(bytes[0], b'C');
        let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        assert_eq!(len as usize, bytes.len() - 1, "length must exclude the type byte");
        assert_eq!(len as usize, m.body().len() + 4, "length must include its own 4 bytes");
    }

    /// Breaking shape: an empty body. `ParseComplete` and friends carry nothing, so their length
    /// is exactly 4 — the case where an implementation that wrote `body.len()` instead of
    /// `body.len() + 4` still produces a plausible-looking message.
    #[test]
    fn a_bodyless_message_is_still_four_bytes_of_length() {
        for m in [Message::ParseComplete, Message::BindComplete, Message::CloseComplete,
                  Message::NoData, Message::PortalSuspended] {
            let bytes = m.encode();
            assert_eq!(bytes.len(), 5, "tag + length and nothing else");
            assert_eq!(i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]), 4);
        }
    }

    #[test]
    fn a_null_column_is_length_minus_one_not_an_empty_byte_string() {
        let bytes = Message::DataRow(vec![None, Some(b"hi".to_vec())]).encode();
        // tag(1) + len(4) + ncols(2) then the first column's length.
        let first = i32::from_be_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);
        assert_eq!(first, -1, "SQL NULL must be -1, or a client cannot distinguish it from a string");
    }

    #[test]
    fn an_empty_string_is_length_zero_and_not_null() {
        let bytes = Message::DataRow(vec![Some(Vec::new())]).encode();
        let first = i32::from_be_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);
        assert_eq!(first, 0, "an empty string must be length 0, distinct from NULL's -1");
    }

    #[test]
    fn error_response_fields_are_terminated_and_typed() {
        let bytes = Message::error("42000", "boom").encode();
        assert_eq!(bytes[0], b'E');
        assert_eq!(*bytes.last().unwrap(), 0, "the field list needs its terminating zero byte");
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("ERROR") && text.contains("42000") && text.contains("boom"));
    }

    #[test]
    fn ready_for_query_carries_the_transaction_status() {
        assert_eq!(*Message::ReadyForQuery(TxnStatus::Idle).body().first().unwrap(), b'I');
        assert_eq!(
            *Message::ReadyForQuery(TxnStatus::InTransaction).body().first().unwrap(),
            b'T',
            "a client that is told 'idle' inside a transaction block will not roll it back"
        );
    }

    #[test]
    fn parameter_description_is_a_count_then_one_oid_each() {
        let body = Message::ParameterDescription(vec![23, 25]).body();
        assert_eq!(i16::from_be_bytes([body[0], body[1]]), 2);
        assert_eq!(i32::from_be_bytes([body[2], body[3], body[4], body[5]]), 23);
        assert_eq!(i32::from_be_bytes([body[6], body[7], body[8], body[9]]), 25);
    }

    #[test]
    fn row_description_advertises_one_field_per_column() {
        let bytes = Message::RowDescription(vec![Field::text("a", 23), Field::text("b", 25)])
            .encode();
        let n = i16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(n, 2);
    }

    /// Breaking shape: a `Bind` that claims two parameters and carries one. Before `Body` existed
    /// this indexed a slice and panicked, which on a threaded server is a killed connection thread
    /// holding whatever locks it had.
    #[test]
    fn a_truncated_body_is_an_error_and_not_a_panic() {
        let buf = [0u8, 1];
        let mut b = Body::new(&buf);
        assert!(b.i16().is_ok());
        assert!(b.i32().is_err(), "reading past the end must be reported");
        assert!(Body::new(&[]).u8().is_err());
        assert!(Body::new(b"no terminator").cstr().is_err());
    }

    #[test]
    fn a_body_reports_what_it_did_not_consume() {
        let buf = [0u8, 1, 9, 9, 9];
        let mut b = Body::new(&buf);
        b.i16().unwrap();
        assert_eq!(b.remaining(), 3, "trailing bytes mean the two ends disagree about the shape");
    }
}
