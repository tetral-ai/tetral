//! The strict pass that every decoder runs over the raw bytes before typed
//! decoding.
//!
//! JSON parsers commonly keep the last of two equal member names, replace a
//! lone surrogate, turn `1e400` into infinity or round a large integer through
//! a float. Two readers that disagree on such input could read one message
//! two ways, so the protocol rejects all of it before any type sees it.

use std::fmt;

/// Objects and arrays nested deeper than this are rejected. `serde_json`, the
/// typed decoder, refuses one level more, so both passes accept the same input.
pub const MAX_NESTING: usize = 127;

/// Why the strict pass rejected the input, with the byte offset where it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrictError {
    pub kind: StrictErrorKind,
    pub offset: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrictErrorKind {
    InvalidUtf8,
    Syntax,
    TrailingData,
    TooDeep,
    DuplicateMember,
    LoneSurrogate,
    NonFiniteNumber,
    IntegerOutOfRange,
}

impl fmt::Display for StrictError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self.kind {
            StrictErrorKind::InvalidUtf8 => "invalid UTF-8",
            StrictErrorKind::Syntax => "invalid JSON",
            StrictErrorKind::TrailingData => "data after the JSON value",
            StrictErrorKind::TooDeep => "nesting too deep",
            StrictErrorKind::DuplicateMember => "a repeated member name",
            StrictErrorKind::LoneSurrogate => "a lone surrogate escape",
            StrictErrorKind::NonFiniteNumber => "a number that is not finite",
            StrictErrorKind::IntegerOutOfRange => "an integer outside the signed 64-bit range",
        };
        write!(f, "{what} at byte {}", self.offset)
    }
}

impl std::error::Error for StrictError {}

/// Accepts exactly one JSON value with only whitespace around it, in valid
/// UTF-8, with no repeated member name in any object, no `\u` escape of a lone
/// surrogate, no number that is infinite as a 64-bit float and no integer
/// token outside the signed 64-bit range.
pub fn check(input: &[u8]) -> Result<(), StrictError> {
    if let Err(error) = std::str::from_utf8(input) {
        return Err(StrictError {
            kind: StrictErrorKind::InvalidUtf8,
            offset: error.valid_up_to(),
        });
    }
    let mut reader = Reader { input, at: 0 };
    reader.whitespace();
    reader.value(0)?;
    reader.whitespace();
    if reader.at != input.len() {
        return Err(reader.error(StrictErrorKind::TrailingData));
    }
    Ok(())
}

struct Reader<'a> {
    input: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn error(&self, kind: StrictErrorKind) -> StrictError {
        StrictError {
            kind,
            offset: self.at,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.at).copied()
    }

    fn whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    fn expect(&mut self, byte: u8) -> Result<(), StrictError> {
        if self.peek() == Some(byte) {
            self.at += 1;
            Ok(())
        } else {
            Err(self.error(StrictErrorKind::Syntax))
        }
    }

    fn literal(&mut self, text: &[u8]) -> Result<(), StrictError> {
        if self.input[self.at..].starts_with(text) {
            self.at += text.len();
            Ok(())
        } else {
            Err(self.error(StrictErrorKind::Syntax))
        }
    }

    fn value(&mut self, depth: usize) -> Result<(), StrictError> {
        match self.peek() {
            Some(b'{') => self.object(depth + 1),
            Some(b'[') => self.array(depth + 1),
            Some(b'"') => self.string().map(drop),
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'n') => self.literal(b"null"),
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => Err(self.error(StrictErrorKind::Syntax)),
        }
    }

    fn object(&mut self, depth: usize) -> Result<(), StrictError> {
        if depth > MAX_NESTING {
            return Err(self.error(StrictErrorKind::TooDeep));
        }
        self.expect(b'{')?;
        self.whitespace();
        if self.peek() == Some(b'}') {
            self.at += 1;
            return Ok(());
        }
        let mut names = std::collections::HashSet::new();
        loop {
            if self.peek() != Some(b'"') {
                return Err(self.error(StrictErrorKind::Syntax));
            }
            let start = self.at;
            let name = self.string()?;
            if !names.insert(name) {
                return Err(StrictError {
                    kind: StrictErrorKind::DuplicateMember,
                    offset: start,
                });
            }
            self.whitespace();
            self.expect(b':')?;
            self.whitespace();
            self.value(depth)?;
            self.whitespace();
            match self.peek() {
                Some(b',') => {
                    self.at += 1;
                    self.whitespace();
                }
                Some(b'}') => {
                    self.at += 1;
                    return Ok(());
                }
                _ => return Err(self.error(StrictErrorKind::Syntax)),
            }
        }
    }

    fn array(&mut self, depth: usize) -> Result<(), StrictError> {
        if depth > MAX_NESTING {
            return Err(self.error(StrictErrorKind::TooDeep));
        }
        self.expect(b'[')?;
        self.whitespace();
        if self.peek() == Some(b']') {
            self.at += 1;
            return Ok(());
        }
        loop {
            self.value(depth)?;
            self.whitespace();
            match self.peek() {
                Some(b',') => {
                    self.at += 1;
                    self.whitespace();
                }
                Some(b']') => {
                    self.at += 1;
                    return Ok(());
                }
                _ => return Err(self.error(StrictErrorKind::Syntax)),
            }
        }
    }

    /// Reads a string token and returns its decoded text, so that two member
    /// names that differ only in escaping compare equal.
    fn string(&mut self) -> Result<String, StrictError> {
        self.expect(b'"')?;
        let mut text = String::new();
        loop {
            let start = self.at;
            match self.peek() {
                None => return Err(self.error(StrictErrorKind::Syntax)),
                Some(b'"') => {
                    self.at += 1;
                    return Ok(text);
                }
                Some(b'\\') => {
                    self.at += 1;
                    let escaped = match self.peek() {
                        Some(b'"') => '"',
                        Some(b'\\') => '\\',
                        Some(b'/') => '/',
                        Some(b'b') => '\u{8}',
                        Some(b'f') => '\u{c}',
                        Some(b'n') => '\n',
                        Some(b'r') => '\r',
                        Some(b't') => '\t',
                        Some(b'u') => {
                            self.at += 1;
                            text.push(self.unicode_escape(start)?);
                            continue;
                        }
                        _ => return Err(self.error(StrictErrorKind::Syntax)),
                    };
                    self.at += 1;
                    text.push(escaped);
                }
                Some(byte) if byte < 0x20 => return Err(self.error(StrictErrorKind::Syntax)),
                Some(_) => {
                    // The input is valid UTF-8, so the run up to the next quote,
                    // backslash or control byte is whole characters.
                    let run = self.input[self.at..]
                        .iter()
                        .position(|&byte| byte == b'"' || byte == b'\\' || byte < 0x20)
                        .map_or(self.input.len(), |length| self.at + length);
                    text.push_str(
                        std::str::from_utf8(&self.input[self.at..run]).unwrap_or_default(),
                    );
                    self.at = run;
                }
            }
        }
    }

    /// Reads the four hexadecimal digits after `\u`, and the low half that a
    /// high surrogate needs.
    fn unicode_escape(&mut self, escape_start: usize) -> Result<char, StrictError> {
        let lone = StrictError {
            kind: StrictErrorKind::LoneSurrogate,
            offset: escape_start,
        };
        let unit = self.hex4()?;
        match unit {
            0xD800..=0xDBFF => {
                if !self.input[self.at..].starts_with(b"\\u") {
                    return Err(lone);
                }
                self.at += 2;
                let low = self.hex4()?;
                if !(0xDC00..=0xDFFF).contains(&low) {
                    return Err(lone);
                }
                let code = 0x10000 + ((u32::from(unit) - 0xD800) << 10) + (u32::from(low) - 0xDC00);
                char::from_u32(code).ok_or(lone)
            }
            0xDC00..=0xDFFF => Err(lone),
            _ => char::from_u32(u32::from(unit)).ok_or(lone),
        }
    }

    fn hex4(&mut self) -> Result<u16, StrictError> {
        let digits = self
            .input
            .get(self.at..self.at + 4)
            .filter(|digits| digits.iter().all(u8::is_ascii_hexdigit))
            .ok_or_else(|| self.error(StrictErrorKind::Syntax))?;
        let value = u16::from_str_radix(std::str::from_utf8(digits).unwrap_or_default(), 16)
            .map_err(|_| self.error(StrictErrorKind::Syntax))?;
        self.at += 4;
        Ok(value)
    }

    fn digits(&mut self) -> usize {
        let start = self.at;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.at += 1;
        }
        self.at - start
    }

    fn number(&mut self) -> Result<(), StrictError> {
        let start = self.at;
        if self.peek() == Some(b'-') {
            self.at += 1;
        }
        match self.peek() {
            Some(b'0') => self.at += 1,
            Some(b'1'..=b'9') => {
                self.digits();
            }
            _ => return Err(self.error(StrictErrorKind::Syntax)),
        }
        let mut integer = true;
        if self.peek() == Some(b'.') {
            integer = false;
            self.at += 1;
            if self.digits() == 0 {
                return Err(self.error(StrictErrorKind::Syntax));
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            integer = false;
            self.at += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.at += 1;
            }
            if self.digits() == 0 {
                return Err(self.error(StrictErrorKind::Syntax));
            }
        }
        let token = std::str::from_utf8(&self.input[start..self.at]).unwrap_or_default();
        let at_start = |kind| StrictError {
            kind,
            offset: start,
        };
        if integer {
            token
                .parse::<i64>()
                .map(drop)
                .map_err(|_| at_start(StrictErrorKind::IntegerOutOfRange))
        } else if token.parse::<f64>().is_ok_and(f64::is_finite) {
            Ok(())
        } else {
            Err(at_start(StrictErrorKind::NonFiniteNumber))
        }
    }
}
