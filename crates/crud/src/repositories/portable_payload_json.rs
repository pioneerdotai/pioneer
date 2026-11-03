//! Bounded streaming projection of SQLite's strict-JSON legacy predicate:
//! coalesce(json_extract(mapping,'$.pioneerContextBasis.historyJson'),'[]')<>'[]'.
//! Only two short keys and the exact string [] are retained. Duplicate object
//! labels use the first match, as SQLite json_extract does. Escaped non-ASCII
//! scalars need no allocation: neither selected key nor [] contains one.
#[derive(Default)]
pub(super) struct Projection {
    stack: Vec<Frame>,
    token: Option<Token>,
    root: bool,
    invalid: bool,
    basis_seen: bool,
    history_seen: bool,
    opaque: bool,
    nul: bool,
}
struct Frame {
    object: bool,
    state: u8,
    scope: u8,
    path: u8,
    selected_array: bool,
}
// Object states: 0 key-or-end, 1 key, 2 colon, 3 value, 4 comma-or-end.
// Array states: 0 value-or-end, 3 value, 4 comma-or-end.
enum Token {
    String {
        key: bool,
        capture: u8,
        bytes: Vec<u8>,
        escape: u8,
        unicode: u16,
        digits: u8,
    },
    Number(u8),
    Literal(&'static [u8], usize),
}
impl Projection {
    pub(super) fn feed(&mut self, bytes: &[u8]) {
        for &b in bytes {
            if b == 0 {
                self.nul = true;
            }
            if !self.invalid {
                self.byte(b);
            }
        }
    }
    pub(super) fn finish(&mut self) -> bool {
        if let Some(token) = self.token.take() {
            match token {
                Token::Number(s) if matches!(s, 1 | 2 | 4 | 7) => {}
                Token::Literal(word, n) if n == word.len() => {}
                _ => self.invalid = true,
            }
        }
        self.invalid || !self.root || !self.stack.is_empty() || self.opaque
    }
    pub(super) fn unsupported(&self) -> bool {
        self.nul
    }
    fn byte(&mut self, b: u8) {
        if let Some(token) = self.token.take() {
            match token {
                Token::String {
                    key,
                    capture,
                    mut bytes,
                    mut escape,
                    mut unicode,
                    mut digits,
                } => {
                    let mut decoded = None;
                    if escape == 2 {
                        if let Some(d) = (b as char).to_digit(16).filter(|_| b.is_ascii_hexdigit())
                        {
                            unicode = (unicode << 4) | d as u16;
                            digits += 1;
                            if digits == 4 {
                                decoded = Some(if unicode <= 127 { unicode as u8 } else { 0x80 });
                                escape = 0;
                            }
                        } else {
                            self.invalid = true;
                            return;
                        }
                    } else if escape == 1 {
                        if b == b'u' {
                            escape = 2;
                            digits = 0;
                            unicode = 0;
                        } else {
                            decoded = match b {
                                b'"' | b'\\' | b'/' => Some(b),
                                b'b' => Some(8),
                                b'f' => Some(12),
                                b'n' => Some(10),
                                b'r' => Some(13),
                                b't' => Some(9),
                                _ => {
                                    self.invalid = true;
                                    return;
                                }
                            };
                            escape = 0;
                        }
                    } else if b == b'"' {
                        if key {
                            let path = if bytes == b"pioneerContextBasis" {
                                1
                            } else if bytes == b"historyJson" {
                                2
                            } else {
                                0
                            };
                            if let Some(frame) = self.stack.last_mut() {
                                frame.path = path;
                                frame.state = 2;
                            } else {
                                self.invalid = true;
                            }
                        } else if capture == 2 {
                            self.opaque = bytes != b"[]";
                        }
                        return;
                    } else if b == b'\\' {
                        escape = 1;
                    } else if b < 0x20 {
                        self.invalid = true;
                        return;
                    } else {
                        decoded = Some(b);
                    }
                    if let Some(c) = decoded {
                        if capture != 0 && bytes.len() < 24 {
                            bytes.push(c);
                        }
                    }
                    self.token = Some(Token::String {
                        key,
                        capture,
                        bytes,
                        escape,
                        unicode,
                        digits,
                    });
                    return;
                }
                Token::Literal(word, n) => {
                    if n < word.len() {
                        if b == word[n] {
                            self.token = Some(Token::Literal(word, n + 1));
                        } else {
                            self.invalid = true;
                        }
                        return;
                    }
                    if !delimiter(b) {
                        self.invalid = true;
                        return;
                    }
                }
                Token::Number(state) => {
                    let next = match (state, b) {
                        (0, b'0') => Some(1),
                        (0, b'1'..=b'9') => Some(2),
                        (1 | 2, b'.') => Some(3),
                        (1 | 2 | 4, b'e' | b'E') => Some(5),
                        (2, b'0'..=b'9') => Some(2),
                        (3 | 4, b'0'..=b'9') => Some(4),
                        (5, b'+' | b'-') => Some(6),
                        (5 | 6 | 7, b'0'..=b'9') => Some(7),
                        _ => None,
                    };
                    if let Some(next) = next {
                        self.token = Some(Token::Number(next));
                        return;
                    }
                    if !matches!(state, 1 | 2 | 4 | 7) || !delimiter(b) {
                        self.invalid = true;
                        return;
                    }
                }
            }
        }
        if matches!(b, b' ' | b'\n' | b'\r' | b'\t') {
            return;
        }
        if let Some(frame) = self.stack.last_mut() {
            if frame.object {
                match frame.state {
                    0 if b == b'}' => {
                        self.stack.pop();
                        return;
                    }
                    0 | 1 if b == b'"' => {
                        frame.state = 2;
                        self.token = Some(Token::String {
                            key: true,
                            capture: 1,
                            bytes: Vec::new(),
                            escape: 0,
                            unicode: 0,
                            digits: 0,
                        });
                        return;
                    }
                    2 if b == b':' => {
                        frame.state = 3;
                        return;
                    }
                    4 if b == b',' => {
                        frame.state = 1;
                        return;
                    }
                    4 if b == b'}' => {
                        self.stack.pop();
                        return;
                    }
                    3 => {}
                    _ => {
                        self.invalid = true;
                        return;
                    }
                }
            } else {
                match frame.state {
                    0 if b == b']' => {
                        self.stack.pop();
                        return;
                    }
                    4 if b == b',' => {
                        frame.state = 3;
                        return;
                    }
                    4 if b == b']' => {
                        self.stack.pop();
                        return;
                    }
                    0 | 3 => {}
                    _ => {
                        self.invalid = true;
                        return;
                    }
                }
            }
        } else if self.root {
            self.invalid = true;
            return;
        }
        let mut path = 0;
        if let Some(frame) = self.stack.last_mut() {
            if frame.selected_array {
                self.opaque = true;
            }
            if frame.object && frame.scope == 1 && frame.path == 1 && !self.basis_seen {
                self.basis_seen = true;
                path = 1;
            }
            if frame.object && frame.scope == 2 && frame.path == 2 && !self.history_seen {
                self.history_seen = true;
                path = 2;
            }
            frame.state = 4;
        } else {
            self.root = true;
        }
        let scope = if self.stack.is_empty() {
            1
        } else if path == 1 {
            2
        } else {
            0
        };
        if path == 2 {
            self.opaque = b != b'n' && b != b'[';
        }
        match b {
            b'{' | b'[' => {
                if self.stack.len() >= 1000 {
                    self.invalid = true;
                    return;
                }
                self.stack.push(Frame {
                    object: b == b'{',
                    state: 0,
                    scope,
                    path: 0,
                    selected_array: path == 2 && b == b'[',
                });
            }
            b'"' => {
                self.token = Some(Token::String {
                    key: false,
                    capture: if path == 2 { 2 } else { 0 },
                    bytes: Vec::new(),
                    escape: 0,
                    unicode: 0,
                    digits: 0,
                })
            }
            b'n' => self.token = Some(Token::Literal(b"null", 1)),
            b't' => self.token = Some(Token::Literal(b"true", 1)),
            b'f' => self.token = Some(Token::Literal(b"false", 1)),
            b'-' => self.token = Some(Token::Number(0)),
            b'0' => self.token = Some(Token::Number(1)),
            b'1'..=b'9' => self.token = Some(Token::Number(2)),
            _ => self.invalid = true,
        }
    }
}
fn delimiter(b: u8) -> bool {
    matches!(b, b' ' | b'\n' | b'\r' | b'\t' | b',' | b']' | b'}')
}
