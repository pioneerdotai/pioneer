//! Route PostgreSQL statements before acquiring a pool connection.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StatementRoute {
    Read,
    Write,
}

pub(crate) fn statement_route(sql: &str) -> StatementRoute {
    let mut scanner = Scanner {
        rest: sql.as_bytes(),
    };
    if scanner.is_read_statement() {
        StatementRoute::Read
    } else {
        StatementRoute::Write
    }
}

fn read_command(token: Token<'_>) -> bool {
    token.keyword("select") || token.keyword("values") || token.keyword("table")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token<'a> {
    Word(&'a str),
    Identifier,
    Open,
    Close,
    Comma,
    Semicolon,
    Other,
    End,
    Invalid,
}

impl Token<'_> {
    fn keyword(self, keyword: &str) -> bool {
        matches!(self, Self::Word(word) if word.eq_ignore_ascii_case(keyword))
    }

    fn is_identifier(self) -> bool {
        matches!(self, Self::Word(_) | Self::Identifier)
    }
}

#[derive(Clone, Copy)]
struct Scanner<'a> {
    rest: &'a [u8],
}

impl<'a> Scanner<'a> {
    fn is_read_statement(&mut self) -> bool {
        let mut head = self.next();
        let mut depth = 0;
        // A CTE's command may start with its own WITH clause. Keep the outer
        // query's parenthesis depth on a stack while inspecting its children.
        let mut cte_parents = Vec::new();
        loop {
            match head {
                Token::Open => {
                    depth += 1;
                    head = self.next();
                    continue;
                }
                token if token.keyword("with") => {
                    let mut name = self.next();
                    if name.keyword("recursive") {
                        name = self.next();
                    }
                    cte_parents.push(depth);
                    head = self.start_cte_body(name);
                    depth = 1;
                    continue;
                }
                token if token.keyword("explain") && depth == 0 && cte_parents.is_empty() => {
                    return self.explain_without_execution();
                }
                _ => {}
            }

            if !read_command(head) || !self.read_tail(depth, !cte_parents.is_empty()) {
                return false;
            }
            let Some(&parent_depth) = cte_parents.last() else {
                return true;
            };
            head = self.after_cte_options();
            if head == Token::Comma {
                let name = self.next();
                head = self.start_cte_body(name);
                depth = 1;
            } else {
                cte_parents.pop();
                depth = parent_depth;
            }
        }
    }

    fn start_cte_body(&mut self, name: Token<'_>) -> Token<'a> {
        if !name.is_identifier() {
            return Token::Invalid;
        }
        let mut as_token = self.next();
        if as_token == Token::Open {
            if !self.read_tail(1, true) {
                return Token::Invalid;
            }
            as_token = self.next();
        }
        if !as_token.keyword("as") {
            return Token::Invalid;
        }

        let mut opening = self.next();
        if opening.keyword("not") {
            opening = self.next();
            if !opening.keyword("materialized") {
                return Token::Invalid;
            }
        }
        if opening.keyword("materialized") {
            opening = self.next();
        }
        if opening == Token::Open {
            self.next()
        } else {
            Token::Invalid
        }
    }

    fn after_cte_options(&mut self) -> Token<'a> {
        let mut following = self.next();
        if following.keyword("search") {
            let order = self.next();
            if !(order.keyword("depth") || order.keyword("breadth"))
                || !self.next().keyword("first")
                || !self.next().keyword("by")
                || !self.cte_columns_and_set_column()
            {
                return Token::Invalid;
            }
            following = self.next();
        }
        if following.keyword("cycle") {
            if !self.cte_columns_and_set_column() {
                return Token::Invalid;
            }
            let mut using = self.next();
            if using.keyword("to") {
                // The optional TO/DEFAULT constants precede the reserved USING.
                loop {
                    using = self.next();
                    if using.keyword("using") {
                        break;
                    }
                    if matches!(using, Token::End | Token::Invalid | Token::Semicolon) {
                        return Token::Invalid;
                    }
                }
            }
            if !using.keyword("using") || !self.next().is_identifier() {
                return Token::Invalid;
            }
            following = self.next();
        }
        following
    }

    fn cte_columns_and_set_column(&mut self) -> bool {
        while self.next().is_identifier() {
            let next = self.next();
            if next != Token::Comma {
                return next.keyword("set") && self.next().is_identifier();
            }
        }
        false
    }

    fn read_tail(&mut self, mut depth: usize, stop_at_close: bool) -> bool {
        let mut query_depth = depth;
        let mut outer_queries = Vec::new();
        loop {
            match self.next() {
                Token::Open => depth += 1,
                Token::Close if depth == 0 => return false,
                Token::Close => {
                    if query_depth == depth {
                        query_depth = outer_queries.pop().unwrap_or(0);
                    }
                    depth -= 1;
                    if depth == 0 && stop_at_close {
                        return true;
                    }
                }
                Token::End => return depth == 0 && !stop_at_close,
                Token::Semicolon if depth == 0 && !stop_at_close => {
                    return self.next() == Token::End;
                }
                Token::Invalid | Token::Semicolon => return false,
                token if read_command(token) && depth > query_depth => {
                    outer_queries.push(query_depth);
                    query_depth = depth;
                }
                token if depth == query_depth && token.keyword("into") => return false,
                token if depth == query_depth && token.keyword("for") && self.starts_row_lock() => {
                    return false;
                }
                _ => {}
            }
        }
    }

    fn starts_row_lock(mut self) -> bool {
        let strength = self.next();
        strength.keyword("update")
            || strength.keyword("share")
            || (strength.keyword("no")
                && self.next().keyword("key")
                && self.next().keyword("update"))
            || (strength.keyword("key") && self.next().keyword("share"))
    }

    fn explain_without_execution(&mut self) -> bool {
        loop {
            match self.next() {
                Token::End => return true,
                Token::Semicolon => return self.next() == Token::End,
                Token::Invalid => return false,
                token if token.keyword("analyze") || token.keyword("analyse") => return false,
                _ => {}
            }
        }
    }

    fn next(&mut self) -> Token<'a> {
        loop {
            let input = self.rest;
            let Some((&first, tail)) = input.split_first() else {
                return Token::End;
            };
            if first.is_ascii_whitespace() {
                self.rest = tail;
                continue;
            }
            if self.rest.starts_with(b"--") {
                let end = tail
                    .iter()
                    .position(|&byte| matches!(byte, b'\r' | b'\n'))
                    .unwrap_or(tail.len());
                self.rest = &tail[end..];
                continue;
            }
            if self.rest.starts_with(b"/*") {
                if !self.skip_block_comment() {
                    return Token::Invalid;
                }
                continue;
            }

            self.rest = tail;
            return match first {
                b'(' => Token::Open,
                b')' => Token::Close,
                b',' => Token::Comma,
                b';' => Token::Semicolon,
                b'"' => self.quoted(b'"', false, Token::Identifier),
                b'\'' => self.quoted(b'\'', false, Token::Other),
                b'e' | b'E' if tail.first() == Some(&b'\'') => {
                    self.rest = &tail[1..];
                    self.quoted(b'\'', true, Token::Other)
                }
                b'u' | b'U' if tail.starts_with(b"&\"") => {
                    self.rest = &tail[2..];
                    self.quoted(b'"', false, Token::Identifier)
                }
                b'$' => self.dollar_quoted(tail),
                _ if identifier_start(first) => {
                    let len = tail
                        .iter()
                        .take_while(|&&byte| identifier_continue(byte) || byte == b'$')
                        .count();
                    self.rest = &tail[len..];
                    match std::str::from_utf8(&input[..len + 1]) {
                        Ok(word) => Token::Word(word),
                        Err(_) => Token::Invalid,
                    }
                }
                _ => Token::Other,
            };
        }
    }

    fn quoted(&mut self, delimiter: u8, backslash_escapes: bool, token: Token<'a>) -> Token<'a> {
        while let Some((&first, tail)) = self.rest.split_first() {
            self.rest = tail;
            if first == delimiter {
                if tail.first() != Some(&delimiter) {
                    return token;
                }
                self.rest = &tail[1..];
            } else if backslash_escapes && first == b'\\' {
                if tail.is_empty() {
                    break;
                }
                self.rest = &tail[1..];
            }
        }
        Token::Invalid
    }

    fn dollar_quoted(&mut self, tail: &'a [u8]) -> Token<'a> {
        let tag_len = if tail.first() == Some(&b'$') {
            0
        } else if tail.first().copied().is_some_and(identifier_start) {
            tail.iter()
                .take_while(|&&byte| identifier_continue(byte))
                .count()
        } else {
            return Token::Other; // Includes positional parameters.
        };
        if tail.get(tag_len) != Some(&b'$') {
            return Token::Other;
        }
        let tag_and_close = &tail[..=tag_len];
        let mut body = &tail[tag_len + 1..];
        while let Some(offset) = body.iter().position(|&byte| byte == b'$') {
            let suffix = &body[offset + 1..];
            if let Some(rest) = suffix.strip_prefix(tag_and_close) {
                self.rest = rest;
                return Token::Other;
            }
            body = suffix;
        }
        self.rest = &[];
        Token::Invalid
    }

    fn skip_block_comment(&mut self) -> bool {
        let mut depth = 1;
        self.rest = &self.rest[2..];
        while let Some((first, tail)) = self.rest.split_first() {
            match (*first, tail.first()) {
                (b'/', Some(b'*')) => {
                    depth += 1;
                    self.rest = &tail[1..];
                }
                (b'*', Some(b'/')) => {
                    depth -= 1;
                    self.rest = &tail[1..];
                    if depth == 0 {
                        return true;
                    }
                }
                _ => self.rest = tail,
            }
        }
        false
    }
}

fn identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
}

fn identifier_continue(byte: u8) -> bool {
    identifier_start(byte) || byte.is_ascii_digit()
}
