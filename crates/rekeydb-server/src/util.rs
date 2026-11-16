//! Small shared utilities.

/// Redis-style glob matching (`*`, `?`, character classes, and `\` escapes).
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    match_at(pattern, text)
}

fn match_at(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    match pattern[0] {
        b'*' => {
            let mut rest = &pattern[1..];
            while rest.first() == Some(&b'*') {
                rest = &rest[1..];
            }
            if rest.is_empty() {
                return true;
            }
            for index in 0..=text.len() {
                if match_at(rest, &text[index..]) {
                    return true;
                }
            }
            false
        }
        b'?' => !text.is_empty() && match_at(&pattern[1..], &text[1..]),
        b'[' => {
            let Some((matched, consumed)) = match_class(&pattern[1..], text.first().copied())
            else {
                // Malformed class: treat '[' literally.
                return !text.is_empty() && text[0] == b'[' && match_at(&pattern[1..], &text[1..]);
            };
            matched && !text.is_empty() && match_at(&pattern[1 + consumed..], &text[1..])
        }
        b'\\' if pattern.len() > 1 => {
            !text.is_empty() && text[0] == pattern[1] && match_at(&pattern[2..], &text[1..])
        }
        byte => !text.is_empty() && text[0] == byte && match_at(&pattern[1..], &text[1..]),
    }
}

/// Returns `(matched, bytes_consumed_including_closing_bracket)`.
fn match_class(pattern: &[u8], candidate: Option<u8>) -> Option<(bool, usize)> {
    let candidate = candidate?;
    let mut index = 0;
    let mut negate = false;
    if pattern.get(index) == Some(&b'^') || pattern.get(index) == Some(&b'!') {
        negate = true;
        index += 1;
    }
    let mut matched = false;
    let mut saw_any = false;
    while index < pattern.len() {
        if pattern[index] == b']' && saw_any {
            return Some((matched != negate, index + 1));
        }
        saw_any = true;
        let start = pattern[index];
        if index + 2 < pattern.len() && pattern[index + 1] == b'-' && pattern[index + 2] != b']' {
            let end = pattern[index + 2];
            if start <= candidate && candidate <= end {
                matched = true;
            }
            index += 3;
        } else {
            if start == candidate {
                matched = true;
            }
            index += 1;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_globs() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"a*", b"abc"));
        assert!(glob_match(b"a?c", b"abc"));
        assert!(!glob_match(b"a?c", b"ac"));
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
        assert!(glob_match(b"h[^a]llo", b"hello"));
        assert!(glob_match(b"key:*", b"key:1"));
        assert!(!glob_match(b"key:*", b"other:1"));
    }

    #[test]
    fn escapes() {
        assert!(glob_match(br"a\*b", b"a*b"));
        assert!(!glob_match(br"a\*b", b"axb"));
    }
}
