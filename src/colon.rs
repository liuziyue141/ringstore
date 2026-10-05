//! Key-component escaping for the operation namespace.

/// Escapes a string with backslashes and colons
///
/// ```rust
/// use ringstore::colon::*;
/// assert_eq!("Testing||Escape|;Sequences", escape("Testing|Escape:Sequences"));
/// assert_eq!("Testing|Escape:Sequences", unescape(escape("Testing|Escape:Sequences")));
/// ```
pub fn escape<T: Into<String>>(s: T) -> String {
    s.into().replace('|', "||").replace(':', "|;")
}

/// Unescapes a string with the values used in [escape]. See [escape] for
/// examples.
pub fn unescape<T: Into<String>>(s: T) -> String {
    let mut out = vec![];
    let mut escaping = false;
    for x in s.into().chars() {
        if !escaping {
            if x == '|' {
                escaping = true;
            } else {
                out.push(x);
            }
        } else {
            if x == ';' {
                out.push(':');
            } else if x == '|' {
                out.push('|');
            } else {
                // should not occur
                out.push(x);
            }
            escaping = false;
        }
    }
    out.into_iter().collect()
}
