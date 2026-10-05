use crate::colon::{escape, unescape};

pub const LOG_PREFIX: &str = "ringstore::v1::";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Scalar,
    List,
}

impl Kind {
    fn tag(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::List => "list",
        }
    }
}

pub fn prefix(bin: &str, kind: Kind) -> String {
    format!("{LOG_PREFIX}{}::{}::", escape(bin), kind.tag())
}

pub fn encode(bin: &str, kind: Kind, key: &str) -> String {
    format!("{}{}", prefix(bin, kind), escape(key))
}

pub fn decode(key: &str) -> Option<(String, Kind, String)> {
    let rest = key.strip_prefix(LOG_PREFIX)?;
    let mut fields = rest.split("::");
    let bin = unescape(fields.next()?);
    let kind = match fields.next()? {
        "scalar" => Kind::Scalar,
        "list" => Kind::List,
        _ => return None,
    };
    let key = unescape(fields.next()?);
    if fields.next().is_some() {
        return None;
    }
    Some((bin, kind, key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespaces_and_delimiters_are_unambiguous() {
        let bin = "::||::";
        for key in ["", "::", "|;", "abc|:def"] {
            assert_eq!(
                decode(&encode(bin, Kind::List, key)),
                Some((bin.into(), Kind::List, key.into()))
            );
            assert_ne!(encode(bin, Kind::List, key), encode(bin, Kind::Scalar, key));
        }
        assert!(!encode("alice2", Kind::List, "x").starts_with(&prefix("alice", Kind::List)));
    }
}
