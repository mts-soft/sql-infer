use std::borrow::Cow;

const ILLEGAL_CHARACTERS: &[&str] = &["\"", "\'"];

pub fn escape_string(string: &str) -> String {
    let mut out = Cow::Borrowed(string);
    for char in ILLEGAL_CHARACTERS {
        out = Cow::Owned(string.replace(char, &format!("\\{char}")));
    }
    out.into_owned()
}
