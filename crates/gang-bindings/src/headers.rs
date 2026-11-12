//! The `headers` command: the production security headers as a JSON object.

use serde::Serializer as _;

/// Renders the production security headers as a pretty-printed JSON object.
pub(crate) fn render() -> Result<String, crate::Error> {
    render_headers(&gang_ui::SECURITY_HEADERS)
}

/// Renders `(name, value)` pairs as a pretty-printed JSON object in table order.
fn render_headers(headers: &[(&str, &str)]) -> Result<String, crate::Error> {
    let mut buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"  ");
    let mut serializer = serde_json::Serializer::with_formatter(&mut buf, formatter);
    serializer.collect_map(headers.iter().copied())?;
    buf.push(b'\n');
    Ok(String::from_utf8(buf)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_a_pretty_object_in_table_order() {
        let rendered = render_headers(&[("b", "2"), ("a", "1")]).expect("render headers");
        assert_eq!(rendered, "{\n  \"b\": \"2\",\n  \"a\": \"1\"\n}\n");
    }

    #[test]
    fn escapes_quotes_backslashes_and_control_characters() {
        assert_eq!(
            render_headers(&[("a\"b\\c\td\u{1}", "value")]).expect("render headers"),
            "{\n  \"a\\\"b\\\\c\\td\\u0001\": \"value\"\n}\n"
        );
    }

    #[test]
    fn renders_an_empty_object_with_a_trailing_newline() {
        assert_eq!(render_headers(&[]).expect("render headers"), "{}\n");
    }
}
