//! A record field cannot hold a `String` or a `&str` (see `RecordValue`).

#[test]
fn a_string_field_does_not_compile() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/string_field.rs");
    cases.compile_fail("tests/ui/str_field.rs");
}
