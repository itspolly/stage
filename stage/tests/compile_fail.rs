//! Tests 11 & 13: compile-time guarantees, checked with `trybuild`.
//!
//! These assert that certain misuses *fail to compile*. The expected compiler
//! output lives in the matching `tests/ui/*.stderr` files; regenerate them with
//! `TRYBUILD=overwrite cargo test --test compile_fail` after a toolchain bump.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
