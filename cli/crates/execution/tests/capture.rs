//! The capture buffer of a task's stream: head and tail retention, the
//! cumulative counters, the byte and line budgets of a snapshot, UTF-8
//! boundaries, and windows that each take only the output since the last.

use execution::capture::{STORED_HALF_BYTES, StreamCapture};
use execution::report::StreamOutput;
use protocol::tool::StreamSnapshot;

/// A capture whose buffers each keep `store` bytes, split evenly between
/// head and tail.
fn capture(store: usize) -> StreamCapture {
    StreamCapture::with_halves(store / 2, store - store / 2)
}

fn written(store: usize, bytes: &[u8]) -> StreamCapture {
    let mut capture = capture(store);
    capture.write(bytes);
    capture
}

/// `(text, total_bytes, total_lines, returned_bytes, truncated)`.
fn parts(snapshot: StreamSnapshot) -> (String, i64, i64, i64, bool) {
    (
        snapshot.text,
        snapshot.total_bytes.get(),
        snapshot.total_lines.get(),
        snapshot.returned_bytes.get(),
        snapshot.truncated,
    )
}

#[test]
fn a_small_output_is_returned_whole() {
    let snapshot = written(1024, b"one\ntwo\n").snapshot(51_200, 2_000);
    assert_eq!(parts(snapshot), ("one\ntwo\n".into(), 8, 2, 8, false));
}

#[test]
fn a_window_without_a_final_newline_is_returned_whole() {
    let mut capture = written(1024, b"ready");
    let window = capture.take_window().snapshot(51_200, 2_000);
    assert_eq!(parts(window), ("ready".into(), 5, 0, 5, false));
}

#[test]
fn a_window_taken_before_any_output_is_empty_and_the_next_holds_the_new_output() {
    let mut capture = capture(1024);
    let empty = capture.take_window().snapshot(51_200, 2_000);
    assert_eq!(parts(empty), (String::new(), 0, 0, 0, false));
    capture.write(b"ready\n");
    let window = capture.take_window().snapshot(51_200, 2_000);
    assert_eq!(parts(window), ("ready\n".into(), 6, 1, 6, false));
}

#[test]
fn an_output_beyond_the_store_keeps_its_head_and_tail_around_the_marker() {
    let snapshot = written(10, b"abcdefghijklmnopqrst").snapshot(10, 2_000);
    assert_eq!(
        parts(snapshot),
        (
            "abcde\n[... 10 bytes truncated ...]\npqrst".into(),
            20,
            0,
            10,
            true
        )
    );
}

#[test]
fn a_hundred_mebibytes_are_counted_while_the_stored_bytes_stay_bounded() {
    let mut capture = StreamCapture::new();
    let chunk = vec![b'a'; 1024 * 1024];
    for _ in 0..100 {
        capture.write(&chunk);
    }
    let whole = capture.snapshot(64 * 1024, 2_000);
    assert_eq!(whole.total_bytes.get(), 100 * 1024 * 1024);
    assert!(whole.truncated);
    assert!(whole.returned_bytes.get() <= 64 * 1024);
    assert!(capture.stored_bytes() <= 4 * STORED_HALF_BYTES);

    let window = capture.take_window().snapshot(64 * 1024, 2_000);
    let omitted = 100 * 1024 * 1024 - window.returned_bytes.get();
    assert!(
        window
            .text
            .contains(&format!("\n[... {omitted} bytes truncated ...]\n")),
        "the window counts every byte written to it"
    );
}

#[test]
fn a_visible_budget_below_the_store_returns_its_head_and_tail_halves() {
    let output = ["a".repeat(40), "z".repeat(40)].concat();
    let snapshot = written(1024, output.as_bytes()).snapshot(20, 2_000);
    let expected = format!(
        "{}\n[... 60 bytes truncated ...]\n{}",
        "a".repeat(10),
        "z".repeat(10)
    );
    assert_eq!(parts(snapshot), (expected, 80, 0, 20, true));
}

#[test]
fn a_line_budget_splits_between_head_and_tail_lines() {
    let lines = b"h1\nh2\nh3\nh4\nh5\n";
    for (store, visible_bytes) in [(1024, 1024), (12, 12)] {
        let snapshot = written(store, lines).snapshot(visible_bytes, 4);
        assert_eq!(
            parts(snapshot),
            (
                "h1\nh2\n\n[... 3 bytes truncated ...]\nh4\nh5\n".into(),
                15,
                5,
                12,
                true
            ),
            "store {store}"
        );
    }
}

#[test]
fn one_visible_line_comes_from_the_tail_only() {
    let snapshot = written(1024, b"one\ntwo\nthree\n").snapshot(1024, 1);
    assert_eq!(
        parts(snapshot),
        (
            "\n[... 8 bytes truncated ...]\nthree\n".into(),
            14,
            3,
            6,
            true
        )
    );
}

#[test]
fn each_window_holds_only_its_own_output_with_the_streams_counters() {
    let mut capture = written(1024, b"one\n");
    let first = capture.take_window().snapshot(1024, 2_000);
    assert_eq!(parts(first), ("one\n".into(), 4, 1, 4, false));
    capture.write(b"two\n");
    let second = capture.take_window().snapshot(1024, 2_000);
    assert_eq!(parts(second), ("two\n".into(), 8, 2, 4, false));
    assert_eq!(capture.snapshot(1024, 2_000).text, "one\ntwo\n");
}

#[test]
fn adjacent_retained_ranges_preserve_the_character_at_their_join() {
    let snapshot = written(6, "αβγ".as_bytes()).snapshot(6, 2_000);
    assert_eq!(parts(snapshot), ("αβγ".into(), 6, 0, 6, false));
}

#[test]
fn a_real_gap_omits_the_characters_cut_at_its_boundaries() {
    assert_eq!(
        parts(written(6, "αβγδ".as_bytes()).snapshot(6, 2_000)),
        ("α\n[... 4 bytes truncated ...]\nδ".into(), 8, 0, 4, true)
    );
}

#[test]
fn invalid_and_incomplete_bytes_do_not_hide_valid_output_or_claim_truncation() {
    let mut incomplete = vec![b'a'; 30_000];
    incomplete.extend_from_slice(&[0xe2, 0x9c]);
    let chinese = "中".repeat(10_000);
    for (bytes, expected) in [
        (incomplete, "a".repeat(30_000)),
        (b"A\xffB\xfeC".to_vec(), "ABC".into()),
        (b"\x9c\x93ready".to_vec(), "ready".into()),
        (vec![0xff, 0xe2, 0x9c], String::new()),
        (chinese.as_bytes().to_vec(), chinese),
    ] {
        let mut capture = written(65_536, &bytes);
        for snapshot in [
            capture.snapshot(51_200, 2_000),
            capture.take_window().snapshot(51_200, 2_000),
        ] {
            assert_eq!(snapshot.text, expected);
            assert_eq!(snapshot.total_bytes.get(), bytes.len() as i64);
            assert_eq!(snapshot.returned_bytes.get(), expected.len() as i64);
            assert!(!snapshot.truncated);
        }
        assert_eq!(capture.stored_text(), expected);
    }
}

#[test]
fn invalid_bytes_in_retained_text_are_not_counted_as_limit_omissions() {
    assert_eq!(
        parts(written(12, b"a\xffbcdefghij\xfe").snapshot(6, 2_000)),
        ("ab\n[... 6 bytes truncated ...]\nij".into(), 12, 0, 4, true)
    );
}

#[test]
fn invalid_bytes_near_selection_edges_keep_text_and_raw_omission_counts() {
    for (input, expected) in [
        (
            &b"A\xe2BCDEFGHIJK"[..],
            "ABC\n[... 4 bytes truncated ...]\nHIJK",
        ),
        (
            &b"ABCDEFGH\x80IJK"[..],
            "ABCD\n[... 4 bytes truncated ...]\nIJK",
        ),
    ] {
        let snapshot = written(12, input).snapshot(8, 2_000);
        assert_eq!(snapshot.text, expected);
        assert_eq!(snapshot.returned_bytes.get(), 7);
    }
}

#[test]
fn storage_boundaries_distinguish_invalid_input_from_valid_split_characters() {
    for (input, expected) in [
        (&b"abcdef\x80XY"[..], "abc\n[... 3 bytes truncated ...]\nXY"),
        ("αβγδ".as_bytes(), "α\n[... 4 bytes truncated ...]\nδ"),
        (&b"ab\xe2defXYZ"[..], "ab\n[... 3 bytes truncated ...]\nXYZ"),
        ("ab🌍XY".as_bytes(), "ab\n[... 4 bytes truncated ...]\nXY"),
    ] {
        for chunk_size in 1..=input.len() {
            let mut capture = capture(6);
            for chunk in input.chunks(chunk_size) {
                capture.write(chunk);
            }
            assert_eq!(
                capture.snapshot(6, 2000).text,
                expected,
                "chunk size {chunk_size}, input {input:?}"
            );
            assert!(capture.stored_bytes() <= 12);
        }
    }
}
