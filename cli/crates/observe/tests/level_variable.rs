//! `Level::from_env` reads `TETRAL_LOG_LEVEL`. The test is alone in its
//! binary because it changes the process's environment.

use observe::{InvalidLevel, Level};

#[test]
fn the_level_comes_from_tetral_log_level() {
    // The values the variable may hold are the table of `Level::parse`.
    let cases: [(Option<&str>, Result<Level, InvalidLevel>); 4] = [
        (None, Ok(Level::Info)),
        (Some(""), Err(InvalidLevel)),
        (Some("debug"), Ok(Level::Debug)),
        (Some("verbose"), Err(InvalidLevel)),
    ];
    for (value, level) in cases {
        // SAFETY: no other thread of this test binary reads the environment.
        unsafe {
            match value {
                Some(value) => std::env::set_var("TETRAL_LOG_LEVEL", value),
                None => std::env::remove_var("TETRAL_LOG_LEVEL"),
            }
        }
        assert_eq!(Level::from_env(), level, "TETRAL_LOG_LEVEL={value:?}");
    }
}
