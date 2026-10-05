# Recorded local validation

Validated on 2026-10-05 in an isolated Linux ARM64 container using Rust 1.99.0 stable. Rust 1.88.0 was also checked against all targets, and the Docker release image built successfully using that version.

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --locked --all-targets -- -D warnings` | Passed |
| `cargo test --locked --all-targets` | 9 unit tests and 14 integration entries passed; integration includes 13 fault scenarios and one service fixture |
| `cargo test --locked --doc` | 1 documentation test passed |
| `cargo +1.88.0 check --locked --all-targets` | Passed |
| `python3 scripts/demo.py` | Passed using the built CLI and real child processes |
| `docker build -t ringstore:showcase .` | Passed |

The demo output matched the README: duplicate values remained distinct, reads succeeded after a replica and primary keeper were killed, the backup repaired an empty replacement, and a new equal append survived removal of earlier occurrences.

These are local results. The GitHub Actions workflow is configured to repeat the Rust checks and demo after publication; no hosted workflow result is claimed here. See [testing.md](testing.md) for the specific claims and limits.
