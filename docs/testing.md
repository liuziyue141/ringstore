# Testing

## Run the checks

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo test --locked --doc
cargo build --locked --bin ringstore
python3 scripts/demo.py
```

GitHub Actions runs the Rust checks and Python demo. The demo exercises the shipped CLI separately. Unit tests live alongside the implementation; process tests live in [tests/faults.rs](../tests/faults.rs).

## What the fault harness does

The parent starts the current integration-test executable with an exact fixture test and role-specific environment variables. Each fixture creates a fresh Tokio runtime and storage object, binds its address, signals readiness, and runs indefinitely. The parent owns the child and kills/waits for it on crash or cleanup. Restart means a new process and empty backend memory.

Bound, non-listening sockets reserve inactive endpoints. Reservations are released immediately before process start, and stopped addresses are reserved again. Tests use isolated port sets, so parallel cases do not intentionally share services. As with any release-then-bind scheme, an unrelated external process can race for the port.

A `PausedCopyStorage` fixture accepts one migration entry and blocks later entries until released through a raw RPC. The parent kills the keeper at that barrier, releases the backend, and starts the replacement keeper. This proves recovery from a particular partial copy, rather than hoping a timed sleep interrupted work.

## Claim-to-test map

| Claim | Representative test |
| --- | --- |
| Client needs no keeper for storage calls. | `single_live_backend_without_keeper_supports_all_storage_calls` |
| Normal writes reach three distinct processes, preserving repeated values. | `writes_replicate_to_three_distinct_nodes_and_preserve_duplicate_values` |
| Reduced redundancy still works with two live backends. | `two_live_backends_receive_two_copies` |
| Reads union disjoint histories and repair the newest scalar. | `reads_merge_disjoint_histories_and_repair_scalar_values` |
| A new keeper resumes an interrupted migration. | `keeper_restarts_partial_migration_by_operation_identity` |
| A backup repairs empty replacement backends. | `backup_keeper_repairs_fresh_backends_after_primary_crash` |
| Failure does not resurrect deleted scalars. | `writes_and_deletions_continue_during_backend_failure` |
| Concurrent appends settle into the same order. | `concurrent_appends_have_one_final_order_across_clients` |
| Concurrent clocks are distinct across restart. | `concurrent_clocks_are_unique_and_survive_allocator_restart` |
| Calls in flight at allocator crash do not collide. | `clock_calls_in_flight_during_crash_and_restart_do_not_collide` |
| Sparse configured membership works at the lab's address count. | `three_live_backends_in_a_300_address_configuration_are_usable` |
| Patterns match logical keys, not escaped fragments. | `key_patterns_match_decoded_characters_without_escape_boundary_matches` |
| Mutation order survives public-clock saturation. | `storage_mutations_still_advance_after_the_public_clock_saturates` |

Operation unit tests also check merge associativity, commutativity, idempotence, conflicting identities, removal after late delivery, scalar deletion, and differences without timestamp cutoffs. Ring tests check normalization, distinct traversal, input permutation, and invalid configurations.

## Limits of the evidence

These are functional and schedule-specific fault tests. The 300-address case runs three live backend processes and 297 unavailable configured endpoints. It is not a 300-process load benchmark. The suite does not establish safety under partitions, arbitrary simultaneous failures, membership reconfiguration, data corruption, or unbounded histories. There is no universal performance or repair-time measurement.
