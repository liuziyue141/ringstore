# Design decisions

## Compute placement locally

Clients and keepers already share static configuration. A lookup service adds another state transfer and availability dependency. Both compute the same ring and walk it directly. One position per endpoint keeps the implementation small; virtual nodes and load-balancing claims are deferred.

## Use identity, not a hash of the value

Two equal appends are distinct mutations. One mutation delivered twice is one mutation. A random operation ID represents that distinction, and logical timestamps plus IDs define a stable order. An ID is reused across every delivery of the ongoing call. Caller-level retries after a crash are separate calls.

## Reconstruct repair instead of checkpointing phases

A keeper-local phase or cursor vanishes on process failure. Surviving backend logs provide the source of truth: merge them and compute missing IDs. Copy work can repeat safely, so leadership is an efficiency choice rather than a correctness requirement. Full log scans cost bandwidth and memory but make recovery inspectable.

## Retain old copies and read them

Restricting reads to the new three targets can hide data while those targets are still empty. Retained holders remain readable until a future safe cleanup protocol exists. This increases fanout and storage consumption; the repository states that cost instead of implying three physical copies at all times.

## Record removals as identities

Physically removing list values permits an old replica to reintroduce them. A removal names the append IDs it observed. Late copies of those IDs remain hidden, while an equal append with a new ID survives. Tombstone cleanup needs evidence that old holders cannot return obsolete histories.

## Keep the clock outside user keys

Backend incarnation and clock-floor metadata are process state exposed through a backend-only service. They do not reserve a user-visible bin/key and do not depend on keeper availability. Fixed numeric slots separate allocator counters. This assumes fixed membership and surviving floor holders; it does not solve partition-safe sequencing.

## Test actual process loss

Cancelling a repair future alone can leave sockets, runtime tasks, or shared memory alive. Integration tests spawn a child executable for each service and kill the process. A copy barrier forces a known partial migration. Dynamic reserved ports and bounded polling isolate tests without relying on lucky fixed sleeps.
