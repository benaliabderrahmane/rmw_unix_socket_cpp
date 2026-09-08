# Changelog

All notable changes to `rmw_unix_socket_cpp` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **QoS status events are refused honestly.** `rmw_event_set_callback` returned
  a failure with no error message behind it — the `": error not set"` half of
  the original `EventsExecutor` crash report. `rmw_take_event`,
  `rmw_event_set_callback`, both `*_event_init` functions and
  `rmw_subscription_set_on_new_message_callback` were also missing the
  `RMW_CHECK_TYPE_IDENTIFIERS_MATCH` every other entry point here already had.

## [0.5.0] - 2026-08-27

The wait/wakeup release. The 200 ms `rmw_wait` poll is gone, replaced by an
event-driven doorbell; TRANSIENT_LOCAL latched replay is rebuilt as a pull from
a per-publisher shared-memory ring; and the per-node graph guard conditions that
`rclcpp`'s `GraphListener` actually waits on are finally triggered.

> **Upgrade together.** See [Upgrade notes](#upgrade-notes-050) — a mixed-build
> fleet can lose latched replay and registry wakeups during the rollout window.

### Added

- **Pull-based TRANSIENT_LOCAL replay** (#60). A latched publisher writes each
  sample into a per-publisher shm ring (`tl_ring`, `qos.depth` per-record-seqlocked
  slots) at publish time; a late-joining subscription reads that ring itself
  inside `rmw_create_subscription`. The idle publisher is never woken, polled, or
  rung. History is enqueued before the subscription handle returns, so it always
  precedes live samples. Payloads over `TL_EMBED_CAP` (1 KiB) ride the existing
  durable segments as 32-byte descriptors; ring bytes are capped at
  `TL_RING_MAX_BYTES` (2 MiB).
- **`rmw_subscription_options_t::ignore_local_publications`** (#49). Previously
  discarded at creation. Each context now draws a random 64-bit `context_id` in
  `rmw_init` and embeds it in the trailing 8 bytes of every GID, so
  `is_same_context()` answers from bytes already on the wire — no extra traffic,
  no wire-format change. "Local" is the same `rmw_context_t`, per the rmw
  contract, not merely the same process.
- **Event-driven registry wakeup (doorbell).** Each context binds a doorbell
  socket at `rmw_init`; every registry mutation sends one octet to every
  registered doorbell strictly *after* bumping the generation, and `rmw_wait`
  drains its doorbell strictly *before* reading the generation. That ordering
  pair makes a lost wakeup impossible. No new threads, no registry layout change.
- **Graph-change triggering of per-node guard conditions** (#43).
- CI: **Lyrical** added to the build & test matrix (#17); CI now also runs on
  `devel` pushes and PRs.
- GitHub issue forms and a pull request template (#56).
- Tests: `test_rmw_wait.cpp` (new, +648 lines), `test_shm_transport.cpp` (+283),
  substantial additions to `test_rmw_qos.cpp` (+873), `test_rmw_graph.cpp`,
  `test_transport.cpp`, `test_rmw_pub_sub.cpp`, and
  `test_rmw_service_client.cpp`.

### Changed

- **VOLATILE late joiners no longer receive latched history.** Pulled records
  are filtered by durability — DDS-correct behaviour. The old push path replayed
  to every subscriber on the topic regardless of its durability QoS.
- The `ENTRY_DOORBELL` slot is now registered **lazily**, on the first
  `rmw_wait` holding a graph guard condition, so only graph-event consumers
  (`wait_for_service`, `GraphListener`, rosbag2) pay for registry wakeups.
  Doorbell-slot mutations no longer ring, which removes a K²/2 registration
  mini-storm.
- Removed with the push machinery: `transient_local_pubs` (and its mutex),
  `known_subscriber_paths`, `CachedMessage` / the heap message cache,
  `transient_local_publish`'s replay loop, and the wait-side TL replay block.
- `registry`: `teardown_slot` now `shm_unlink`s `tl_`-prefixed slot paths, and
  the orphan sweep gained a `ros2_uds_tl_` prefix pass.
- `DESIGN.md` documents the doorbell wakeup, the graph-event wiring, GID
  composition and context identity, and the TL ring.

### Fixed

- **`rmw_wait` timeout contract.** The 200 ms poll bound made every wait return
  `RMW_RET_TIMEOUT` at 200 ms regardless of the caller's deadline — a 600 ms wait
  returned at 200 ms, and an infinite wait, which must never time out, returned
  `TIMEOUT`. Idle processes also woke 5×/s. `RMW_RET_TIMEOUT` now surfaces only
  at the caller's own deadline.
- **`rmw_wait` reported spurious timeouts** when a drain legitimately yielded
  nothing (an `ignore_local_publications` drop, or a shm descriptor whose sender
  is gone).
- **A wait set holding only guard conditions got a null context** and skipped
  the registry check entirely — the shape a publish-only node's executor
  produces. The wait set now stores its context at `rmw_create_wait_set`.
- **`rmw_wait` dispatched fds the current call never armed.** The armed-fd cache
  survived across calls and was insert-only, so a guard condition owned by
  another wait set could have its eventfd consumed with the trigger recorded
  nowhere — a permanent lost wakeup that hung `wait_for_service` and
  `GraphListener`. Failed `epoll_ctl` ADDs now degrade to polling instead of
  being silently dropped.
- **Dead graph-guard trigger.** `rmw_wait` fired `ctx->graph_guard_condition`, a
  context-level field that is declared and never assigned, so the branch was
  dead code and the object `rcl` waits on was never triggered (#43).
- **Zero-length datagrams were never dequeued.** `recv_from` peeks to size its
  buffer; a zero-length datagram made the peek return 0 and the function bailed
  out. `MSG_PEEK` does not consume, so the socket stayed readable forever,
  spinning every wait that polled that fd.
- **Junk datagrams are now consumed atomically.** The zero-length cleanup used a
  blind 1-byte `recv` after a separate, non-atomic peek; under a
  `MultiThreadedExecutor` a racing drain could consume the peeked datagram and
  the 1-byte `recv` would dequeue a real message and silently discard it.
- **Truncated consumes are detected with `MSG_TRUNC` and dropped.** Pre-existing:
  the consuming `recv` omitted `MSG_TRUNC`, so a concurrently-dequeued peek
  followed by a larger datagram was silently truncated to the smaller buffer and
  delivered as valid.
- **TL: a writer filling a slot the pull scan skipped** opened a sequence gap
  with `overlapped` left false, defeating the contiguous-prefix trim.
- **TL: the replay watermark now stops at an unresolvable record.** `max_seq` was
  fixed before the enqueue loop, so a record whose descriptor could no longer be
  resolved was skipped while its sequence stayed under the watermark — the
  sample was lost, not lapped.
- `rmw_init`'s stale sweep is now stamped, and the throttle-scope comments were
  corrected.
- Docs: `TL_RING_MAX_BYTES` was documented as 1 MiB (it is 2 MiB), and the
  durable-segment budgeting note understated tmpfs sizing for over-cap latched
  samples (a retained 5 MiB sample costs 5 MiB, not "an inode and a couple of
  pages"). Reported by Copilot on #60.

### Performance

- **`rmw_wait` drains only the fds `epoll` reported ready** (#57). It previously
  made three passes over every entity per call — drain every socket, re-arm every
  fd, drain every socket again — roughly 2150 syscalls for a 719-entity wait set,
  nearly all returning `EAGAIN` or `EEXIST`.
- **The stale-slot sweep is throttled off the graph query path** (#58).
  `query_all` ran `registry_cleanup_stale` before every graph query across 13
  call sites, so `rmw_get_node_names`, `rmw_count_publishers`,
  `rmw_count_subscribers`, `rmw_get_topic_names_and_types` and the rest each
  walked every live slot, copied 1164 bytes out of each, and `stat`ed
  `/proc/<pid>`.
- **Launch arithmetic at N=200 nodes:** doorbell datagrams drop from ~100,300
  (19,900 of them from doorbell self-registration alone) to roughly the number of
  graph-event consumers; wake-side registry copies go from
  O(mutations × processes × slots) to zero for plain pub/sub processes.
- Latched-replay latency improves from ~20 ms (doorbell wake) to synchronous at
  subscription creation.

### Upgrade notes (0.5.0)

- **Latched replay across a mixed-build pair is lost.** A 0.5.0 publisher no
  longer push-replays and a pre-0.5.0 subscriber never pulls, so latched replay
  between that pair does not work during a rolling upgrade — upgrade together
  (same precedent as the shm payload flag). Old publisher + new subscriber keeps
  working: an empty `socket_path` skips the pull and the old push path still
  delivers.
- **Registry wakeups across a mixed-build fleet can be missed.** A build that
  predates the doorbell bumps the generation but never rings.
- **Containers need `--shm-size` ≥ 1 GiB** to run the full test suite. The 64 MB
  Docker default cannot hold the ~38 MB registry plus the payload rings.

## [0.4.1] - 2026-07-23

### Reverted

- `fix(rmw_wait): deliver TRANSIENT_LOCAL latched samples to late joiners of
  idle publishers` (#37). The push-based approach woke the publisher process to
  resend, and the doorbell broadcast it relied on did not scale — every registry
  mutation rang every process, and every wake rescanned the registry, which
  melted 200-node launches. Replaced in 0.5.0 by the pull-based ring (#60).

## [0.4.0] - 2026-07-16

### Added

- **Shared-memory ring transport for large topic payloads**, with CDR serialized
  directly into the ring record.
- **Durable shm segments for large TRANSIENT_LOCAL messages**, including the
  serialized-publish path.
- **Large service request/response payloads routed through shm.**
- `fork()`-based cross-process shm integration tests.

### Changed

- The inline-vs-shm send/receive decision unified into two helpers.
- `DESIGN.md` expanded into a fuller architecture document (#16); doc/comment
  drift from the merge-readiness audit fixed.
- Repository prepared for open-source release (#15).

### Fixed

- Contained the inline-path resize; the ring inline fallback is now tested.
- Honest TRANSIENT_LOCAL publish return value.

### Performance

- Shorter `cache_mutex` hold, smaller ring floor, POD-keyed reader cache.

## [0.3.0] - 2026-06-17

Audit hardening & registry/fan-out performance.

### Added

- TRANSIENT_LOCAL replay to late-joining subscribers (#2).
- README.

### Fixed

- Contained `fastcdr`/`std` exceptions in `serialize()` so none cross the
  `extern "C"` boundary (#4).
- Publish a registry slot's state only after its payload commits under the
  seqlock (#5).
- Deep-copy `security_options` in `rmw_init_options_copy` to prevent a
  double-free (#6).
- Accumulate the `rmw_wait` timeout in `int64` so `RMW_DURATION_INFINITE` keeps
  blocking (#8).
- Write `rmw_take_sequence` output at the taken cursor (#7).
- Surface `EMSGSIZE` as `RMW_RET_ERROR` on publish (#3).
- Assorted P2/P3 hardening and identifier checks, plus dead-code removal (#9).
- Probe `localhost_only` via include dirs rather than linked targets, so Rolling
  configure stops aborting.

### Performance

- Bound query/cleanup scans with a shared high-water mark (#14).
- Filter registry slots by type before the per-slot snapshot copy (#11).
- Copy-on-write subscriber-path cache; prune the stale known-subscriber set on
  graph change (#12).
- Cache `rmw_service_server_is_available` on the registry generation (#13).

## [0.2.0] - 2026-05-11

Observability.

### Changed

- Failure paths routed through `rcutils` logging.
- `ament_target_dependencies` replaced with direct `target_link_libraries`.
- `ament_lint_auto` dropped from the test suite.

### Fixed

- CI: install `test_msgs` explicitly so Rolling stops silently skipping it; tell
  `rosdep` to install `test_depend` packages; raise `/dev/shm` in the container
  to 1 GiB.

## [0.1.0] - 2026-05-11

Initial release: a ROS 2 RMW implementation over `AF_UNIX` datagram sockets with
a lock-free shared-memory registry.

### Added

- Lock-free registry via per-slot seqlock + atomic state.
- Cached discovery lookups and scale benchmarks.
- Build & test workflows for Rolling, Jazzy, and Kilted.

### Fixed

- Guard `rmw_init_options_t::localhost_only` behind a CMake probe.

[0.5.0]: https://github.com/benaliabderrahmane/rmw_unix_socket_cpp/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/benaliabderrahmane/rmw_unix_socket_cpp/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/benaliabderrahmane/rmw_unix_socket_cpp/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/benaliabderrahmane/rmw_unix_socket_cpp/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/benaliabderrahmane/rmw_unix_socket_cpp/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/benaliabderrahmane/rmw_unix_socket_cpp/releases/tag/v0.1.0
