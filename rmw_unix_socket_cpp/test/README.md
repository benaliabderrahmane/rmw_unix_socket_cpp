# rmw_unix_socket_cpp — Test Suite

This document describes every test in the suite, what it validates, and why each test exists.

---

## Test Architecture

Tests are organized in two tiers:

1. **Unit tests** — test internal components (registry, serialization, transport) in isolation by compiling source files directly into the test binary. These bypass the shared library's hidden visibility.
2. **RMW API tests** — test the public `rmw_*` C functions through the shared library, validating the full stack from API call to socket I/O.

All tests use Google Test (gtest) via `ament_add_gtest`. Each test uses domain ID 98/99 to avoid collisions with a running ROS system.

---

## Unit Tests

### test_registry.cpp

Tests the POSIX shared memory discovery registry (`/dev/shm/ros2_uds_<domain_id>`).

| Test | What it validates | Why it matters |
|------|-------------------|----------------|
| `OpenCreatesValidHeader` | `registry_open` initializes version and max_entries correctly | A corrupted header would break all discovery. This confirms the mmap'd region is properly initialized on first creation. |
| `AddAndRemoveEntry` | Add a node entry, query it back, remove it, confirm it's gone | Core CRUD. If add/remove is broken, nodes/publishers/subscribers can't register or unregister, causing ghost entries or missing discovery. |
| `GenerationIncrementsOnChange` | Generation counter increases on add and again on remove | The generation counter drives graph guard condition triggers. If it doesn't increment, `ros2 node list` and `ros2 topic list` never update. |
| `QueryFiltersByType` | Adding a PUBLISHER and SUBSCRIPTION on the same topic, then querying by type returns only the requested type | Prevents `rmw_count_publishers` from accidentally counting subscribers, or `rmw_publish` from sending to other publishers instead of subscribers. |
| `MultipleEntriesSameType` | Add 50 node entries, query returns all 50 | Validates the registry scales beyond trivial counts. Catches off-by-one errors in slot scanning and ensures the linear search over 8192 slots works. |

### test_serialization.cpp

Tests the introspection-based binary serializer that converts ROS messages to/from byte buffers.

| Test | What it validates | Why it matters |
|------|-------------------|----------------|
| `BasicTypes` | Round-trip bool, byte, char, float32/64, int8/16/32/64, uint8/16/32/64 | Every primitive type must survive serialization. A single misaligned offset or wrong size would corrupt all downstream messages. |
| `Strings` | Round-trip `std::string` fields | Strings are length-prefixed (uint32 + bytes). Incorrect length encoding would truncate or over-read, causing data corruption or crashes. |
| `EmptyStrings` | Round-trip empty `""` strings | Edge case: zero-length strings must serialize as `[0x00000000]` (4-byte zero), not be skipped. Many serializers break on empty strings. |
| `NestedMessage` | Round-trip a message containing another message (`Nested` contains `BasicTypes`) | Tests recursive serialization. If the sub-message offset or member traversal is wrong, nested fields get misaligned. |
| `UnboundedSequences` | Round-trip `std::vector<int32_t>`, `std::vector<std::string>`, `std::vector<bool>`, `std::vector<double>` | Dynamic sequences use `size_function`/`get_const_function`/`resize_function` from introspection. `std::vector<bool>` is a bitfield requiring special handling (`fetch_function`/`assign_function` instead of raw pointers). This test caught a segfault from null `get_function` on bool vectors. |
| `EmptySequences` | Round-trip a message where all sequences are empty (size=0) | Ensures `resize_function(ptr, 0)` and zero-count serialization don't crash. Default-constructed messages have empty vectors. |
| `Arrays` | Round-trip fixed-size arrays of bools, ints, doubles, strings | Fixed arrays use direct offset math (not function pointers). Incorrect `array_size_` handling would read/write wrong memory. |
| `LargeString` | Round-trip a 10,000-character string | Validates the serializer handles payloads larger than typical socket buffers. Catches buffer overflow bugs in `append_bytes`. |

### test_transport.cpp

Tests the Unix domain socket send/receive layer.

| Test | What it validates | Why it matters |
|------|-------------------|----------------|
| `CreateBoundSocket` | `create_bound_socket` returns a valid fd | If socket creation fails (permissions, path too long, missing directory), no subscriber can receive messages. |
| `CreateSendSocket` | `create_send_socket` returns a valid unbound fd | The send socket is shared by all publishers in a context. If it fails, nothing can publish. |
| `SendAndReceive` | Send a WireHeader + payload, receive and verify all fields match | End-to-end datagram integrity. Validates `sendmsg`/`recvmsg` with `iovec`, WireHeader packing, and payload extraction. |
| `RecvFromEmptyReturnsF` | `recv_from` on a socket with no pending data returns false | Non-blocking recv must not block or crash. This is called in tight loops during `rmw_wait` draining. |
| `MultipleMessages` | Send 10 messages, receive all 10 in order with correct sequence numbers | Validates FIFO ordering (guaranteed by Unix DGRAM sockets) and that no messages are lost or reordered in rapid succession. |

---

## RMW API Tests

### test_rmw_init.cpp

Tests context initialization and shutdown lifecycle.

| Test | What it validates | Why it matters |
|------|-------------------|----------------|
| `GetIdentifier` | `rmw_get_implementation_identifier()` returns `"rmw_unix_socket_cpp"` | The identifier string is used by `RMW_CHECK_TYPE_IDENTIFIERS_MATCH` in every API call. Wrong identifier = every call rejected. |
| `GetSerializationFormat` | `rmw_get_serialization_format()` returns `"uds_introspection"` | Used by rosbag2 and tools to identify the wire format. Must be non-null and stable. |
| `InitOptionsLifecycle` | Init, copy, fini of `rmw_init_options_t` | Options carry domain_id, security, allocator. A leak in copy/fini would accumulate over process lifetime. |
| `InitShutdownLifecycle` | `rmw_init` → `rmw_shutdown` → `rmw_context_fini` | Full lifecycle. Validates shared memory registry is opened, send socket is created, and everything is cleaned up. Leaked fds or shm would accumulate across init/fini cycles. |
| `NullArgumentsRejected` | Null pointers return `RMW_RET_INVALID_ARGUMENT` | Prevents segfaults from user error. Every public API must validate inputs. |

### test_rmw_node.cpp

Tests node creation and destruction.

| Test | What it validates | Why it matters |
|------|-------------------|----------------|
| `CreateDestroyNode` | Node has correct name, namespace, identifier | Node metadata is used by `ros2 node list` and graph introspection. Wrong values = broken tooling. |
| `MultipleNodes` | Create 10 nodes, destroy all | Validates registry supports multiple nodes and cleanup doesn't corrupt other entries. |
| `GraphGuardCondition` | `rmw_node_get_graph_guard_condition` returns a valid guard condition | The graph guard condition wakes up `rmw_wait` when the graph changes. Without it, `ros2 topic list` would never update. |
| `NullNodeArgs` | Null context/name/namespace rejected | Prevents crashes from invalid arguments. |

### test_rmw_guard_condition.cpp

Tests eventfd-based guard conditions.

| Test | What it validates | Why it matters |
|------|-------------------|----------------|
| `CreateDestroyGuardCondition` | Valid eventfd is created, identifier matches | Guard conditions are the signaling backbone of `rmw_wait`. A failed eventfd = executor hangs forever. |
| `TriggerGuardCondition` | Trigger writes to eventfd, read confirms non-zero value | Validates the trigger→wakeup path. Used by graph changes, timers, and shutdown signals. |
| `NullGuardConditionArgs` | Null pointers rejected | Prevents crashes. |

### test_rmw_pub_sub.cpp

Tests the complete publish/subscribe data path.

| Test | What it validates | Why it matters |
|------|-------------------|----------------|
| `CreateDestroyPublisher` | Publisher has correct topic, identifier, `can_loan_messages=false` | Publisher metadata used by graph introspection and QoS negotiation. |
| `CreateDestroySubscription` | Subscription has correct topic, identifier | Subscription creates a bound socket and registry entry. Failure here = no messages received. |
| `PublishAndTake` | Publish BasicTypes → take → verify all fields match | The fundamental data path: serialize → sendmsg → recvmsg → deserialize. If this fails, nothing works. |
| `TakeWithInfo` | Take with message_info, verify timestamps and sequence numbers | Message metadata (source_timestamp, reception_sequence_number, publisher_gid) is used by QoS monitoring, rosbag2, and application logic. |
| `TakeEmptyReturnsFalse` | Take on empty subscription sets `taken=false` | Executor relies on this to know when to stop taking. A wrong return would cause infinite loops. |
| `MultipleMessages` | Publish 5, take 5, verify values and order | Validates FIFO ordering and that the message queue doesn't drop or reorder. |
| `PublisherCountMatchedSubscriptions` | 0 before sub, 1 after sub | Used by `ros2 topic info` and application-level "wait for subscriber" patterns. |
| `PublisherGetGid` | GID is non-zero and has correct identifier | GIDs identify publishers across the system. Zero GIDs would break `rmw_compare_gids_equal`. |
| `StringMessages` | Publish/take `test_msgs::msg::Strings` with a string payload | Tests serialization of variable-length types through the full pub/sub stack (not just the serializer unit test). |

### test_rmw_service_client.cpp

Tests service request/response communication.

| Test | What it validates | Why it matters |
|------|-------------------|----------------|
| `CreateDestroyService` | Service has correct name, identifier | Service server creates a socket and registry entry. |
| `CreateDestroyClient` | Client has correct name, identifier | Client creates a socket for receiving responses. |
| `ServiceServerIsAvailable` | False before service exists, true after | Used by `ros2 service call --wait` and rclcpp's `wait_for_service`. Without this, clients can't discover servers. |
| `RequestResponseRoundTrip` | Client sends request → service takes it → service sends response → client takes it, all fields verified | Full RPC round-trip: serialize request → send to service socket → deserialize → serialize response → look up client socket by GID → send → deserialize. Tests the GID-based response routing. |

### test_rmw_wait.cpp

Tests the epoll-based wait set.

| Test | What it validates | Why it matters |
|------|-------------------|----------------|
| `CreateDestroyWaitSet` | Valid epoll fd created, identifier matches | The wait set is the core of the executor. Every node has at least one. |
| `WaitWithGuardCondition` | Pre-triggered guard condition causes immediate return with `RMW_RET_OK` | Validates the "already ready" fast path — avoids unnecessary epoll_wait when data is already available. |
| `WaitTimeoutWhenNoData` | Returns `RMW_RET_TIMEOUT` after specified duration, guard condition nulled out | Validates the timeout path. Without this, the executor would hang indefinitely when no messages arrive. |
| `WaitWithSubscription` | Publish → wait on subscription → returns ready → take succeeds | End-to-end: message arrives on socket → epoll detects it → drain into queue → subscription marked ready → take succeeds. Tests the full executor wakeup path. |

### test_rmw_graph.cpp

Tests graph introspection (discovery queries).

| Test | What it validates | Why it matters |
|------|-------------------|----------------|
| `GetNodeNames` | Our test node appears in node list with correct namespace | Powers `ros2 node list`. Reads from shared memory registry. |
| `CountPublishersAndSubscribers` | Counts go 0→1 on create, 1→0 on destroy | Powers `ros2 topic info`. Validates registry add/remove correctly updates counts. |
| `GetTopicNamesAndTypes` | Created publisher's topic appears with its type | Powers `ros2 topic list -t`. Validates type name encoding from introspection members. |
| `CompareGidsEqual` | Same GID data = true, different = false | Used internally to match service responses to clients by GID. |

### test_rmw_qos.cpp

Tests QoS policies, TRANSIENT_LOCAL (latched), and multi-endpoint scenarios.

| Test | What it validates | Why it matters |
|------|-------------------|----------------|
| `TransientLocalLateJoiner` | Publish 3 messages before subscriber exists → create subscriber → publish 1 more → subscriber receives all 4 | **The core latched topic test.** TRANSIENT_LOCAL publishers cache messages. When a new subscriber is detected (on next publish), the cache is replayed. Without this, `ros2 topic echo --qos-durability transient_local` on a latched topic would miss the initial value. |
| `TransientLocalCacheDepthEnforced` | Publish 5 into depth=3 cache → late joiner gets only the 3 most recent + current | Prevents unbounded memory growth. The cache evicts oldest messages when full. |
| `VolatileNoCache` | Publish before subscriber with VOLATILE → late joiner gets nothing | Confirms VOLATILE publishers don't cache. A bug here would waste memory on every publisher. |
| `QueueDepthEnforced` | Publish 5 into depth=3 subscription queue → take returns only last 3 | Subscription message queues must respect KEEP_LAST depth. Without enforcement, a slow subscriber would consume unbounded memory. |
| `QosCompatibilityCheck` | RELIABLE+RELIABLE=OK, BEST_EFFORT+RELIABLE=WARNING, VOLATILE+TRANSIENT_LOCAL=WARNING, TRANSIENT_LOCAL+TRANSIENT_LOCAL=OK | `rmw_qos_profile_check_compatible` is called by rclcpp to warn users about mismatched QoS. Wrong results = silent data loss or misleading warnings. |
| `MultipleSubscribersOnePublisher` | 1 publisher, 3 subscribers — all 3 receive the message | Validates the publisher fanout loop sends to every subscriber socket in the registry. A bug here would cause some subscribers to miss messages. |
| `MultipleClientsOneService` | 2 clients, 1 service — both clients see the service as available | Validates service discovery works with multiple clients. |

---

## What is NOT tested (and why)

| Feature | Reason |
|---------|--------|
| Loaned messages | Not implemented — Unix sockets copy through kernel, zero-copy requires shared memory. Returns `RMW_RET_UNSUPPORTED`. |
| Dynamic messages | Not implemented — requires middleware type discovery. Returns `RMW_RET_UNSUPPORTED`. |
| Network flow endpoints | Not applicable — AF_UNIX has no IP endpoints. Returns `RMW_RET_UNSUPPORTED`. |
| Content filtering | Not implemented. Returns `RMW_RET_UNSUPPORTED`. |
| Cross-process tests | All tests run in a single process. Cross-process communication works via the same shared-memory registry and socket paths, but testing it requires launching separate processes (integration test territory). |
| Deadline / lifespan QoS | Not enforced — these are timer-based policies that require background monitoring. Accepted as a known limitation for this lightweight implementation. |
| Stress / scale tests (200+ nodes) | Requires a launch file and process management. Can be tested with the `test200.launch.xml` launch file separately. |

---

## Running the tests

```bash
# Build with tests
source /opt/ros/jazzy/setup.bash
colcon build --packages-select rmw_unix_socket_cpp --cmake-args -DBUILD_TESTING=ON

# Run only gtest (skip linters)
source install/setup.bash
cd build/rmw_unix_socket_cpp
ctest -L gtest --output-on-failure

# Run a specific test
./test_rmw_qos --gtest_filter="*TransientLocal*"

# Run everything including linters
ctest --output-on-failure
```
