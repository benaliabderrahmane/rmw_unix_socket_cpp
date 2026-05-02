# rmw_unix_socket_cpp - Design Document

A lightweight, localhost-only RMW (ROS Middleware) implementation for ROS 2 using Unix domain sockets and POSIX shared memory. Designed for single-machine deployments with 200+ nodes and minimal resource usage.

Reference: [ROS 2 Creating an RMW Implementation](https://docs.ros.org/en/rolling/Tutorials/Advanced/Creating-An-RMW-Implementation.html)

---

## Architecture Overview

```
+-----------------------------------------------------+
|  ROS 2 Client Library (rcl / rclcpp)                 |
+-----------------------------------------------------+
|  rmw_unix_socket_cpp                                 |
|  +------------+ +------------+ +-------------------+ |
|  | Registry   | | Transport  | | Serialization     | |
|  | (shm)      | | (UDS)      | | (introspection)   | |
|  +------------+ +------------+ +-------------------+ |
+-----------------------------------------------------+
|  Linux Kernel (AF_UNIX, epoll, eventfd, shm, mmap)   |
+-----------------------------------------------------+
```

---

## Design Choices and Rationale

### 1. Transport: `SOCK_DGRAM` Unix Domain Sockets

**Choice**: All data (topics, service requests, service responses) flows through `SOCK_DGRAM` (datagram) Unix domain sockets.

**Why not `SOCK_STREAM`?**
- `SOCK_STREAM` requires connection management (`accept`, `connect`, per-connection fds). For pub/sub with N subscribers this means N connections per publisher, plus framing logic to delimit messages in the byte stream.
- `SOCK_DGRAM` preserves message boundaries natively -- each `recvmsg()` returns exactly one complete message. This eliminates all framing code.
- `SOCK_DGRAM` is connectionless -- the publisher simply sends to each subscriber's socket path. No connection setup, no teardown, no state to manage.

**Why not shared memory for data?**
- Shared memory (e.g., iceoryx) gives zero-copy performance but adds significant complexity: memory pools, slot management, reference counting, flow control.
- Unix sockets are a single `sendmsg()` / `recvmsg()` call and the kernel handles all buffering, flow control, and backpressure.
- For localhost communication, Unix sockets are already highly optimized in the Linux kernel (the data never touches a network interface).

**Trade-off**: Message size is limited by the socket buffer (default ~212 KB, configured to 48 MB). For truly large messages (point clouds, images), this may require tuning `rmem_max` / `wmem_max` sysctl. This was accepted as the right trade-off for simplicity.

### 2. Discovery: POSIX Shared Memory Registry

**Choice**: A single shared memory file (`/dev/shm/ros2_uds_<domain_id>`) acts as the discovery registry. Every node, publisher, subscriber, service, and client registers itself here with its socket path.

**Why not a daemon process (like ROS 1's rosmaster)?**
- A daemon adds a process to manage (start, stop, crash recovery).
- Shared memory is always available -- no startup ordering issues.
- Direct memory access is faster than IPC to a daemon for every lookup.

**Why not multicast/broadcast UDP?**
- This is a localhost-only implementation. Shared memory is the simplest possible IPC for discovery on a single machine.
- No network stack involvement at all.

**Why a fixed-size layout?**
- Fixed 8192 entries (each ~1156 bytes, ~9.2 MB total) avoids the complexity of dynamic resizing (`mremap`, pointer invalidation, etc.).
- 8192 entries comfortably supports 200+ nodes with ~20 endpoints each (4000+ entries) with 2x headroom.
- The memory is mapped `MAP_SHARED` so all processes see the same data.

**Stale entry cleanup**: When a process dies without calling the destroy APIs (SIGKILL, OOM, crash, container restart), its registry entries and `/tmp/ros2_uds/<domain>/` socket files are left behind. Cleanup uses `stat("/proc/<pid>")` — if the PID is not visible in the caller's PID namespace, the entry is either from a dead process or from a process we cannot reach anyway, so reclaiming it is safe. Orphan socket files are cleaned the same way: the PID is encoded in the file name (`<prefix>_<pid>_<unique>.sock`), so the same `/proc` check applies.

Cleanup is triggered at three points:

1. **On `rmw_init()`** for each context, right after the registry is opened. Stale registry slots are cleared and orphan socket files in `/tmp/ros2_uds/<domain>/` are unlinked. This is the primary recovery path after ungraceful shutdowns — every process startup scrubs state left by prior crashes.
2. **On `EOWNERDEAD`** when any process acquires the registry mutex and the previous owner died while holding it (via the robust mutex).
3. **Inside `registry_add()`** when all slots are full: stale entries are reclaimed and the insertion is retried once before returning "registry full".

No background cleanup thread is needed.

### 3. Locking: POSIX Robust Mutex

**Choice**: A `pthread_mutex_t` with `PTHREAD_PROCESS_SHARED` and `PTHREAD_MUTEX_ROBUST` attributes, stored in the shared memory header.

**Why not file locks (`flock`/`fcntl`)?**
- File locks cannot atomically protect shared memory operations.
- Process-shared mutexes integrate naturally with `mmap`'d memory.

**Why robust mutexes?**
- If a process holding the mutex crashes, the next process to lock it receives `EOWNERDEAD` and can call `pthread_mutex_consistent()` to recover.
- Without robust mutexes, a crashed lock-holder would permanently deadlock all other processes.

### 4. Serialization: CDR via `fastcdr` + `rosidl_typesupport_fastrtps`

**Choice**: Use the eProsima `fastcdr` library with `rosidl_typesupport_fastrtps_cpp` generated callbacks for CDR serialization. This is the same approach used by `rmw_zenoh_cpp`.

**How it works**:
- Each ROS message type has generated `cdr_serialize()` / `cdr_deserialize()` functions provided by `rosidl_typesupport_fastrtps_cpp`.
- At publisher/subscription creation, we resolve the `message_type_support_callbacks_t` from the type support handle.
- `rmw_publish` calls `callbacks->cdr_serialize(ros_message, cdr)` — compiled per-message-type code, no runtime field walking.
- `rmw_take` calls `callbacks->cdr_deserialize(cdr, ros_message)` — same compiled code in reverse.

**Why this approach was chosen (and what we tried first)**:

We originally implemented a custom introspection-based serializer that walked message fields at runtime using `rosidl_typesupport_introspection_cpp::MessageMembers`. This failed in production for several reasons:

1. **C vs C++ ABI mismatch**: ROS 2 has two introspection variants — C (`rosidl_typesupport_introspection_c`) and C++ (`rosidl_typesupport_introspection_cpp`). They have different `MessageMember` struct layouts: `resize_function` returns `bool` in C but `void` in C++; string fields are `rosidl_runtime_c__String` (24 bytes) in C but `std::string` (32 bytes) in C++; nested message pointers resolve through different paths. When the C introspection was loaded instead of C++ (or vice versa), the serializer read/wrote wrong memory offsets, causing crashes and data corruption.

2. **`std::vector<bool>` bitfield**: The C++ `std::vector<bool>` is a bitfield, not contiguous memory. The introspection's `get_function` returns `nullptr` for bool sequences. Required special handling via `fetch_function`/`assign_function` with different signatures than other types.

3. **Nested message resolution**: For nested messages (e.g., `BridgeCommFleetStatus` containing a sequence of `BridgeCommVehicleStatus` containing a sequence of `BridgeCommLinkStatus` with strings), the submember resolution through `member.members_->data` required going through the typesupport dispatch layer which could return the wrong introspection variant.

4. **Tested on real production system with 100+ nodes**: The introspection serializer worked correctly for simple messages (BasicTypes, Strings, Nested) but failed on complex messages used in production (BridgeCommFleetStatus, InsStatus, TFMessage). Every fix for one message type introduced regressions on others.

**Why fastcdr solves all of this**:
- Uses **compiled per-message-type code** — no runtime introspection walking, no C/C++ ABI issues.
- Handles all message types correctly: nested sequences, strings, bools, wstrings, bounded sequences.
- Battle-tested: used by `rmw_fastrtps_cpp` (default ROS 2 RMW), `rmw_zenoh_cpp`, and `rmw_connextdds`.
- Only ~130 lines of serialization code (vs ~400 lines for the introspection approach).

**Dependencies added**: `fastcdr`, `rosidl_typesupport_fastrtps_c`, `rosidl_typesupport_fastrtps_cpp`. These are standard ROS 2 packages installed with any ROS 2 distribution.

### 5. Wait Mechanism: `epoll` + `eventfd`

**Choice**: `rmw_wait()` uses `epoll_wait()` to block on subscription socket fds, service/client socket fds, and guard condition `eventfd`s.

**Why `epoll` instead of `poll`/`select`?**
- `epoll` is O(1) with respect to the number of watched fds, while `poll`/`select` are O(n).
- With 200+ nodes and many subscriptions, the fd count can be high. `epoll` handles thousands of fds efficiently.

**Why `eventfd` for guard conditions?**
- `eventfd` is the lightest possible signaling primitive in Linux (a single 8-byte kernel counter).
- It integrates directly with `epoll` -- no pipe pair needed.
- Writing `1` triggers it, reading it consumes the trigger. Perfect for the guard condition pattern.

### 6. No Background Threads

**Choice**: All socket I/O is performed inside `rmw_wait()`. There are no background receiver threads.

**Why?**
- Background threads introduce lock contention on message queues.
- Thread management (creation, shutdown, error handling) adds complexity.
- The ROS 2 executor model already calls `rmw_wait()` in a tight loop. Draining sockets at this point is natural and sufficient.
- This is the same approach used by `rmw_zenoh_cpp`.

**How it works**:
1. At the start of `rmw_wait()`, drain all subscription/service/client sockets into per-entity message queues.
2. Check if any entity already has data ready. If yes, return immediately.
3. If nothing is ready, register all fds with `epoll` and block.
4. After `epoll_wait()` returns, drain sockets again and report which entities are ready.

### 7. QoS Simplification

**Choice**: All QoS policies are simplified:
- `RELIABLE` and `BEST_EFFORT` are treated identically (Unix sockets don't drop messages on localhost).
- `TRANSIENT_LOCAL` is not supported (no late-joining replay).
- `KEEP_LAST` with configurable depth is the only history mode.

**Why?**
- Unix domain sockets provide reliable, in-order delivery by the kernel. There is no network loss to protect against.
- Implementing `TRANSIENT_LOCAL` would require storing published messages persistently, adding significant complexity.
- These simplifications cover the vast majority of ROS 2 use cases.

### 8. Publisher-to-Subscriber Fanout

**Choice**: When a publisher publishes, it queries the registry for all subscribers on that topic and sends a copy to each subscriber's socket.

**Why not a central router?**
- A router process adds latency (extra hop), complexity (another process to manage), and a single point of failure.
- Direct publisher-to-subscriber communication is simpler and has lower latency.
- The registry lookup is cheap (shared memory scan) and only needs to find matching socket paths.

**Trade-off**: Each subscriber gets its own copy of the message. With N subscribers, the publisher makes N `sendmsg()` calls. For high-fanout topics (>50 subscribers to the same topic), this could become a bottleneck. Acceptable for the 200-node target.

### 9. Service Implementation

**Choice**: Services use the same `SOCK_DGRAM` transport as topics. The client sends a request to the service server's socket. The service server sends the response back to the client's socket (looked up from the registry by GID).

**Why DGRAM instead of STREAM for services?**
- Keeps the implementation uniform -- one transport mechanism for everything.
- Avoids connection management complexity.
- Service requests/responses are typically small and fit easily in a single datagram.

### 10. Graph Guard Condition

**Choice**: The shared memory registry has a `generation` counter that increments on every add/remove. Each `rmw_wait()` call checks this counter. If it changed, the node's graph guard condition is triggered.

**Why poll instead of push?**
- Push notification across processes without a daemon requires complex IPC (signals, pipes per-process, etc.).
- Polling the generation counter in `rmw_wait()` is a single atomic load from shared memory.
- Since `rmw_wait()` is called very frequently by the ROS 2 executor, graph changes are detected within milliseconds.

### 11. GID Generation

**Choice**: GIDs are 16 bytes: `[pid (4 bytes)][atomic counter (4 bytes)][zeros (8 bytes)]`.

**Why this scheme?**
- Guaranteed unique on a single machine (different PIDs for different processes, monotonic counter within a process).
- No UUID library dependency.
- 16 bytes matches `RMW_GID_STORAGE_SIZE` required by the RMW API.

### 12. Build System

**Choice**: `ament_cmake` with `configure_rmw_library()` and `register_rmw_implementation()`.

**Dependencies**: Only standard ROS 2 packages + Linux kernel APIs:
- `rmw` (abstract interface)
- `rcutils`, `rcpputils` (utilities)
- `rosidl_typesupport_introspection_c/cpp` (message introspection)
- `pthread`, `rt` (POSIX threading, shared memory)

**Why no external middleware?**
- The entire point is to avoid heavy middleware like DDS.
- All functionality is implemented using Linux kernel primitives that are always available.

---

## File Structure

| File | Purpose |
|------|---------|
| `identifier.hpp` | Implementation identifier string |
| `types.hpp` | All internal C++ data types |
| `registry.hpp/cpp` | Shared memory discovery registry |
| `serialization.hpp/cpp` | Introspection-based message serializer |
| `transport.hpp/cpp` | Unix socket send/receive helpers |
| `rmw_init.cpp` | Context initialization and shutdown |
| `rmw_node.cpp` | Node create/destroy |
| `rmw_publisher.cpp` | Publisher create/destroy/publish |
| `rmw_subscription.cpp` | Subscription create/destroy/take |
| `rmw_service.cpp` | Service server |
| `rmw_client.cpp` | Service client |
| `rmw_guard_condition.cpp` | Guard conditions (eventfd) |
| `rmw_wait.cpp` | Wait set (epoll) |
| `rmw_graph.cpp` | Graph introspection queries |
| `rmw_event.cpp` | Event stubs |
| `rmw_serialize.cpp` | Public serialize/deserialize API |
| `rmw_features.cpp` | Feature queries, identifier, QoS |
| `rmw_unsupported.cpp` | Unsupported feature stubs |

---

## Resource Usage

| Resource | Usage |
|----------|-------|
| Shared memory | ~9.2 MB (fixed, one per domain) |
| Socket per subscriber | 1 fd + 1 file in `/tmp/ros2_uds/` |
| Socket per service/client | 1 fd + 1 file each |
| Send socket per context | 1 fd (shared by all publishers) |
| epoll fd per wait set | 1 fd |
| eventfd per guard condition | 1 fd |
| Background threads | **0** |

For 200 nodes with 10 pub + 10 sub each: ~4000 registry entries, ~2000 socket files, ~2000 fds total across all processes.

---

## Usage

```bash
# Build
colcon build --packages-select rmw_unix_socket_cpp

# Use
export RMW_IMPLEMENTATION=rmw_unix_socket_cpp
ros2 run demo_nodes_cpp talker
# In another terminal:
export RMW_IMPLEMENTATION=rmw_unix_socket_cpp
ros2 run demo_nodes_cpp listener
```

---

## Limitations and Unsupported Features

### Transport limitations

| Limitation | Detail | Why |
|---|---|---|
| **Localhost only** | No network communication | This RMW is designed for single-machine deployments. Unix domain sockets (`AF_UNIX`) are local-only by definition. For network communication, use CycloneDDS or Zenoh. |
| **Message size limit** | Default 48 MB per datagram | Constrained by `SOCK_DGRAM` socket buffer. Tunable via `net.core.wmem_max` / `net.core.rmem_max` sysctl. Point clouds and images may exceed this. |
| **Linux only** | Not portable to macOS or Windows | Uses Linux-specific kernel APIs: `epoll` (wait), `eventfd` (guard conditions), `/dev/shm` (shared memory registry), `AF_UNIX` (transport). |
| **Docker requires shared IPC** | Containers must share `/dev/shm` and `/tmp/ros2_uds/` | The shared memory registry lives in `/dev/shm/ros2_uds_<domain_id>` and socket files live in `/tmp/ros2_uds/`. Both must be visible to all containers. Use `--ipc=host` plus `-v /tmp/ros2_uds:/tmp/ros2_uds`. |
| **Docker PID namespace** | Use `--pid=host` when sharing `/tmp/ros2_uds/` across containers | Stale-entry cleanup uses `stat("/proc/<pid>")`. PIDs outside the caller's namespace are invisible, so without `--pid=host` a container's cleanup pass would unlink entries and socket files owned by processes in other containers. With `--pid=host` (required anyway alongside `--ipc=host` and the `/tmp/ros2_uds` bind mount for cross-container communication), PIDs are unified and cleanup is correct. |
| **No DDS interoperability** | Cannot communicate with DDS-based RMW implementations | All nodes on the machine must use `rmw_unix_socket_cpp`. Cannot mix with CycloneDDS, FastDDS, or Connext nodes. |

### Functions that return `RMW_RET_UNSUPPORTED`

These functions are part of the RMW API but cannot be implemented with Unix socket transport. They return `RMW_RET_UNSUPPORTED` per the RMW specification, which tells rcl/rclcpp to skip the feature gracefully.

| Function(s) | Feature | Why unsupported |
|---|---|---|
| `rmw_borrow_loaned_message` | **Zero-copy loaned messages** | Loaned messages require shared memory between publisher and subscriber so both can access the same memory region without copying. Unix sockets copy data through kernel buffers (`sendmsg`/`recvmsg`). Zero-copy requires a shared-memory transport like iceoryx. |
| `rmw_publish_loaned_message` | | Same as above. |
| `rmw_take_loaned_message` | | Same as above. |
| `rmw_take_loaned_message_with_info` | | Same as above. |
| `rmw_return_loaned_message_from_publisher` | | Same as above. |
| `rmw_return_loaned_message_from_subscription` | | Same as above. |
| `rmw_take_dynamic_message` | **Dynamic message types** | Dynamic type support requires the middleware to discover message types at runtime from remote endpoints and populate `rosidl_dynamic_typesupport_dynamic_data_t` structs. This requires a type discovery protocol (like DDS Type Object) which our simple registry doesn't provide. |
| `rmw_take_dynamic_message_with_info` | | Same as above. |
| `rmw_serialization_support_init` | | Same as above — initializes the dynamic type serialization framework. |
| `rmw_publisher_get_network_flow_endpoints` | **Network flow endpoints** | Returns IP address and port information for QoS tracing. `AF_UNIX` sockets have no IP addresses or ports — they use filesystem paths. There is no meaningful network flow endpoint to report. |
| `rmw_subscription_get_network_flow_endpoints` | | Same as above. |
| `rmw_subscription_set_content_filter` | **Content filtering** | Content filtering evaluates a SQL-like expression on each message before delivery to the subscriber. This requires either middleware-level filtering (DDS content-filtered topics) or a filter evaluation engine. Not implemented to keep the RMW simple. All subscribers receive all messages. |
| `rmw_subscription_get_content_filter` | | Same as above. |
| `rmw_event_set_callback` | **Event callbacks** | Setting callbacks on events (QoS violations, matched events). Events are initialized successfully (to prevent crashes when rcl adds them to wait sets) but no events are ever generated. `rmw_event_type_is_supported` returns `false` for all event types. |

### Functions that return `RMW_RET_OK` but are no-ops

These functions succeed silently but don't do anything. This is intentional — the RMW spec allows implementations to accept but not enforce certain features.

| Function(s) | Feature | Why no-op |
|---|---|---|
| `rmw_init_publisher_allocation` / `rmw_fini_publisher_allocation` | **Pre-allocated publishing** | Our serialization allocates dynamically per-publish (via `fastcdr`). Pre-allocation hints are accepted but ignored. |
| `rmw_init_subscription_allocation` / `rmw_fini_subscription_allocation` | **Pre-allocated taking** | Same as above — allocation hints are accepted but ignored. |
| `rmw_publisher_assert_liveliness` | **Manual liveliness assertion** | We don't track liveliness. The function succeeds without doing anything. |
| `rmw_node_assert_liveliness` | **Node liveliness** | Same as above. |
| `rmw_publisher_wait_for_all_acked` | **Wait for acknowledgment** | Unix domain sockets deliver immediately on localhost — there's nothing to wait for. Returns success instantly. |
| `rmw_set_log_severity` | **RMW log level** | We don't have internal logging. The severity is accepted but has no effect. |

### QoS policies: what's enforced and what's not

| QoS Policy | Status | Detail |
|---|---|---|
| `history` = KEEP_LAST | **Enforced** | Subscription message queue depth is capped at QoS `depth`. Oldest messages are dropped when the queue is full. |
| `history` = KEEP_ALL | **Not enforced** | Treated as KEEP_LAST with depth=10. Unbounded queues are not supported. |
| `depth` | **Enforced** | Controls both subscription queue size and TRANSIENT_LOCAL publisher cache size. |
| `durability` = VOLATILE | **Enforced** | Publisher doesn't cache. Late-joining subscribers miss past messages. |
| `durability` = TRANSIENT_LOCAL | **Enforced** | Publisher caches last `depth` messages. Late-joining subscribers receive the cache on first publish after they join. |
| `reliability` = RELIABLE | **Accepted, not differentiated** | Unix sockets are inherently reliable on localhost. No messages are dropped in transit. |
| `reliability` = BEST_EFFORT | **Accepted, not differentiated** | Behaves identically to RELIABLE because Unix sockets don't lose messages locally. |
| `deadline` | **Not enforced** | Accepted but ignored. Would require a timer thread to monitor message delivery intervals. |
| `lifespan` | **Not enforced** | Accepted but ignored. Would require timestamped expiration checks on every message. |
| `liveliness` | **Not enforced** | Accepted but ignored. Would require periodic heartbeat checking. |
| `liveliness_lease_duration` | **Not enforced** | Accepted but ignored. |

### QoS compatibility checking (`rmw_qos_profile_check_compatible`)

The following combinations are flagged as `RMW_QOS_COMPATIBILITY_WARNING`:

| Publisher | Subscriber | Result | Reason |
|---|---|---|---|
| BEST_EFFORT | RELIABLE | Warning | Subscriber expects reliable delivery but publisher offers best-effort. On this RMW they behave identically, but it indicates a likely intent mismatch. |
| VOLATILE | TRANSIENT_LOCAL | Warning | Subscriber expects late-joining replay but publisher doesn't cache. The subscriber will never receive past messages. |

All other combinations return `RMW_QOS_COMPATIBILITY_OK`.
