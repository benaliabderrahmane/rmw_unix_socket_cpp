# rmw_unix_socket_cpp - Design Document

A lightweight, localhost-only RMW (ROS Middleware) implementation for ROS 2 using Unix domain sockets and POSIX shared memory. Designed for single-machine deployments with 200+ nodes and minimal resource usage.

This document is self-contained: it covers every design decision, wire format, synchronization primitive, and operational quirk.

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
|  | (shm)      | | (UDS)      | | (fastcdr)         | |
|  +------------+ +------------+ +-------------------+ |
+-----------------------------------------------------+
|  Linux Kernel (AF_UNIX, epoll, eventfd, shm, mmap)   |
+-----------------------------------------------------+
```

Each block is one design choice with rationale below.

---

## ABI conventions

These apply everywhere in `src/` and are non-obvious to readers new to the RMW layer.

- **All public entry points are `extern "C"`.** rcl `dlopen`s the RMW shared library and calls fixed C symbols; C++ name mangling would break linkage.
- **The implementation identifier is compared by pointer, not by `strcmp`.** Every RMW handle carries `implementation_identifier`. We declare it as `inline constexpr char identifier[] = "rmw_unix_socket_cpp";` so it has a single address across all translation units (C++17 guarantee).
- **No exceptions cross the RMW boundary.** Every public function returns `rmw_ret_t` or a pointer. Exceptions from `fastcdr` are caught at the serialization boundary.
- **The `void * data` field on every RMW struct holds our private struct** (`UdsNode`, `UdsPublisher`, ...). Defined in `types.hpp`. Recovered by `static_cast` on every entry.
- **Lock-free registry, mutex-protected per-entity caches.** The shared-memory registry uses a per-slot seqlock + atomic state machine (no global mutex on hot paths). Per-publisher / -client / -service caches use `std::mutex` because they are process-local and rare.

---

## Design Choices and Rationale

### 1. Transport: `SOCK_DGRAM` Unix Domain Sockets

**Choice**: All data (topics, service requests, service responses) flows through `SOCK_DGRAM` (datagram) Unix domain sockets in `/tmp/ros2_uds/<domain_id>/`.

**Why not `SOCK_STREAM`?**
- Stream sockets require connection management (`accept`, `connect`, per-connection fds). For pub/sub with N subscribers this means N connections per publisher, plus framing logic to delimit messages.
- `SOCK_DGRAM` preserves message boundaries natively — each `recvmsg()` returns exactly one complete message.
- `SOCK_DGRAM` is connectionless — the publisher simply sends to each subscriber's socket path. No connection setup, no teardown, no state to manage.

**Why not shared memory for data?**
- Shared memory (e.g., iceoryx) gives zero-copy performance but adds significant complexity: memory pools, slot management, reference counting, flow control.
- Unix sockets are a single `sendmsg()` / `recvmsg()` call and the kernel handles all buffering, flow control, and backpressure.
- For localhost the data never touches a network interface.

**Socket asymmetry**:
- **Subscribers, services, and clients hold bound sockets** in `/tmp/ros2_uds/<domain>/<prefix>_<pid>_<unique>.sock` (prefix = `sub` / `srv` / `cli`). The PID is in the filename so stale-file cleanup can match it against `/proc/<pid>`.
- **Publishers do not have their own socket.** A single unbound `SOCK_DGRAM` send socket per context is shared by every publisher in the process. Possible because each `sendmsg` supplies the destination address inline (`msg.msg_name`). Saves one fd per publisher.

**Socket-path uniqueness**: `/tmp/ros2_uds/<domain>/<prefix>_<pid>_<atomic_counter_high16 | microseconds_low16>.sock`. The PID guards against host-wide collisions; the counter+timestamp guards against same-PID restarts before the kernel reaps the dying process. The full path stays under 100 bytes (Linux `sockaddr_un.sun_path` limit is 108 bytes).

**Wire format per datagram**: a single `sendmsg` writes a scatter-gather pair (`iovec[2]`) that the kernel concatenates into one datagram:

```
+-------------------------------+--------------------+----------------------+
| WireHeader (37 bytes, packed) | CDR encap (4 bytes)| CDR body (variable)  |
+-------------------------------+--------------------+----------------------+
```

`WireHeader` (defined in `types.hpp`, `__attribute__((packed))`):

| Field | Bytes | Purpose |
|---|--:|---|
| `gid[16]` | 16 | Sender GID (publisher / requester / responder). |
| `sequence_number` | 8 | Per-entity monotonic counter. Used to detect drops on the wire. |
| `source_timestamp_ns` | 8 | Send-side `CLOCK_REALTIME` in ns. |
| `payload_size` | 4 | Length of the CDR body that follows. |
| `msg_type` | 1 | `0`=topic, `1`=service request, `2`=service response. |

The 4-byte CDR encapsulation header (`{kind=CDR_LE, options=0x00}`) makes the body byte-compatible with DDS CDR. The reader negotiates endianness off the encap header — mixed-endian setups work even though we only ever target Linux/amd64 today.

**Send flags**: every `sendmsg` uses `MSG_DONTWAIT | MSG_NOSIGNAL`. Non-blocking: if the destination's recv buffer is full we get `EAGAIN` and drop silently (logging at high rates would spam). `MSG_NOSIGNAL` blocks `SIGPIPE` even though it's a belt-and-suspenders for datagram sockets.

**Receive**: a two-step `recv(MSG_PEEK | MSG_TRUNC)` then `recv(MSG_DONTWAIT)`. The first call asks the kernel for the true datagram size without consuming it (`MSG_TRUNC` semantics on `SOCK_DGRAM` are Linux-specific); we resize a `thread_local` 256 KB starter buffer if needed, then consume. This avoids preallocating a max-sized buffer per subscription. The thread-local high-water mark means each executor thread keeps one growing buffer per process lifetime.

**Trade-off**: Per-datagram size is capped by `SO_RCVBUF` / `SO_SNDBUF`. We request 48 MB on every socket via `setsockopt`, but the kernel silently clamps to `net.core.rmem_max` / `net.core.wmem_max`. **Without sysctl tuning these clamp to ~212 KB** — fine for control messages but small images and point clouds will be dropped (`EMSGSIZE` on send). A production deployment can set `rmem_max=wmem_max=48000000` at boot.

**Buffer overflow note**: each Unix datagram socket also has a per-socket queue depth cap from `net.unix.max_dgram_qlen` (default 512 datagrams). On a 150-node fleet, when a new subscriber appears, every publisher in the system can fan out simultaneously and overflow that queue before the new sub gets to drain. Symptom is `EAGAIN` on `sendmsg` with small messages, even though there's no byte-budget pressure. Production deployment bumps this to 4096.

### 2. Discovery: POSIX Shared Memory Registry

**Choice**: A single shared memory file `/dev/shm/ros2_uds_<domain_id>` acts as the discovery registry. Every node, publisher, subscriber, service, and client registers itself here with its socket path.

**Why not a daemon process (like ROS 1's rosmaster)?**
- A daemon adds a process to manage (start, stop, crash recovery).
- Shared memory is always available — no startup ordering issues.
- Direct memory access is faster than IPC to a daemon for every lookup.

**Why not multicast/broadcast UDP?**
- This is a localhost-only implementation. Shared memory is the simplest IPC for discovery on a single machine.
- No network stack involvement at all.

**File layout**:

```
struct RegistryHeader {                // 56 bytes
  std::atomic<uint32_t> version;       // REGISTRY_VERSION = 3
  uint32_t max_entries;                // REGISTRY_MAX_ENTRIES = 32768
  std::atomic<uint64_t> generation;    // bumped on every add/remove — cache invalidation tag
  pthread_mutex_t init_mutex;          // VESTIGIAL — see §3
};

struct RegistryEntrySlot {             // 1168 bytes
  std::atomic<uint32_t> seq;           // even = stable, odd = mid-write (seqlock)
  std::atomic<uint8_t>  state;         // ENTRY_EMPTY / NODE / PUBLISHER / SUBSCRIPTION / SERVICE / CLIENT
  uint8_t  pad[3];
  pid_t    pid;                        // owning process — checked via /proc/<pid> for stale cleanup
  uint8_t  gid[16];
  char     node_name[256];
  char     node_namespace[256];
  char     topic_name[256];            // also reused as service name
  char     type_name[256];
  char     socket_path[108];           // matches sockaddr_un.sun_path
  uint8_t  qos_reliability;
  uint8_t  qos_durability;
  uint8_t  qos_history;
  uint8_t  pad2;
  uint32_t qos_depth;
};
```

Total file size = 56 + 32768 × 1168 ≈ **36.5 MiB**. Mode `0666` so containers running as different UIDs can share it.

**Why a fixed-size layout?**
- 32768 entries × ~1168 bytes avoids the complexity of dynamic resizing (`mremap`, pointer invalidation, etc.).
- Comfortably supports the production target of 150+ nodes with 30+ endpoints each (~5000 entries) with large headroom for fleet growth.
- The original 8192 limit was hit in production at 151 nodes; the registry was bumped to 32768 and `REGISTRY_VERSION` was incremented (v1 → v2 raised the entry count, v2 → v3 dropped the global mutex). Incompatible files are detected by file-size mismatch on `fstat` after `shm_open`, or by version mismatch during the post-attach spin on `header->version`.

**Per-node baseline cost**: an empty `rclcpp::Node` registers **9 entries**: 1 NODE, 1 PUBLISHER (`/parameter_events`), and 7 SERVICEs (the six parameter services plus implicit `~/get_type_description`). At 150 nodes that's ~1350 entries before the first user pub/sub.

**Initialization race**: the first process to `shm_open(O_CREAT | O_EXCL)` becomes the initializer. It `ftruncate`s, zeroes the file, initializes the header, and publishes `header->version = REGISTRY_VERSION` with **release** ordering as the very last step. Late arrivers spin on `version.load(acquire)` for up to ~10000 yields. If we time out with `version == 0`, the creator died mid-init and we best-effort re-init. If `version` is some other non-zero value, the file is stale → `shm_unlink` and retry.

**Stale entry cleanup**: when a process dies without calling the destroy APIs (SIGKILL, OOM, crash, container restart), its registry entries and socket files are left behind. We use `stat("/proc/<pid>")` to test liveness: if the PID is not visible in the caller's PID namespace, the entry is from a dead process (or from a process we cannot reach anyway, so reclaiming is safe). Orphan socket files in `/tmp/ros2_uds/<domain>/` are scrubbed the same way — the PID is encoded in the filename (`<prefix>_<pid>_<unique>.sock`).

Cleanup runs at three points:

1. **On `rmw_init()`** for each context: `registry_cleanup_stale()` + `cleanup_orphan_socket_files()`. This is the primary recovery path after ungraceful shutdowns.
2. **On every discovery query** (`rmw_get_node_names`, `rmw_get_topic_names_and_types`, etc.): each call walks the registry through `query_all()`, which calls `registry_cleanup_stale()` as a side effect. So CLI tools (`ros2 node list`, `ros2 topic list`) are self-healing.
3. **Inside `registry_add()`** when all 32768 slots are full: stale entries are reclaimed and the insertion is retried once before returning "registry full".

No background cleanup thread is needed. **There is no `EOWNERDEAD` path** — that would require a robust mutex on the hot paths, and we don't have one (see §3).

**Container caveat**: cleanup correctness depends on every process seeing the same PID namespace. If container A reclaims slots whose PIDs belong to container B (because B's PIDs are `ENOENT` in A's `/proc`), the registry is corrupted. Production deployments run all containers with `--pid=host` for this reason. `--ipc=host` and a bind mount of `/tmp/ros2_uds/` are also required so all containers see the same `/dev/shm` and socket files.

### 3. Locking: Per-slot seqlock + atomic state machine (lock-free)

**Choice**: Hot paths (add / remove / query / cleanup) use a per-slot seqlock plus an atomic state CAS. **There is no global mutex on the hot path.** The `init_mutex` field in the header is initialized as a `PTHREAD_PROCESS_SHARED | PTHREAD_MUTEX_ROBUST` mutex for historical reasons but is **never locked anywhere in the code** — it is vestigial and kept only because removing it would change the header size and bump `REGISTRY_VERSION` again.

**Why we abandoned the global mutex (v1/v2 → v3)**: at ~150 nodes in production, every `rmw_publish` / `rmw_send_request` / `rmw_send_response` was serializing through a single mutex to look up destination paths. End-to-end latency rose to ~500 ms. The fix had two parts: per-publisher caches keyed off a generation counter (§8a) and a lock-free registry so cache misses also don't take a lock.

**Per-slot state machine**:

```
state == ENTRY_EMPTY
   │
   │  writer: CAS(state, EMPTY -> target_type)
   ▼
state == <type>      // payload protected by seqlock
   │
   │  remover: CAS(state, <type> -> EMPTY)
   ▼
state == ENTRY_EMPTY
```

Only one party can win the CAS in either direction. Losers retry or move on. No locks.

**Per-slot seqlock**: the atomic `state` is enough to claim ownership of a slot, but the payload (pid, gid, node_name, topic_name, socket_path, ...) is non-atomic and bigger than any CAS can cover. We protect it with a seqlock — the classical Linux-kernel `seqcount_t` pattern, applied per slot:

```
writer (after winning state CAS):
  1. seq.fetch_add(1, acq_rel)   ->  seq becomes odd (write in progress)
  2. memcpy payload fields
  3. seq.fetch_add(1, acq_rel)   ->  seq becomes even (snapshot stable)

reader:
  1. s1 = seq.load(acquire); if (s1 & 1) writer in progress -> yield + retry
  2. st = state.load(acquire); if EMPTY -> skip slot
  3. memcpy payload into a local snapshot
  4. s2 = seq.load(acquire); if s1 == s2 -> success, else retry
```

Bounded at 16 retries (`MAX_READ_RETRIES`) so a pathological writer cannot starve readers forever. A reader that gives up just returns "slot was unstable" — the caller treats it as not-found and the next iteration tries again.

**Why this is safe across processes**: the atomics live in a `MAP_SHARED` mmap. Static asserts at compile time check that `std::atomic<u8/u32/u64>::is_always_lock_free` is `true`. Lock-freedom on x86/ARM implies "address-free" — the implementation is a single CAS/load/store instruction with no helper state outside the memory location — which is the requirement for cross-process atomics on the same physical page.

**Why not file locks (`flock` / `fcntl`)?** File locks cannot atomically protect arbitrary shared-memory operations, and they would be per-fd / per-process state outside the mmap.

**Memory ordering summary**:

| Operation | Order |
|---|---|
| Writer claims slot (state CAS) | `acq_rel` success, `relaxed` failure |
| Writer begin/end payload (`seq.fetch_add`) | `acq_rel` |
| Writer ends with `generation.fetch_add` | `acq_rel` |
| Reader pre-check `state.load` | `acquire` |
| Reader begin/end `seq.load` | `acquire` |
| `version` publish on init | `release` |
| `version` attach-wait load | `acquire` |
| `registry_generation` load | `acquire` |

The acquire/release pairs guarantee that a reader who observes `version == REGISTRY_VERSION` also observes the post-init zero-fill of the slot array, and that a reader who observes a `generation` bump observes all preceding slot writes.

### 4. Serialization: CDR via `fastcdr` + `rosidl_typesupport_fastrtps`

**Choice**: Use the eProsima `fastcdr` library with `rosidl_typesupport_fastrtps_cpp` generated callbacks for CDR serialization. Same approach as `rmw_fastrtps_cpp` (the default ROS 2 RMW), `rmw_zenoh_cpp`, and `rmw_connextdds`.

**How it works**:
- Each ROS message type has generated `cdr_serialize()` / `cdr_deserialize()` functions provided by `rosidl_typesupport_fastrtps_cpp`.
- At publisher/subscription creation, we resolve the `message_type_support_callbacks_t` from the type support handle. If the C++ variant isn't present we fall back to the C variant (`rosidl_typesupport_fastrtps_c`).
- `rmw_publish` calls `callbacks->cdr_serialize(ros_message, cdr)` — compiled per-message-type code, no runtime field walking.
- `rmw_take` calls `callbacks->cdr_deserialize(cdr, ros_message)` — same compiled code in reverse.

**Why fastcdr (and what we tried first)**:

We originally implemented a custom introspection-based serializer that walked message fields at runtime using `rosidl_typesupport_introspection_cpp::MessageMembers`. This failed in production for several reasons:

1. **C vs C++ ABI mismatch**: ROS 2 has two introspection variants — C (`rosidl_typesupport_introspection_c`) and C++ (`rosidl_typesupport_introspection_cpp`). They have different `MessageMember` struct layouts: `resize_function` returns `bool` in C but `void` in C++; string fields are `rosidl_runtime_c__String` (24 bytes) in C but `std::string` (32 bytes) in C++; nested message pointers resolve through different paths. When the C introspection was loaded instead of C++ (or vice versa), the serializer read/wrote wrong memory offsets, causing crashes and data corruption.

2. **`std::vector<bool>` bitfield**: The C++ `std::vector<bool>` is a bitfield, not contiguous memory. The introspection's `get_function` returns `nullptr` for bool sequences. Required special handling via `fetch_function`/`assign_function` with different signatures than other types.

3. **Nested message resolution**: For nested messages (e.g., `BridgeCommFleetStatus` containing a sequence of `BridgeCommVehicleStatus` containing a sequence of `BridgeCommLinkStatus` with strings), the submember resolution through `member.members_->data` required going through the typesupport dispatch layer which could return the wrong introspection variant.

4. **Tested on real production system with 100+ nodes**: The introspection serializer worked correctly for simple messages (BasicTypes, Strings, Nested) but failed on complex messages used in production (BridgeCommFleetStatus, InsStatus, TFMessage). Every fix for one message type introduced regressions on others.

**Why fastcdr solves all of this**:
- Uses **compiled per-message-type code** — no runtime introspection walking, no C/C++ ABI issues.
- Handles all message types correctly: nested sequences, strings, bools, wstrings, bounded sequences.
- Battle-tested by every major DDS-based RMW.
- ~130 lines of serialization code vs ~400 lines for the introspection approach.

**Dependencies added**: `fastcdr`, `rosidl_typesupport_fastrtps_c`, `rosidl_typesupport_fastrtps_cpp`. These ship with every ROS 2 distribution.

### 5. Wait Mechanism: `epoll` + `eventfd`

**Choice**: `rmw_wait()` uses `epoll_wait()` to block on subscription socket fds, service/client socket fds, and guard-condition eventfds.

**Why `epoll` instead of `poll`/`select`?**
- `epoll` is O(1) with respect to the number of watched fds; `poll`/`select` are O(n).
- With 200+ nodes and many subscriptions, the fd count can exceed a thousand. `epoll` handles it efficiently.

**Why `eventfd` for guard conditions?**
- `eventfd` is the lightest possible signaling primitive in Linux: a single 8-byte kernel counter.
- It integrates directly with `epoll` — no pipe pair needed.
- Writes increment the counter; reads consume it. Coalescing is automatic: 100 triggers between two reads coalesce into "counter was non-zero".
- Created with `EFD_NONBLOCK` so reads return `EAGAIN` instead of blocking.
- **Intra-process only.** An eventfd has no cross-process meaning. All guard conditions are signaled within a single process; cross-process graph events are polled via the registry's `generation` counter, not pushed via eventfd (see §10).

**`rmw_wait` flow**:

1. **Pre-drain**: drain every subscription / service / client socket into per-entity message queues. Catches data that arrived while the executor was busy.
2. **Ready check**: if any entity has a queued message ready, return immediately without blocking.
3. **Register fds with epoll** (additively, see below) and block in `epoll_wait` for up to the caller's timeout.
4. **Post-drain**: drain everything again after wake-up.
5. **Mark ready entities** in the wait set so rcl can dispatch them.

This drain-three-times pattern (pre, post, and again inside each `rmw_take`) is intentional and defensive: rcl makes no guarantees about when between wait-and-take a callback runs, and we want zero data loss.

**Persistent epoll registration**: an early version called `EPOLL_CTL_ADD` for every fd at the start of every wait, then `EPOLL_CTL_DEL` at the end — two syscalls per fd per wait, pure overhead under a tight executor loop. The current implementation tracks `registered_fds` (a `std::set<int>` in `UdsWaitSet`) and only `EPOLL_CTL_ADD`s fds we haven't seen before. We never `EPOLL_CTL_DEL` — the kernel automatically removes a fd from epoll when it's closed (`man epoll`, Q6).

**Trade-off**: a wait set "remembers" every fd it has ever observed. This is fine because (a) the kernel already cleaned up the fd at `close()` time, (b) the set is bounded by the number of distinct fds the process ever creates, which is bounded by the file-descriptor ulimit. There is a theoretical risk if fd numbers are recycled by a brand-new entity the wait set didn't know about — in practice it has never manifested.

**Timeout granularity**: epoll's timeout is in milliseconds. A caller passing `nsec > 0` is rounded up to 1 ms minimum. Typical 50–200 Hz control loops don't care about sub-ms precision; if they did, we'd need `timerfd` instead.

### 6. No Background Threads

**Choice**: All socket I/O is performed inside `rmw_wait()`. There are no background receiver threads.

**Why?**
- Background threads introduce lock contention on message queues.
- Thread management (creation, shutdown, error handling) adds complexity.
- The ROS 2 executor model already calls `rmw_wait()` in a tight loop. Draining sockets at this point is natural and sufficient.
- Same approach as `rmw_zenoh_cpp`.

This is what makes the resource footprint deterministic — the process never has phantom threads holding fds or memory.

### 7. QoS: what we enforce and what we accept-but-ignore

**Choice**: implement the QoS policies whose semantics actually differ on a localhost-only transport; accept (but do not differentiate) the rest.

| QoS policy | Behavior |
|---|---|
| `history` = KEEP_LAST | **Enforced.** Subscription queue capped at QoS `depth`. Oldest messages dropped when full. |
| `history` = KEEP_ALL | **Treated as KEEP_LAST with depth=10.** Unbounded queues are not supported. |
| `depth` | **Enforced.** Controls both subscription queue size and TRANSIENT_LOCAL publisher cache size. |
| `durability` = VOLATILE | **Enforced.** Publisher doesn't cache. Late-joining subscribers miss past messages. |
| `durability` = TRANSIENT_LOCAL | **Enforced.** Publisher caches the last `depth` messages in a `std::deque<CachedMessage>`. Replay to late-joining subscribers fires through **two paths**: (1) inside `rmw_publish`, after appending the new message, replaying `message_cache[0 .. size-2]` to any new path (the most recent entry is the about-to-be-sent live message, delivered by the live publish loop below); (2) inside `rmw_wait`, on registry `generation` change, replaying the **entire** `message_cache` to any new path — because no live publish is following. Both paths cooperate via `known_subscriber_paths` (a per-publisher `std::set<std::string>`): whichever runs first marks the path, the other becomes a no-op. The wait-path is what makes a publisher that publishes once at startup and stays idle still serve cached messages to subscribers that join later. `known_subscriber_paths` grows monotonically — if a subscriber dies its path is reclaimed but the set entry remains (bounded by total subscribers ever seen on that topic in this process). To support the wait-path, every TRANSIENT_LOCAL publisher is registered in `UdsContext::transient_local_pubs` at create time, removed at destroy, and the mutex protecting that list is held across the whole `rmw_wait` iteration so concurrent destroys can't free a publisher mid-iteration. |
| `reliability` = RELIABLE | **Accepted, not differentiated.** Unix sockets are inherently reliable on localhost (kernel doesn't drop unless `SO_RCVBUF` overflows). |
| `reliability` = BEST_EFFORT | **Accepted, not differentiated.** Identical to RELIABLE on this transport. |
| `deadline` | **Not enforced.** Accepted but ignored. Would require a timer thread to monitor delivery intervals. |
| `lifespan` | **Not enforced.** Accepted but ignored. Would require timestamped expiration checks on every message. |
| `liveliness` | **Not enforced.** Accepted but ignored. Would require periodic heartbeat checking. |
| `liveliness_lease_duration` | **Not enforced.** |

**QoS compatibility check** (`rmw_qos_profile_check_compatible`):

| Publisher | Subscriber | Result | Reason |
|---|---|---|---|
| BEST_EFFORT | RELIABLE | Warning | Subscriber expects reliable delivery but publisher offers best-effort. Identical on this transport, but flagged as intent mismatch. |
| VOLATILE | TRANSIENT_LOCAL | Warning | Subscriber expects late-joining replay but publisher doesn't cache. The subscriber will never receive past messages. |

All other combinations return `RMW_QOS_COMPATIBILITY_OK`.

### 8. Publisher-to-Subscriber Fanout

**Choice**: When a publisher publishes, it queries the registry for all subscribers on that topic and sends a copy to each subscriber's socket.

**Why not a central router?**
- A router process adds latency (extra hop), complexity (another process to manage), and a single point of failure.
- Direct publisher-to-subscriber communication has lower latency.

**Trade-off**: Each subscriber gets its own copy. With N subscribers the publisher makes N `sendmsg()` calls. For very high-fanout topics (>50 subs to the same topic) this could become a bottleneck. Acceptable for the 200-node target.

### 8a. Generation-keyed discovery cache (hot-path optimization)

**Problem**: Once the production system reached ~150 nodes, every `rmw_publish`, `rmw_send_request`, and `rmw_send_response` was contending for a single registry mutex (the v1/v2 layout) to look up destination socket paths. End-to-end latency rose to ~500 ms because dozens of publishers were serializing through one lock.

**Choice**: Each publisher, client, and service caches its discovery results, keyed off the registry's monotonically-increasing `generation` counter. The cache is invalidated only when the topology actually changes.

**How it works**:
- The registry header has an `std::atomic<uint64_t> generation`, bumped whenever an entry is added or removed.
- Each `UdsPublisher` stores `(cached_generation, cached_subscriber_paths)`.
- On publish, we read the current generation with a single relaxed-acquire atomic load. If it matches `cached_generation`, we send to the cached paths immediately — zero mutex acquisitions, zero registry scans.
- If the generation changed, we lock the per-publisher cache mutex (cheap, uncontended), call `registry_query` (now itself lock-free thanks to §3), refresh the cache, and publish.
- Same pattern for `UdsClient` (caches the resolved service socket path) and `UdsService` (caches a `GID -> socket_path` map for routing responses).

**Why this is safe**:
- The generation counter only increases. A stale cache means we either (a) miss a brand-new subscriber — fine, they'll get the next message after the cache refreshes — or (b) try to send to a socket file that no longer exists. Case (b) is harmless: `sendto` returns `ENOENT`, which we silently swallow, and the next publish refreshes the cache.
- The atomic load on `generation` is the only cross-process synchronization needed for the fast path.

**Result**: Publish/take latency on a 150-node fleet drops from hundreds of milliseconds back to the tens-of-microseconds range that the kernel's UDS path actually delivers.

### 9. Service Implementation

**Choice**: Services use the same `SOCK_DGRAM` transport and the same `WireHeader` as topics. The client sends a request (`msg_type=1`) to the service server's socket; the server sends the response (`msg_type=2`) back to the client's socket, looked up from the registry by client GID.

**Why DGRAM instead of STREAM for services?**
- Keeps the implementation uniform — one transport for everything.
- Avoids connection management complexity.
- Service requests/responses are typically small and fit easily in a single datagram.

**Routing**: the server caches a `(gid, socket_path)` map of every client it has seen. When `rmw_send_response` is called with a response and the client's GID, the server looks up the path and `sendmsg`s. Same generation-keyed invalidation as §8a.

**Service-server availability** (`rmw_service_server_is_available`): a registry walk. No active probing or ping. Cheap because the registry walk is lock-free.

**Multiple services with the same name**: first-match-wins. Deterministic but not round-robin (DDS does load-balance across matching servers; we don't). In practice production has exactly one server per service.

**Request queue depth**: **not enforced.** Unlike subscriptions, services drain the bound socket into an unbounded `std::deque<ReceivedMessage>`. Back-pressure comes from the kernel: when the service's `SO_RCVBUF` fills, the client's `sendmsg` returns `EAGAIN`. We could enforce a per-service depth but it has never been a problem in practice.

### 10. Graph Guard Condition

**Choice**: The shared memory registry has a `generation` counter that increments on every add/remove. Each `rmw_wait()` call snapshots the counter; if it changed since the last wait, the **per-node** graph guard conditions are triggered, waking executors that registered for graph events.

**Why poll instead of push?**
- Push notification across processes without a daemon requires complex IPC (signals, a shared eventfd per node, etc.).
- Polling the generation counter is one atomic load per wait call.
- `rmw_wait` is called very frequently by the executor; graph changes are detected within milliseconds.

**Implementation note**: there is also a `graph_guard_condition` field on the context. It is currently dead code — per-node guard conditions cover every observed use case, and the context-level GC was never wired up by any caller. We keep the field for ABI completeness.

### 11. GID Generation

**Choice**: GIDs are 16 bytes: `[pid (4 bytes)][atomic counter (4 bytes)][zeros (8 bytes)]`.

**Why this scheme?**
- Guaranteed unique on a single machine (different PIDs for different processes, monotonic counter within a process).
- No UUID library dependency.
- 16 bytes matches `RMW_GID_STORAGE_SIZE`.

### 12. Build System

**Choice**: `ament_cmake` with `configure_rmw_library()` and `register_rmw_implementation()`.

**Dependencies**: only standard ROS 2 packages + Linux kernel APIs:
- `rmw` (abstract interface)
- `rcutils`, `rcpputils` (utilities, logging)
- `rosidl_typesupport_fastrtps_c` / `rosidl_typesupport_fastrtps_cpp` (CDR callbacks)
- `fastcdr` (serialization)
- `pthread`, `rt` (POSIX threading, shared memory)

**Why no external middleware?**
- The whole point is to avoid heavy middleware like DDS.
- Discovery, transport, wait, and serialization are all built on Linux primitives or per-type generated code.

---

## File Structure

| File | Purpose |
|------|---------|
| `identifier.hpp` | Implementation identifier (single inline `constexpr char[]`). |
| `types.hpp` | All internal C++ data types (`UdsContext`, `UdsNode`, `UdsPublisher`, `UdsSubscription`, `UdsService`, `UdsClient`, `UdsWaitSet`, `WireHeader`, ...). |
| `registry.hpp/cpp` | Shared memory discovery registry, seqlock + atomic state machine, stale-PID cleanup. |
| `transport.hpp/cpp` | Unix socket send/receive helpers, socket-file lifecycle, orphan cleanup. |
| `serialization.hpp/cpp` | CDR serialization wrappers around fastcdr callbacks. |
| `logging.hpp` | `rcutils`-backed logging macros used throughout. |
| `rmw_init.cpp` | Context initialization, registry open, send-socket creation, shutdown. |
| `rmw_node.cpp` | Node create/destroy and graph-guard-condition accessor. |
| `rmw_publisher.cpp` | Publisher create/destroy/publish, generation-keyed cache, TRANSIENT_LOCAL replay. |
| `rmw_subscription.cpp` | Subscription create/destroy/take, queue, `on_new_message` callback. |
| `rmw_service.cpp` | Service server. |
| `rmw_client.cpp` | Service client. |
| `rmw_guard_condition.cpp` | Eventfd-backed guard condition. |
| `rmw_wait.cpp` | epoll-based wait set, drain/ready/block/drain. |
| `rmw_graph.cpp` | Discovery / introspection queries — all reduce to lock-free registry walks. |
| `rmw_event.cpp` | Event stubs — events are accepted but never generated. |
| `rmw_serialize.cpp` | Standalone serialize/deserialize APIs. |
| `rmw_features.cpp` | Identifier query, feature flags, QoS-compatibility check. |
| `rmw_unsupported.cpp` | All APIs that return `RMW_RET_UNSUPPORTED`. |

---

## Resource Usage

| Resource | Usage |
|----------|-------|
| Shared memory | ~36.5 MiB per domain (fixed, one file per `ROS_DOMAIN_ID`) |
| Socket per subscription | 1 fd + 1 file in `/tmp/ros2_uds/<domain>/` |
| Socket per service / client | 1 fd + 1 file each |
| Send socket per context | 1 fd (shared by all publishers in the process) |
| epoll fd per wait set | 1 fd |
| eventfd per guard condition | 1 fd |
| Background threads | **0** |
| Registry entries per minimal node | 9 (node + parameter_events publisher + 7 services) |

For 200 nodes with 10 pub + 10 sub each: ~4000 registry entries from user pub/sub, ~1800 from node baselines, ~2000 socket files, ~2000 fds total across all processes.

---

## Operational requirements

Required system tuning for a production deployment. Without these, you will hit transient drops and silent buffer clamps under load.

| Knob | Default | Recommended | Why |
|---|--:|--:|---|
| `net.core.rmem_max` | ~212 KB | 48 MB+ | Caps `SO_RCVBUF`. We request 48 MB; without this we get ~212 KB and large messages drop. |
| `net.core.wmem_max` | ~212 KB | 48 MB+ | Same for `SO_SNDBUF`. Especially relevant for `EMSGSIZE` on point clouds. |
| `net.unix.max_dgram_qlen` | 512 | 4096 | Per-Unix-dgram-socket pending-datagram cap. Bursty fan-out (e.g. a new node appearing on a 150-node fleet) overflows 512 with small messages. |
| `ulimit -n` (`nofile`) | 1024 | 65536 | Each pub/sub/service/client takes 1–2 fds. A 150-node fleet exceeds 1024 quickly. |

Docker requirements:
- `--ipc=host` — for `/dev/shm` sharing
- `--pid=host` — for cross-container stale-PID cleanup correctness
- `-v /tmp/ros2_uds:/tmp/ros2_uds` — for socket file sharing

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
| **Localhost only** | No network communication | This RMW is designed for single-machine deployments. `AF_UNIX` is local-only by definition. For network communication use CycloneDDS or Zenoh. |
| **Message size limit** | Tunable via sysctl; defaults to ~212 KB | Capped by `SO_SNDBUF` / `SO_RCVBUF` which are themselves capped by `wmem_max` / `rmem_max`. Point clouds and images may exceed defaults. |
| **Linux only** | Not portable to macOS or Windows | Uses Linux-specific kernel APIs: `epoll`, `eventfd`, `/dev/shm`, `AF_UNIX`. |
| **Docker requires shared IPC, PID, and `/tmp` mount** | See *Operational requirements* | Cross-container discovery and stale-PID cleanup correctness require unified namespaces. |
| **No DDS interoperability** | Cannot communicate with DDS-based RMW implementations | All nodes on the machine must use `rmw_unix_socket_cpp`. |

### Functions that return `RMW_RET_UNSUPPORTED`

These are part of the RMW API but cannot be implemented with Unix socket transport. They return `RMW_RET_UNSUPPORTED` per the RMW specification, which tells rcl/rclcpp to skip the feature gracefully.

| Function(s) | Feature | Why unsupported |
|---|---|---|
| `rmw_borrow_loaned_message`, `rmw_publish_loaned_message`, `rmw_take_loaned_message`, `rmw_take_loaned_message_with_info`, `rmw_return_loaned_message_from_publisher`, `rmw_return_loaned_message_from_subscription` | **Zero-copy loaned messages** | Require shared memory between publisher and subscriber so both can access the same memory region. Unix sockets copy through kernel buffers. Zero-copy needs an iceoryx-style transport. |
| `rmw_take_dynamic_message`, `rmw_take_dynamic_message_with_info`, `rmw_serialization_support_init` | **Dynamic message types** | Require a runtime type-discovery protocol (DDS Type Object). Our registry doesn't carry per-field schema information. |
| `rmw_publisher_get_network_flow_endpoints`, `rmw_subscription_get_network_flow_endpoints` | **Network flow endpoints** | Return IP/port for QoS tracing. `AF_UNIX` sockets have neither. |
| `rmw_subscription_set_content_filter`, `rmw_subscription_get_content_filter` | **Content filtering** | DDS evaluates SQL-like expressions per message. Not implemented. All subscribers receive all messages. |
| `rmw_event_set_callback` | **Event callbacks** | Events are initialized successfully (to prevent crashes when rcl adds them to wait sets) but never generated. `rmw_event_type_is_supported` returns `false` for all event types. |

### Functions that return `RMW_RET_OK` but are no-ops

The RMW spec allows implementations to accept but not enforce certain features.

| Function(s) | Feature | Why no-op |
|---|---|---|
| `rmw_init_publisher_allocation` / `rmw_fini_publisher_allocation` | **Pre-allocated publishing** | Our serialization allocates dynamically per-publish (via `fastcdr`). Hints are accepted but ignored. |
| `rmw_init_subscription_allocation` / `rmw_fini_subscription_allocation` | **Pre-allocated taking** | Same — hints accepted but ignored. |
| `rmw_publisher_assert_liveliness`, `rmw_node_assert_liveliness` | **Manual liveliness assertion** | We don't track liveliness. |
| `rmw_publisher_wait_for_all_acked` | **Wait for acknowledgment** | Unix domain sockets deliver immediately on localhost — nothing to wait for. |
| `rmw_set_log_severity` | **RMW log level** | We use `rcutils` logging; severity is accepted but has no effect on our own logs. |
