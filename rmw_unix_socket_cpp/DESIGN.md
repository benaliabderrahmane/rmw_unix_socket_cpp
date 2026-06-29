# rmw_unix_socket_cpp - Design Document

`rmw_unix_socket_cpp` is a ROS 2 middleware (RMW) implementation that runs entirely on a single host. It replaces the DDS layer that ROS 2 normally sits on top of. There is no discovery daemon, no message broker, and no central router. Two Linux kernel primitives do all the work: `AF_UNIX` datagram sockets carry every message directly from sender to receiver, and a single POSIX shared-memory object per ROS domain serves as the discovery registry that every process reads and writes. When no process is using the system, nothing is running. The shared-memory table *is* the discovery system.

The design solves one problem: DDS discovery does not scale well to large single-host node graphs. On a production system of well over a hundred nodes, DDS-based RMW implementations made bring-up slow and unstable, and discovery never settled. Adding one node, or even a `ros2 topic list`, could trigger another round of discovery traffic that disturbed the running system. This RMW optimizes for two goals, in order. The first is robustness: discovery must be cheap so that bring-up is fast and stable, and so that adding a node or issuing a CLI command does not set off a storm of activity across the graph. The second is near-zero configuration: there should be no large set of QoS and discovery parameters to tune before the middleware behaves. The target is install it, set `RMW_IMPLEMENTATION`, and run.

The scope is narrow. This is a localhost-only transport by design. `AF_UNIX` sockets are local to one machine, the shared-memory registry lives in that machine's `/dev/shm`, and the socket files live under its `/tmp/ros2_uds/`. There is no network path and no DDS interoperability, so every node on the host must use this RMW, and nodes cannot span hosts. That constraint is what makes discovery cheap enough to meet the robustness goal: a single shared table on one machine is the simplest way for processes to find each other, and it avoids the network-wide announcement traffic that makes distributed discovery expensive. The rest of this document explains the registry, the transport, the wait mechanism, the simplifications that fall out of being single-host, and the reasoning behind each choice.

## Architecture Overview

The middleware has four components: a shared-memory discovery registry, an AF_UNIX datagram transport, a serialization layer, and a wait mechanism built on `epoll`. There is no daemon and no broker process. Every process that loads the RMW maps the same registry, opens its own sockets, and talks directly to its peers. When no ROS 2 process is running, nothing of this middleware is running either. This is the central design decision: discovery is reduced to reading and writing a shared table, which is what makes bring-up fast and stable at high node counts.

**Discovery registry (shared memory).** The registry is a single POSIX shared-memory object, created with `shm_open` and mapped `MAP_SHARED` into every process so they all see the same bytes. The name carries the ROS domain id (`/dev/shm/ros2_uds_<domain_id>`), so different domains map different files and never see each other. The layout is fixed up front: a small header (`RegistryHeader`) followed by a flat array of 32768 fixed-size slots (`RegistryEntrySlot`, 1168 bytes each). A fixed array avoids the complexity of dynamic growth, free lists, and pointer invalidation. Every node, publisher, subscription, service, and client occupies exactly one slot holding what a peer needs to reach it: entity type, owning PID, GID, names, type string, QoS, and the Unix socket path that is the endpoint's actual address. Reads and writes are lock-free. Each slot is a seqlock (an even/odd sequence counter that lets a reader detect a torn read — seeing half of one write and half of the next) with an atomic `state` field, so writers claim and publish slots with compare-and-swap (CAS) and readers never take a global lock. A robust mutex in the header is used only once, for the creation handshake. Putting a lock on the discovery path would serialize every process at startup, which is exactly the contention this RMW exists to avoid.

**Transport (AF_UNIX datagrams).** All data, whether topic messages, service requests, or service responses, travels over `SOCK_DGRAM` Unix domain sockets. Each receiving endpoint binds its own socket to a file under `/tmp/ros2_uds/<domain_id>/`; that file path is its address, and it is the same path published in the registry. Each process has exactly one unbound send socket shared by all of its publishers and clients. `SOCK_DGRAM` is chosen over `SOCK_STREAM` because it preserves message boundaries (one `recvmsg` returns exactly one message, so there is no framing code) and is connectionless (no `connect`, `accept`, or per-peer state to manage). The data never touches the IP stack, the loopback interface, or a port number; it goes straight through the kernel's Unix-socket layer from one process to another. The cost of this simplicity is an extra copy: every message is copied through a kernel socket buffer rather than shared in place, so there is no zero-copy payload path. A single message must also fit in one datagram (roughly 400 KB on a stock kernel; see the per-message size cap under Transport).

**Serialization (CDR via fastcdr).** Messages are serialized to CDR using `fastcdr` driven by the `rosidl_typesupport_fastrtps_cpp` callbacks generated for each message type. This is the same encoding the default DDS-based RMWs use. Because the serialize and deserialize routines are compiled per message type, there is no runtime field walking. The Serialization section explains why an earlier introspection-based serializer was abandoned.

**Wait mechanism (`epoll` + `eventfd`).** `rmw_wait()` blocks on `epoll`, watching the receive-socket file descriptors (fds) of every subscription, service, and client in the wait set plus an `eventfd` per guard condition. `epoll` is used here rather than `poll`/`select`; the wait section explains why. There are no background receiver threads: all socket draining happens inside `rmw_wait()`, which the executor already calls in a tight loop. This keeps the data path single-threaded per process and avoids lock contention on internal queues.

**End-to-end publish flow.** On `rmw_publish`, the node serializes the ROS message to CDR once, producing the payload. It fills in a fixed 37-byte wire header (sender GID, sequence number, send timestamp, payload size, and a type byte). It then reads the registry's `generation` counter and compares it to the value cached on the publisher. If the graph has not changed, the publisher reuses its cached list of subscriber socket paths and never touches the registry. Only when `generation` has moved does it re-scan the registry to rebuild that list. For each subscriber path it issues one `sendmsg` with gather I/O, so the header and payload become one datagram with no intermediate copy. Sends are non-blocking and best-effort, one kernel copy into each subscriber's socket buffer. On the receiving side, a later `rmw_wait()` drains the bound socket, splits the header from the payload, and queues the message; `rmw_take` then deserializes it and hands it to the subscription callback. In steady state, the only discovery cost per publish is a single atomic read of the generation counter.

```
PUBLISHER PROCESS                                  SUBSCRIBER PROCESS
-----------------                                  ------------------

  rmw_publish(msg)
        |
        v
  +------------------+
  | fastCDR serialize|   ROS message -> CDR bytes (the payload)
  +------------------+
        |
        v
  +------------------+
  | build WireHeader |   37 bytes: gid, seq, timestamp, size, type
  +------------------+
        |
        |   look up subscriber paths (cached;
        |   re-queried only when registry
        |   generation counter changes)
        v
  for each subscriber path:
  +-----------------------------+
  | sendmsg() gather I/O        |
  | iov[0]=header  iov[1]=payload|
  | one datagram, named to path |
  +-----------------------------+
        |
        |   ===== kernel AF_UNIX datagram =====
        |   (no TCP, no IP, no loopback;
        |    one copy into the dest buffer)
        |
        v
                                          /tmp/ros2_uds/<domain>/sub_<pid>_<id>.sock
                                          +---------------------------+
                                          | bound receive socket      |
                                          | (one per subscription)    |
                                          +---------------------------+
                                                      |
                                                      v
                                          +---------------------------+
                                          | recv_from: peek size,     |
                                          | grow buffer, real recv,   |
                                          | split header + payload    |
                                          +---------------------------+
                                                      |
                                                      v
                                          +---------------------------+
                                          | rmw_take -> deserialize   |
                                          | -> subscription callback  |
                                          +---------------------------+
```

## ABI conventions

These apply everywhere in `src/` and are non-obvious to readers new to the RMW layer.

- **All public entry points are `extern "C"`.** rcl `dlopen`s the RMW shared library and calls fixed C symbols; C++ name mangling would break linkage.
- **The implementation identifier is compared by pointer, not by `strcmp`.** Every RMW handle carries `implementation_identifier`. We declare it as `inline constexpr char identifier[] = "rmw_unix_socket_cpp";` so it has a single address across all translation units (C++17 guarantee).
- **No exceptions cross the RMW boundary.** Every public function returns `rmw_ret_t` or a pointer. Exceptions from `fastcdr` are caught at the serialization boundary.
- **The `void * data` field on every RMW struct holds our private struct** (`UdsNode`, `UdsPublisher`, ...). Defined in `types.hpp`. Recovered by `static_cast` on every entry.
- **Lock-free registry, mutex-protected per-entity caches.** The shared-memory registry uses a per-slot seqlock + atomic state machine (no global mutex on hot paths). Per-publisher / -client / -service caches use `std::mutex` because they are process-local and rare.

## Transport: AF_UNIX datagram sockets

All message data (topic publications, service requests, and service responses) moves between processes over `AF_UNIX` (Unix domain) sockets of type `SOCK_DGRAM`. This is the local-only kernel transport. It never touches the IP stack: there is no TCP, no loopback interface, no `127.0.0.1`, and no port numbers. A datagram is handed to the kernel addressed to a filesystem path, and the kernel copies it into the receiving process's socket buffer. For single-host communication this is the shortest path the kernel offers.

### Why `SOCK_DGRAM` and not `SOCK_STREAM`

The transport is connectionless on purpose, and the choice of `SOCK_DGRAM` follows directly from how pub/sub works.

A datagram socket preserves message boundaries. Each `sendmsg()` produces exactly one datagram, and each `recvmsg()` returns exactly one complete message. A ROS message maps one-to-one onto a datagram, so there is no framing layer: no length prefixes, no buffering partial reads, no logic to find where one message ends and the next begins. With `SOCK_STREAM` the kernel delivers an undelimited byte stream, and the RMW would have to add and parse that framing itself.

A datagram socket is also connectionless. The sender names a destination path on every send; there is no `connect()`, no `accept()`, and no per-connection state. This matters at the node counts this RMW targets. A publisher with N subscribers would otherwise need N open stream connections, each set up, torn down, and tracked. With datagrams the publisher holds no connection state at all. It sends to a path, and if nobody is bound there the send simply fails (the publisher treats that as a routine teardown race; see the send path below). Fewer file descriptors, no connection lifecycle, no half-open states to reason about.

This is closer in behavior to UDP than to TCP, but without any of the network stack. The kernel's Unix domain socket layer moves the bytes from one process directly to another.

### Addressing: the file path is the address

A Unix domain socket is addressed by a path on the filesystem. The path is stored in the kernel's `sockaddr_un.sun_path` field, which is 108 bytes; that fixed size is why the registry stores socket paths in a 108-byte field.

Each receiving endpoint creates its own socket and `bind()`s it to a unique path under `/tmp/ros2_uds/<domain_id>/`. "Receiving endpoint" means a subscription, a service server, or a service client. Each calls `create_bound_socket()` with a path from `make_socket_path()`. That bound path is the endpoint's address, and it is exactly what the endpoint writes into its registry slot. Discovery's entire job is to hand a publisher the bound socket paths of the subscribers it should send to.

The domain id appears in the directory name (`/tmp/ros2_uds/0/`, `/tmp/ros2_uds/1/`, and so on), so endpoints on different ROS domains bind under different directories and never address each other. The path within that directory is built by `make_socket_path()` as `<prefix>_<pid>_<unique>.sock`, where the prefix names the endpoint kind (`sub`, `srv`, `cli`), the PID identifies the owning process, and a per-process counter combined with a time component guarantees uniqueness. Two endpoints never collide, and because the PID is embedded in the name, a socket file left behind by a crashed process can be recognized and unlinked later (`cleanup_orphan_socket_files()` parses the PID out of the name and checks `/proc/<pid>`).

### One shared send socket per process

Receiving needs a bound, addressable socket; sending does not. A sender only ever names a destination, so it needs no address of its own.

Each process therefore creates exactly one send socket, in `rmw_init()`, stored on the context as `send_socket_fd`. It is an unbound, anonymous `SOCK_DGRAM` socket. Every publisher, service, and client in that process shares it: they all call `sendmsg()` on the same descriptor, naming the destination path per call. The send socket never receives anything and never binds to a path. Because the descriptor is shared, concurrent senders rely on a single `sendmsg()` being atomic at the datagram level, which the kernel guarantees for `SOCK_DGRAM`: two threads each sending one datagram cannot interleave their bytes.

This keeps the file-descriptor count low: a process pays one fd per receiving endpoint plus a single shared send fd, regardless of how many publishers it hosts or how many peers they talk to.

Both socket types request a large buffer (`SO_RCVBUF` on the bound socket, `SO_SNDBUF` on the send socket). The kernel silently clamps these requests to `net.core.rmem_max` / `net.core.wmem_max`, which bounds the largest message a single datagram can carry; see The per-message size cap below.

### The send path

On `rmw_publish`, the ROS message is serialized exactly once. The publisher hands the message to the cached `rosidl_typesupport_fastrtps` callback, which produces a single flat byte buffer: the CDR encapsulation header followed by the message body. This buffer is the payload. It is built once per publish regardless of how many subscribers will receive it, because the bytes on the wire are identical for every subscriber.

Next a fixed wire header is filled in. `WireHeader` is a packed 37-byte struct (`types.hpp`), and its layout is pinned by `static_assert`s so it stays a stable cross-process, cross-build contract. The fields are:

- `gid` (16 bytes): the publisher's GID. Its routing role is covered under Services.
- `sequence_number` (8 bytes): a per-publisher monotonic counter, taken with a relaxed atomic fetch-add.
- `source_timestamp_ns` (8 bytes): the send time in nanoseconds, used by the receiver to populate message info.
- `payload_size` (4 bytes): the CDR payload length, which lets the receiver split the datagram back into header and payload and detect truncation.
- `msg_type` (1 byte): `0` for a topic message, `1` for a service request, `2` for a service response. The same transport carries all three, and this byte tells the receiver which queue the datagram belongs in.

The header is built on the stack and is never serialized through CDR. It is a plain byte blit, which is why its layout must be fixed rather than negotiated.

With the payload and header in hand, the publisher resolves the subscriber list (the cached set of socket paths, refreshed only when the registry generation counter has moved) and then loops over it. For each subscriber path it calls `send_to`, which performs the actual transmission.

`send_to` sends the header and payload as one datagram using gather I/O (a single `sendmsg` that reads from several separate buffers and writes them as one datagram). It sets up a two-element `iovec`: `iov[0]` points at the wire header, `iov[1]` points at the payload buffer. A single `sendmsg` call, with the destination socket path set in `msg_name`, then writes both regions into one datagram. The kernel reads from both buffers and stitches them into a single message, so there is no intermediate buffer that concatenates header and payload first. Each subscriber therefore costs one `sendmsg` and one kernel copy into that subscriber's receive buffer. With N subscribers the publisher issues N `sendmsg` calls and pays N copies; there is no multicast and no shared buffer between subscribers. If the payload is empty, `msg_iovlen` is set to 1 so only the header is sent.

The send is non-blocking and best-effort. `sendmsg` is called with `MSG_DONTWAIT` (never block on a full buffer) and `MSG_NOSIGNAL` (a dead peer returns an error instead of raising `SIGPIPE` and killing the process). On success the function returns true. On failure it classifies `errno`, logs at an appropriate throttled level, and returns false. It does not throw, retry, or abort the loop. The publisher ignores the return value and proceeds to the next subscriber. This is the deliberate fan-out semantics: a failure for one peer causes a drop for that peer only, and delivery to every other subscriber is unaffected. The error classification is described under Send failure handling below.

### The receive path

Every receiving endpoint owns its socket. Each subscription (and each service and client) creates one `AF_UNIX` `SOCK_DGRAM` socket and binds it to a file under `/tmp/ros2_uds/<domain_id>/` (the path format is covered under Addressing above). That bound socket is the endpoint's address: it is the path a publisher writes into the registry, and the path senders name on every `sendmsg`. Because the socket is per-subscription, the kernel keeps a separate receive buffer per endpoint, so a slow subscriber can only fill its own queue. There is no shared inbox and no demultiplexing step on the receive side.

Reading one datagram off a socket is done by `recv_from` in `transport.cpp`. The complication it solves is that the caller does not know in advance how big the next message is, and a `SOCK_DGRAM` `recv` that supplies too small a buffer silently truncates the datagram and discards the rest. The function avoids that in two steps:

1. **Peek to learn the size.** A first `recv` is issued with `MSG_PEEK | MSG_TRUNC | MSG_DONTWAIT`. `MSG_PEEK` leaves the datagram on the socket, and `MSG_TRUNC` makes the call return the datagram's true length even when the peek buffer was smaller. A thread-local 256 KB scratch buffer is reused across calls so the common small-message case allocates nothing. The peek return value `n` is the real size on the wire.
2. **Size the buffer, then recv for real.** If `n` is larger than the scratch buffer, the buffer is grown to `n`. A second `recv` (without `MSG_PEEK`) then consumes the datagram into a buffer that is now guaranteed large enough, so no truncation occurs.

`recv_from` distinguishes the three outcomes of the peek. A negative return with `errno == EAGAIN`/`EWOULDBLOCK` is the steady-state "nothing to read" case driven by the `rmw_wait` loop and is silent by design; any other negative errno (for example `EBADF` after shutdown) is logged throttled. A zero return means no message. Only a positive return proceeds to the real `recv`.

**Splitting the wire header from the payload.** Each datagram is one `WireHeader` (the 37-byte struct built on the send path) immediately followed by the serialized message body. After the real `recv`, `recv_from` validates and splits:

- If fewer than `sizeof(WireHeader)` bytes arrived, the datagram is a runt and is dropped (logged throttled).
- The header is copied out of the front of the buffer with a single `memcpy` into `header_out`.
- The payload length is computed as `n - sizeof(WireHeader)`. It is cross-checked against the `payload_size` field the sender wrote into the header. A mismatch means the kernel truncated an oversized datagram on the way in; the length is clamped to the header's value and the event is logged so it can be correlated with the sender-side `EMSGSIZE`.
- The remaining bytes are copied into the caller's `payload_out` vector.

`recv_from` returns one parsed message at a time. The subscription drains its socket in a loop: `drain_subscription` calls `recv_from` repeatedly until it returns false (socket empty). Each datagram is filtered by `msg_type` so that only topic messages land in this queue, wrapped in a `ReceivedMessage` (header plus payload), and pushed onto the per-subscription queue under its mutex. The queue is capped at the QoS `depth`; when a push exceeds the cap the oldest entry is dropped, which is the KEEP_LAST history policy enforced at the rcl boundary rather than in the kernel. This drain runs inside `rmw_wait`, not on a background thread, which is why there is no receiver thread to manage.

**Deserialize and deliver.** `rmw_take` (and its variants) first drains the socket, then pops one `ReceivedMessage` from the queue. The payload, which is still raw CDR bytes, is handed to `deserialize`, which invokes the message type's compiled `cdr_deserialize` callback to reconstruct the ROS message in place. On a deserialization failure the take returns "no message taken" rather than a hard error, since a corrupted or truncated payload should not abort the executor; the failure is logged throttled. The `_with_info` variants additionally copy the source timestamp, sequence number, and publisher GID out of the wire header into the `rmw_message_info_t` returned to the caller. The subscription callback registered by the executor is triggered from the drain step when a new message is enqueued, which is what wakes `rmw_take`.

#### In-process concurrency

There are no background threads, so all sends, drains, and takes run on the caller's thread, typically the executor thread inside `rmw_wait`. Under a `MultiThreadedExecutor`, several threads may call `rmw_take`, `rmw_publish`, and `rmw_wait` concurrently on different entities. The data structures are sized for this: each subscription, service, and client guards its own receive queue with a per-entity `queue_mutex`, and each publisher, service, and client guards its discovery cache with its own cache mutex (`sub_cache_mutex`, `cache_mutex`, `svc_cache_mutex`, `client_cache_mutex`). Concurrent operations on different entities therefore do not contend. The one process-wide shared resource on the send side is the single send socket; concurrent senders on it are safe because the kernel delivers each `SOCK_DGRAM` `sendmsg` as one atomic datagram, so two threads' messages never interleave.

### The per-message size cap (~400 KB)

Each message travels as exactly one `SOCK_DGRAM` datagram. A datagram is atomic: the kernel either accepts the whole thing or rejects it. So one ROS message, after the 37-byte wire header and the CDR payload are added together, must fit inside a single datagram. The ceiling on that datagram size is the socket buffer, and the socket buffer is not whatever the code asks for.

`create_bound_socket()` and `create_send_socket()` both request a 48 MiB buffer:

```c
static constexpr int RECV_BUF_SIZE = 48 * 1024 * 1024;  // source comment loosely labels this 48 MB; the value is 48 MiB
static constexpr int SEND_BUF_SIZE = 48 * 1024 * 1024;
```

That request is not honored as written. When `setsockopt(SO_SNDBUF)` or `setsockopt(SO_RCVBUF)` is called, the kernel clamps the value to the system-wide limits `net.core.wmem_max` (send) and `net.core.rmem_max` (receive). `setsockopt` still returns 0 on a clamp, so the clamp is silent: there is no error to catch, the buffer is just smaller than asked for. Neither call site reads the effective size back with `getsockopt`; the warning at `transport.cpp:66-68` / `86-88` fires only on an outright `setsockopt` failure and logs the requested constant, not the effective size. So a clamp is invisible to the process.

The kernel stores twice the requested `SO_SNDBUF`/`SO_RCVBUF` value (`sk_sndbuf = 2 x request`, itself clamped to `wmem_max`), and an `AF_UNIX` datagram must fit within that doubled buffer minus per-`skb` overhead. On a stock kernel both `wmem_max` and `rmem_max` default to `212992` bytes (about 208 KiB), so the doubled buffer is about 416 KiB and the usable single-datagram payload is somewhat less after overhead, on the order of a few hundred KB. The 48 MiB request is far above this and gets clamped on any untuned system.

A message that exceeds the cap is not split or queued. `sendmsg()` fails immediately with `EMSGSIZE`. The transport treats this as a configuration error rather than transient backpressure, and the error message names the fix directly:

```
UDS send to '%s' failed: message too big (%zu bytes incl. header).
Raise net.core.{wmem_max,rmem_max} or split the message.
```

This is acceptable for the target workload because a large single-host node graph mostly carries commands, status, and telemetry, all small messages that never approach the cap. It only bites on large payloads such as point clouds and uncompressed images. To raise it, increase both sysctls (the send side is bounded by `wmem_max`, the receive side by `rmem_max`, so both must be raised) before the nodes start:

```bash
sysctl -w net.core.wmem_max=8388608
sysctl -w net.core.rmem_max=8388608
```

These values are 8 MiB; after the kernel's 2x they yield a usable single-datagram ceiling of roughly 16 MiB. The 48 MiB request in the code is deliberately well above any reasonable sysctl value so that raising the limits is the only thing a user has to do; the code never has to be rebuilt to use a larger buffer.

### Send failure handling

Every send goes through `send_to` in `transport.cpp`, which calls `sendmsg()` once with `MSG_DONTWAIT | MSG_NOSIGNAL`. The send is non-blocking and never raises `SIGPIPE`, so a failure surfaces as a return value plus `errno` rather than a signal or a stall. On success the function returns immediately. On failure it captures `errno` and routes it through a `switch` that maps each cause to a deliberate log level and message. Classifying matters because "send failed" covers three very different situations: a misconfigured system, a slow consumer, and a peer that is simply gone. Logging them identically would either bury a real problem or flood the log with noise that is not a problem.

Every category is reachable on the hot path, so the loud ones are rate-limited (`*_THROTTLE` with a per-category interval in milliseconds). A single persistently-failing peer cannot drown the log. The destination path in the message identifies the offending endpoint on its own: `make_socket_path` encodes the peer's prefix and PID into the filename, so the path alone is enough to find the subscriber without correlating against other state.

**`EMSGSIZE` (logged as error, 5s throttle).** The message exceeds the kernel's per-datagram cap (see The per-message size cap above). This is a configuration-level fault, not transient backpressure: it will fail every time until the limits are raised or the message is split, and it indicates the system is set up wrong for the traffic it carries. So it is logged at error level with the full byte count (header plus payload) and an actionable message naming the two sysctls to raise. The 5s throttle is looser than the slow-consumer case because this condition is steady, not bursty.

**`EAGAIN` / `EWOULDBLOCK` / `ENOBUFS` (logged as warning, 1s throttle).** These mean the destination socket's receive queue is full (or, briefly, the local send queue). This is the classic slow-subscriber symptom: the subscriber is not calling `rmw_take` fast enough to drain its socket, so the kernel has nowhere to put the new datagram and the message is dropped. It is a warning, not an error, because the system is wired correctly and may recover once the consumer catches up; it is also not routine teardown, so silently dropping it would hide a real performance problem. The message reports the byte count and points at the two likely causes (a slow subscriber or an undersized `SO_RCVBUF`). The 1s throttle is tighter than `EMSGSIZE` because a backed-up consumer can fail on every publish.

**`ENOENT` / `ECONNREFUSED` (logged as debug).** `ENOENT` means the destination socket file no longer exists; `ECONNREFUSED` means it exists but is no longer bound. Both happen routinely during graceful shutdown. A publisher caches its subscriber socket paths and only re-reads the registry when the generation counter moves, so its cached list can lag one topology change behind reality: a subscriber may have unbound and unlinked its socket while the publisher still holds the path. This is the cache catching up, not a fault, so it is demoted to debug. Logging it as a warning would alarm users at every node teardown for something that is expected and harmless. These two cases are not throttled because they are already silent at the default log level.

**Any other `errno` (logged as warning, 1s throttle).** The default arm catches anything unclassified and logs it at warning level with the raw `errno`, its string form, and the byte count, throttled at 1s. This is the safety net: an unexpected failure is surfaced rather than swallowed, but still rate-limited so a novel persistent failure cannot flood the log.

In all failure cases `send_to` returns `false` and the publisher moves on to the next subscriber. Delivery is best-effort and fire-and-forget, so a failed send to one subscriber never blocks delivery to the rest.

## Discovery: the shared-memory registry

The registry is a single POSIX shared-memory object per ROS domain. It is created with `shm_open(O_CREAT | O_RDWR)` and mapped into each process with `mmap(..., PROT_READ | PROT_WRITE, MAP_SHARED, ...)`. `MAP_SHARED` is the part that matters: every process maps the same physical pages, so a write by one process is immediately visible to all the others with no copying and no message passing. A registry lookup is a plain memory read, and registration is a plain memory write. This is what keeps discovery cheap and bring-up stable: there is no protocol to converge, no traffic to settle, and no separate process whose startup, crash, or restart has to be managed.

On Linux a POSIX shared-memory object is backed by a file under `/dev/shm`, so the registry can be inspected directly. The object name carries the ROS domain id. `registry_open()` formats the name as `/ros2_uds_<domain_id>`, which appears on disk as `/dev/shm/ros2_uds_0` for domain 0, `/dev/shm/ros2_uds_1` for domain 1, and so on. This per-domain naming is how ROS domain isolation is enforced: processes on different domains map different files and therefore can never see each other's endpoints. There is no filtering step; isolation falls out of the file name.

The shm object is **not** unlinked on a clean context shutdown. `rmw_context_fini` only unmaps the region (via `registry_close`); it does not `shm_unlink`. So `/dev/shm/ros2_uds_<domain>` persists for the lifetime of the host and is reused by the next process that comes up on that domain. It is removed only by an incompatible-version reclaim (described below) or manually (`rm /dev/shm/ros2_uds_<id>`). The practical implications: a container gets a fresh `/dev/shm` and therefore a fresh registry, and `/dev/shm` must have room for the fixed region (about 38 MB, page-resident only as slots are touched, see Registry layout). A default `/dev/shm` tmpfs is typically sized at half of RAM, so this is rarely a constraint, but it is charged against that budget.

Because multiple processes may start at the same instant, the creation and initialization handshake is what makes concurrent startup safe; it is described in full under Creation and the init handshake below.

The header itself is small: the layout `version`, the `max_entries` count, a 64-bit `generation` counter that is bumped on every add, remove, and reclaim, and a single robust `pthread_mutex_t`. That mutex exists only for the one-time creation handshake. None of the hot paths (add, remove, query, cleanup) ever take it; they are lock-free and rely on per-slot atomics instead, which is why concurrent discovery activity from many processes does not serialize on a shared lock.

### Registry layout

The registry is one POSIX shared-memory object per ROS domain, mapped `MAP_SHARED` into every process. Its contents are a fixed-size header immediately followed by a flat array of fixed-size slots. There is no dynamic growth, no `mremap`, no free list, and no hash table. The whole region is sized once at creation and never changes shape.

```
  Host (one ROS domain)
  ──────────────────────────────────────────────────────────────────

  /dev/shm/ros2_uds_0          (POSIX shm, mmap'd MAP_SHARED into every process)
  ┌──────────────────────────────────────────────────────────────┐
  │ RegistryHeader                                                 │
  │   version    = 3          (layout tag + init handshake)        │
  │   max_entries = 32768                                          │
  │   generation  = N         (bumped on every add/remove/reclaim) │
  │   init_mutex              (one-time creation handshake only)   │
  ├──────────────────────────────────────────────────────────────┤
  │ slot[0]      RegistryEntrySlot  (1168 bytes)                   │
  │ slot[1]      RegistryEntrySlot                                 │
  │ slot[2]      RegistryEntrySlot                                 │
  │   ...                                                          │
  │ slot[32767]  RegistryEntrySlot                                 │
  └──────────────────────────────────────────────────────────────┘
                         │
                         │  one slot =
                         ▼
  ┌──────────────────────────────────────────────────────────────┐
  │ RegistryEntrySlot                                              │
  │   seq        (seqlock: even = stable, odd = mid-write)         │
  │   state      EMPTY | NODE | PUBLISHER | SUBSCRIPTION |         │
  │              SERVICE | CLIENT   (RESERVED = claimed, uncommitted)│
  │   pid        owning process (liveness check)                   │
  │   gid[16]    RMW GID                                           │
  │   node_name[256]                                               │
  │   node_namespace[256]                                          │
  │   topic_name[256]   (service name for services/clients)        │
  │   type_name[256]    e.g. "pkg/msg/Name"                        │
  │   socket_path[108]  <-- the Unix socket address to connect to  │
  │   qos: reliability, durability, history, depth                 │
  └──────────────────────────────────────────────────────────────┘
```

The header is `RegistryHeader`. It holds four fields: an atomic `version` tag (currently `3`), the slot count `max_entries`, an atomic `generation` counter bumped on every add, remove, and reclaim, and a single `init_mutex`. The first three are integers at fixed offsets (0, 4, 8) guarded by `static_assert`. The `init_mutex` follows at offset 16. Its total size is deliberately not pinned by a `static_assert`, because `sizeof(pthread_mutex_t)` varies by C library and CPU architecture (56 bytes on glibc/x86_64). That mutex is taken only once, during the one-time creation handshake when the shm object is first initialized. None of the hot paths (add, remove, query, cleanup) ever touch it.

After the header comes the slot array. There are `REGISTRY_MAX_ENTRIES = 32768` slots, each a `RegistryEntrySlot` of exactly `1168` bytes (pinned by `static_assert`, so a layout change forces a `REGISTRY_VERSION` bump). That is roughly 38 MB of slot array. The array starts at `sizeof(RegistryHeader)` bytes past the base pointer, so a slot index maps directly to an address with no indirection. A slot carries the entity type, owning PID, GID, node name and namespace, topic or service name, type string, the Unix socket path used to reach the endpoint, and the QoS fields. Each slot also begins with a per-slot seqlock (`seq`) and an atomic `state`, which is what lets the array be scanned without a lock.

The layout is fixed and flat for two reasons.

First, lock-free scanning. Because every slot is the same size and sits at a computable offset, a reader walks the array by simple pointer arithmetic and reads each slot's atomic `state` directly. An empty slot costs a single atomic read before the scan moves on. A live slot is read through its seqlock as a consistent snapshot, with bounded retries if a writer was mid-update, and is otherwise skipped. Readers never block and never take a global lock. A dynamic structure (a hash table, or anything with a free list) would need its own synchronization to stay consistent under concurrent inserts and removals across processes, which is exactly the cost this design avoids.

Second, a shared mapping must contain no pointers. The region is mapped at a different virtual address in each process, so any internal pointer would be meaningless in another process. A flat array sidesteps this entirely: positions are referenced by integer index, which is valid in every mapping. The same property is why dynamic growth is rejected. Growing the region would mean `mremap` and re-pointering in every process at once, an operation with no safe cross-process coordination here. Sizing the region once at creation removes that problem, at the cost of a fixed upper bound on entity count.

### What a slot holds

Every entity in the system claims exactly one slot in the shared-memory registry. That includes each node, but also each publisher, subscription, service server, and service client individually. A node with ten publishers and ten subscriptions occupies twenty-one slots (one for the node plus one for each of the twenty endpoints), not one. This is the slot-accounting rule for the whole registry: the count that fills the fixed array is the number of entities, not the number of nodes.

A slot is the `RegistryEntrySlot` struct mapped into shared memory (`registry.hpp`). It is plain data with no pointers, because pointers would be meaningless across processes that map the region at different addresses. Two fields at the front coordinate concurrent access, and the rest describe the endpoint:

- `seq` (32-bit atomic) — the seqlock counter. Even means the payload is stable; odd means a writer is mid-update.
- `state` (8-bit atomic) — the entity kind and lifecycle marker: `ENTRY_EMPTY`, `ENTRY_NODE`, `ENTRY_PUBLISHER`, `ENTRY_SUBSCRIPTION`, `ENTRY_SERVICE`, `ENTRY_CLIENT`, plus the internal `ENTRY_RESERVED` value a writer sets to claim an empty slot before its payload is filled in.
- `pid` — the owning process ID. This is the liveness handle. Stale-entry cleanup reclaims a slot when `/proc/<pid>` no longer exists.
- `gid[16]` — the RMW GID, the 16-byte unique identity of this endpoint.
- `node_name[256]` and `node_namespace[256]` — which node owns this endpoint, used to answer graph queries.
- `topic_name[256]` — the topic for publishers and subscriptions, or the service name for services and clients.
- `type_name[256]` — the ROS type string in `pkg/msg/Name` form, used to match endpoints by type.
- `socket_path[108]` — the filesystem path of this endpoint's bound `AF_UNIX` socket. The 108-byte size matches the kernel's `sockaddr_un.sun_path` limit, so the path can be copied straight into a socket address without a length check.
- QoS, stored as four small fields: `qos_reliability`, `qos_durability`, `qos_history`, and `qos_depth`. These are kept as compact integers rather than the full `rmw_qos_profile_t` because only these four policies affect behavior on this transport.

The `socket_path` is the field everything else exists to deliver. The wire protocol addresses a datagram to a path, not to a GID or a topic name. So the entire purpose of discovery is to turn a query ("who subscribes to topic `/foo` with a matching type?") into the set of `socket_path` strings a publisher can send to. Every other field in the slot is there to answer that query correctly: `state` and `type_name` and `topic_name` decide whether a slot matches, `pid` decides whether it is still alive, and `socket_path` is the answer that gets returned.

The same data exists in a second form. `RegistryEntry` (also in `registry.hpp`) is the caller-facing copy passed to `registry_add()`. It carries the same payload fields plus a plain `type` field (which the registry copies into the slot's atomic `state`), and omits the `seq`/`state` concurrency machinery the caller has no business touching. This makes `RegistryEntry` 1164 bytes against the slot's 1168. The registry code copies the payload from `RegistryEntry` into the live `RegistryEntrySlot` under the seqlock. Both layouts are pinned with `static_assert`s on their size and field offsets, because the bytes are shared across processes and across separately compiled builds. Any change to the field set is a wire-format change and must bump `REGISTRY_VERSION`.

### Creation and the init handshake

The registry is a single shared-memory object per ROS domain, named `/ros2_uds_<domain_id>`. Any number of processes can start at the same instant and all race to open it. Creation has to produce exactly one initialized header no matter how many processes arrive together, and no process may read a slot before the header is valid. The handshake below guarantees both.

#### First writer wins; everyone else waits

Each process opens the object with `shm_open(name, O_CREAT | O_EXCL | O_RDWR, …)`. The `O_EXCL` flag makes the create atomic: the kernel hands the file to exactly one caller and fails every other caller with `EEXIST`. This is what elects a single creator without any lock being held yet (the lock itself lives inside the not-yet-initialized header, so it cannot be used to coordinate its own creation).

- The winner sees a valid fd, sets `we_created = true`, and grows the empty object to the full layout size with `ftruncate`. It then maps the region and calls `initialize_header`.
- Every loser gets `EEXIST`, reopens the same object with plain `O_RDWR` (no `O_EXCL`), maps it, and waits for the creator to finish.

The creator initializes the header in a fixed order. It zeroes the whole region, writes `max_entries`, resets `generation`, and constructs the process-shared robust `init_mutex`. The version field is written last:

```
header->version.store(REGISTRY_VERSION, std::memory_order_release);
```

#### The version field is the ready signal

The version is published last, with release ordering, so it doubles as a one-time "initialization complete" flag. A late joiner spins on it with acquire ordering:

```
for (int i = 0; i < 10000; ++i) {
  if (header->version.load(std::memory_order_acquire) == REGISTRY_VERSION) { initialized = true; break; }
  sched_yield();
}
```

The release/acquire pair is what makes this safe. Because the waiter reads `REGISTRY_VERSION` with acquire ordering, every write the creator performed before the release store (the zeroing, `max_entries`, the mutex construction) is guaranteed visible to the waiter once it observes the version. So seeing the right version is not just a flag check, it certifies that the entire header is constructed. Until then the field reads as 0 (the object is zero-filled by the kernel on creation, and `ftruncate` of a fresh object yields zeroes), so a waiter that arrives in the small window before the creator's last store simply spins. The spin is bounded and yields the CPU between checks, so it costs nothing meaningful in the normal case where init finishes in microseconds.

This is why `version` sits at offset 0 of the header and why bumping `REGISTRY_VERSION` is the required action whenever the layout changes: it is both the layout tag and the synchronization point.

#### Mismatched or stale layouts are reclaimed, not mapped

A leftover object from an incompatible build is dangerous. If its size is smaller than the current layout, mapping it and then touching the slot array would read past the end of the file and the process would take a `SIGBUS`. Two checks guard against this, both of which `shm_unlink` the bad object and `continue` to retry the create-exclusive path. One retry is enough: after the unlink, the next `O_CREAT | O_EXCL` makes a fresh, correctly sized object.

- **Wrong size.** A process that found an existing object (`EEXIST`) calls `fstat` and compares `st_size` against the expected `compute_registry_size()`. A mismatch means the object was created by a build with a different `REGISTRY_MAX_ENTRIES` or a different slot size, so it is unlinked and recreated.
- **Wrong version tag.** If the spin times out and the version field holds a non-zero value that is neither 0 nor `REGISTRY_VERSION`, the object is the right size by coincidence but carries different semantics. It is treated as stale, unmapped, unlinked, and recreated.

The retry loop runs at most twice (`attempt < 2`) for exactly this reason: a single reclaim-and-recreate resolves any stale object, and a second failure means something is genuinely wrong, so the function returns an error rather than looping forever.

#### If the creator dies mid-init, a waiter takes over

There is a narrow window where the elected creator could crash after `shm_open` but before its final version store. The object would then exist at the right size with `version` stuck at 0, and every waiter would spin forever. The bounded spin handles this. When a waiter exhausts its 10000 iterations and the version is still 0 (not a mismatched non-zero tag, which is the stale-layout case above), it falls through to a best-effort takeover and runs `initialize_header` itself:

```
// Best-effort init: creator crashed in the ~ms window during init.
initialize_header(header, total_size);
```

Re-running `initialize_header` is safe to repeat because it fully reconstructs the header from scratch, and the only state at risk in that tiny window is header bytes that were never validly published. The takeover converges the system to a single initialized registry even when the original creator never finished.

### Registering an entry

Adding an entry to the registry takes no lock. The point of the design is that a node coming up should never block on, or be blocked by, any other process touching the table. A single atomic compare-and-swap claims a slot, and a per-slot seqlock guards the payload while it is written. Readers never wait on a writer, and writers never wait on each other beyond the one CAS that decides who owns a slot.

The sequence is a scan, a claim, a fill, and a publish.

**Scan for an empty slot.** `try_add_once` walks the slot array from index 0. Each slot's `state` byte is the gate. A slot with `state == ENTRY_EMPTY` is a candidate. Because the table is a fixed flat array with no free list, the scan is just a linear loop over `max_entries`.

**Claim it with one CAS.** When the scan finds a candidate, it attempts to flip the slot's `state` from `ENTRY_EMPTY` to `ENTRY_RESERVED` with a single `compare_exchange_strong`. Exactly one writer can win this CAS on a given slot; any concurrent writer that also saw the slot as empty loses the exchange and moves on to the next slot. The winner now owns the slot.

`ENTRY_RESERVED` is the reason a half-written entry is never observed. It is a transient marker that lives only in the atomic `state` field. Readers (`snapshot_slot`) treat `ENTRY_RESERVED` exactly like `ENTRY_EMPTY` and skip the slot. So between the moment a writer claims the slot and the moment it publishes the real entity type, no reader will copy out the slot's payload, even though the payload is being filled in concurrently.

**Fill the payload under a seqlock.** Once the writer owns the slot, `write_slot_payload` writes the entry fields (pid, gid, names, socket path, QoS) using a seqlock. A seqlock is the right tool here because the payload is larger than any single atomic and readers must be able to detect a torn read (seeing half of one write and half of the next) without taking a lock:

1. `seq.fetch_add(1)` bumps the counter from even to **odd**, marking a write in progress.
2. The payload fields are copied into the slot.
3. `seq.fetch_add(1)` bumps the counter from odd back to **even**, marking the snapshot stable.

A reader records `seq` before and after copying the payload. If it read an odd value, or the two reads differ, it knows a writer touched the slot mid-read and retries (bounded) or skips. This is the seqlock protocol the whole registry relies on; the reader side and its retry bound are described under Looking up entries.

**Publish the real state.** With the payload committed (seq even), the writer stores the actual entity type into `state` with release ordering:

```cpp
slots[i].state.store(static_cast<uint8_t>(entry.type), std::memory_order_release);
```

The release store pairs with the acquire load every reader does on `state`. Any reader that now observes a real type (`ENTRY_NODE`, `ENTRY_PUBLISHER`, and so on) is guaranteed to also see the fully-written payload that was published before it. This store is the single point at which the slot becomes visible to discovery. Until it happens, the slot reads as `ENTRY_RESERVED` and is invisible.

**Bump the generation counter.** Finally the writer does `header->generation.fetch_add(1)`. The generation counter is the table's change signal. Publishers, services, and clients cache their lookup results and re-scan only when the generation moves, so bumping it here tells every cached reader that the graph changed and its cache is stale. This is what turns a new registration into a graph event without any push notification or daemon.

**The slot index is remembered, so removal needs no scan.** `registry_add` returns the slot index, and the owning entity stores it. Removal (`registry_remove`) indexes straight to that slot and CASes its `state` back to `ENTRY_EMPTY`. There is no second scan to find the entry on the way out, which keeps teardown cheap and makes removal symmetric with the single-CAS claim used on the way in.

**Failure path.** If the scan finds no empty slot, `registry_add` runs the stale-cleanup pass once (reclaiming slots whose owning PID is dead) and retries the scan a single time. If the table is still full after that, the add fails and returns -1. There is no resize and no spinning.

### Looking up entries

A lookup is a linear scan over the whole slot array. It returns every slot whose contents match the filters the caller passes: entity type, topic name, node name, and node namespace. Any filter left null matches everything. The scan is the only way to find endpoints because the registry has no index; it is a flat array with a header, chosen for its simplicity over a hash table or free list.

Most slots in a large table are empty, so the scan keeps the empty case as cheap as possible. Before doing anything else it reads the slot's `state` with a single atomic acquire load. If that value is `ENTRY_EMPTY`, the slot holds nothing and the scan moves on immediately. This fast pre-check means an empty slot costs exactly one atomic read.

A non-empty slot is read through a seqlock snapshot, using the protocol described under Registering an entry: the reader loads `seq`, skips the slot if it is odd (a write is in progress), otherwise records the even value, copies the non-atomic payload fields (PID, GID, names, socket path, QoS), then loads `seq` a second time. If the two reads match, no write happened in between and the snapshot is consistent. If they differ, the copy was torn by a concurrent writer and the reader discards it.

A torn read is retried, but only a bounded number of times (`MAX_READ_RETRIES`, 16). A writer that keeps the slot perpetually odd would otherwise spin a reader forever; bounding the retries means a slot that cannot be read cleanly right now is simply skipped for this query rather than blocking the whole scan. Skipping is safe because the registry is queried often and a missed entry will be seen on the next lookup once the writer is done. A slot in the `ENTRY_RESERVED` state is treated like an empty one and skipped for the same reason: it is claimed but not yet published.

Once a snapshot succeeds, the scan applies the filters against the snapshot copy, not the live slot, so the comparison sees a stable view. The type filter, then the topic, node name, and namespace string comparisons run in turn, and any mismatch skips the slot. Surviving slots are converted into a result holding the node name, namespace, topic, type, socket path, GID, and QoS, and appended to the returned vector. The socket path is the part callers actually want: it is the address a publisher sends to.

Readers never take a lock. The seqlock is a one-sided protocol where only writers mutate the counter and readers merely observe it, so any number of processes can scan the registry concurrently without contending on a mutex and without blocking each other or a writer. This is what makes discovery cheap enough to run on every topology change, while the generation counter (covered next) keeps it off the per-message send path.

### The generation counter and the subscriber cache

A registry lookup is a linear scan of the slot array. Running that scan on every publish would be far too costly for the data path: the loop bound is the fixed table size, and on a busy topic a publisher may call `rmw_publish` thousands of times per second. The scan would also have to coordinate with concurrent writers, which is exactly the work the hot path should avoid.

The registry header carries one `generation` counter, an atomic that is incremented on every successful add, remove, and stale-slot reclaim. It is the single source of truth for "did the graph change." Each publisher uses it to decide whether its view of the subscribers is still valid.

A publisher keeps two pieces of cached state alongside its own data: the list of subscriber socket paths it found the last time it scanned, and the `generation` value that was current at that scan. On publish it does the following:

1. Read the current generation with one atomic load (`registry_generation()`, an acquire load of the header field).
2. Compare it to the cached generation. If they match, the graph has not changed since the last scan, so the cached paths are still correct. The publisher copies the cached list and sends to each path without ever calling `registry_query` or touching the registry.
3. If they differ, something joined or left. The publisher re-scans the registry for matching subscriptions, rebuilds its path list, and stores the new generation. The next publish will hit the fast path again.

The result is that the expensive scan happens once per topology change, not once per message. In steady state, where the graph is not changing, the per-publish discovery cost is a single atomic read followed by a local copy of an already-built vector.

The cache is protected by a small per-publisher mutex (`sub_cache_mutex`), not the registry lock. This mutex is private to one publisher and is held only long enough to check the generation, optionally refresh, and copy the path list out. The actual `sendmsg` calls happen after the mutex is released, so a slow send never blocks the cache and the cache never blocks the registry.

One consequence is that the cache can be a single topology change behind reality for a brief window: a subscriber may have torn down its socket while the publisher still holds the old generation and the stale path. The send to the gone path then fails (`ENOENT`/`ECONNREFUSED`), which is expected during shutdown and demoted to debug; see Send failure handling. The next publish after the generation bump rebuilds the list and the stale path disappears.

Services and clients cache the same way against the same generation counter; the request and reply routing that uses those caches is covered under Services.

### Cleaning up dead and crashed entries

Two failure modes leave entries behind: a clean shutdown, and a process that dies without running its destructors. They are handled separately because only one of them can be trusted to tell the truth about itself.

**Clean shutdown.** When an endpoint is destroyed normally, it removes its own slot. Because each entity records the slot index it was assigned at registration, removal needs no scan: it indexes straight to its slot. `registry_remove` reads the slot's current `state`, then attempts a single compare-and-swap from that state value to `ENTRY_EMPTY`. The CAS is what makes the removal safe under concurrency: exactly one caller can win it, and the winner owns the teardown. If the CAS fails, someone else already emptied the slot, so the function returns without touching anything.

The teardown itself runs under the slot's seqlock (the same odd/even protocol used on the write side). `teardown_slot` bumps `seq` to odd, copies out the socket path, zeroes every payload field, then bumps `seq` back to even. Any reader that was mid-snapshot sees the odd `seq` and retries, so it can never read a half-erased slot. The socket file is unlinked after the seqlock closes, because a filesystem operation does not need to be observable atomically by other processes. Finally `generation` is incremented so every cached reader learns the graph changed. The order matters: `state` flips to `ENTRY_EMPTY` first (no new reader can match the slot), the payload is wiped under the seqlock, and only then does `generation` move.

**Crash.** A process killed by SIGKILL, OOM, or a container teardown never reaches `registry_remove`. Its slot keeps a stale socket path that no longer accepts connections. There is no destructor to trust, so the registry infers death from the operating system instead. `registry_cleanup_stale` walks the table, and for each live slot it takes a seqlock snapshot and checks the owning PID by stat-ing `/proc/<pid>`. The decision is deliberately conservative:

- `/proc/<pid>` is gone (`stat` returns `ENOENT`): the owner has exited, so the slot is reclaimable.
- `/proc/<pid>` exists: the owner is alive, so the slot is kept.
- Any other `stat` error (ambiguous): the slot is kept, treated as alive.

The bias toward keeping is intentional. The cost of wrongly keeping a dead slot is one wasted slot until the next pass; the cost of wrongly reclaiming a live slot is tearing down a working endpoint. A `pid` of 0 is also never reclaimed, since that value only appears in the transient snapshot of a slot whose payload has not been written yet, never in a committed entry.

The dangerous window is between vetting a slot and tearing it down. The owner could exit, the slot could be reclaimed by a clean remove, and a different process could re-add an entry of the same type into the same slot, all before this pass acts. This is the classic ABA problem: the slot looks identical but is a different entry, and the `state` value alone cannot tell them apart. The seqlock counter closes the gap. The snapshot captures the slot's `seq` (`snap_seq`), and immediately before the reclaim CAS the pass re-reads `seq` and compares. Any intervening write (a remove or a re-add both go through the seqlock) advances the counter, so a mismatch means the slot is no longer the entry that was checked, and the pass skips it. Only when `seq` is unchanged does it CAS the observed `state` to `ENTRY_EMPTY`, log a one-line breadcrumb naming the slot, PID, node, and topic so an ungraceful exit is visible in the ROS log, run the same `teardown_slot`, and bump `generation`.

**When it runs.** There are two triggers.

The first is eager and runs once at every context startup. Before a new process opens its sockets, `rmw_init` runs `registry_cleanup_stale()` and then `cleanup_orphan_socket_files()` on the domain. The two are paired because an ungraceful exit leaves two distinct kinds of garbage: a dead registry slot, and a dangling socket file under `/tmp/ros2_uds/<domain>/`. The registry sweep reclaims slots whose PID is gone; the file sweep parses the PID out of each socket file name and unlinks files whose owner is no longer alive. Running both at every startup means a process joining the domain cleans up after crashed predecessors, which is what keeps an uncleaned shutdown from permanently leaking either resource even if the table never fills.

The second is lazy and runs only when `registry_add` scans the whole table and finds no empty slot: it calls `registry_cleanup_stale` once and retries the add a single time. If the table is still full after reclaiming, the add fails. This second path is what bounds the worst case where the table actually fills during a long-running session, paying the linear sweep only at the moment a slot is genuinely needed.

### Scaling characteristics

Every registry walk iterates over the fixed table size, `REGISTRY_MAX_ENTRIES` (32768 slots), not over the number of entries currently in use. A discovery scan (`registry_query`), a worst-case add (`try_add_once`), and the stale-cleanup pass (`registry_cleanup_stale`) all loop from slot 0 to `max_entries` regardless of whether the system holds 5 endpoints or 5000. The loop bound is a compile-time constant, so these operations are O(table size), not O(live count). The per-empty-slot work is kept to a single atomic load, so a sparsely-populated table is still scanned quickly.

On the data path the scan is not paid per message; the generation counter keeps it off the send path (see The generation counter and the subscriber cache). In steady state, sending a message costs one atomic read, and each endpoint pays for a full scan only once per topology change.

The fixed table is the thing to watch as a system grows. Because every entity (not just every node) consumes one slot, a system with well over a hundred nodes and many endpoints each can occupy a meaningful fraction of the 32768 slots, and the cost of the add-time scan and the cleanup pass is tied to that fixed table size rather than to the live count. The design works at the 200-node target, but how close the live entity count comes to filling the table is the scaling parameter to monitor.

## Locking

Almost nothing in this RMW takes a lock. The discovery registry, which every process reads and writes, is coordinated entirely with lock-free atomics: a compare-and-swap (CAS) to claim a slot, and a per-slot seqlock to publish and read its payload. The single `pthread_mutex_t` in the system, `init_mutex` in the registry header, exists only for the one-time creation handshake of the shared region and is never touched on any data or discovery path.

### Why the hot paths are lock-free

The registry lives in shared memory mapped into every process on the host. A cross-process lock taken on a hot path is dangerous in a way an in-process lock is not: if the lock holder is killed (SIGKILL, OOM, container stop) while holding it, every other process that needs the lock blocks forever. With well over a hundred nodes mapping the same region, one unlucky crash would freeze discovery for the entire system. The hot path must never be able to block one process behind another across a process boundary.

So the hot paths avoid locks entirely. They rely on the fact that `std::atomic<uint8_t>`, `<uint32_t>`, and `<uint64_t>` are lock-free and work correctly no matter which address they live at (the C++ standard calls this "address-free"). Address-freedom matters because the same atomic is accessed through different virtual addresses in different processes, and an atomic that depended on its address could not be shared this way. Static asserts at the top of `registry.cpp` make both properties a compile-time requirement, so the build fails loudly on any platform where they do not hold.

Three mechanisms replace the lock:

- **Claiming a slot is a single CAS.** To add an entry, a writer scans for an empty slot and flips its `state` from `ENTRY_EMPTY` to `ENTRY_RESERVED` with one atomic compare-and-swap. Exactly one writer wins. `ENTRY_RESERVED` is a private "claimed but not yet filled in" marker (see Registering an entry for the full read/write protocol). Removal is the mirror image: a CAS of `state` back to `ENTRY_EMPTY` decides who owns the teardown.

- **Publishing and reading a slot is a seqlock.** A writer brackets the payload copy with an odd/even `seq` counter; a reader snapshots `seq` before and after the copy and retries on mismatch. The full protocol is described in Registering an entry. The point for locking is that readers never block a writer and writers never block a reader, and the bounded retry (`MAX_READ_RETRIES`) guarantees a persistent writer cannot spin a reader forever.

- **The send path reads one atomic.** Publishers, clients, and services cache the socket paths they discovered along with the `generation` they saw, and on each send compare a single atomic load of `generation` against that cached value (see The generation counter and the subscriber cache). The expensive linear scan happens only when the graph actually changes.

The cost of being lock-free is that readers can observe a torn or stale view and must cope with it rather than exclude it: snapshots are validated by the seqlock re-read, the stale-slot reclaim re-checks the captured `seq` before tearing a slot down (an ABA guard against a remove-and-re-add in the window), and a cached socket path may briefly outlive the subscriber it points to. That last case produces routine send failures during shutdown, which the transport demotes to debug level (see Send failure handling). These are accepted as the price of never holding a cross-process lock on a path that runs per message.

### Why the one lock is a robust mutex

The single mutex guards the moment the shared region is brought into existence. When several processes start at once, exactly one wins the `O_CREAT | O_EXCL` creation and is responsible for initializing the header, including initializing this very mutex. The mutex is created with two non-default attributes:

- `PTHREAD_PROCESS_SHARED`, because it lives in the mapped region and must be meaningful to every process, not just the one that created it. A default mutex is only valid within the process that initialized it.
- `PTHREAD_MUTEX_ROBUST`, because the holder is a separate process that can die at any instant. A robust mutex lets the kernel hand the next acquirer an `EOWNERDEAD` return instead of an unrecoverable deadlock. That process can then call `pthread_mutex_consistent()` to take ownership and repair whatever the dead holder left half-done. Without the robust attribute, a process killed while holding the mutex during initialization would wedge every later process that tried to lock it, and the registry could never be opened again until the shm file was manually removed.

The creation handshake itself relies primarily on the lock-free `version` signal rather than the mutex; that handshake, including takeover when the creator dies mid-init, is described in full under Creation and the init handshake. The robust mutex backs it up so that any path which does acquire the mutex can detect and recover from a holder that died mid-init, rather than inherit a corrupt or permanently locked region. It is a recovery guarantee for a rare one-time event, not a serialization point on any path that runs while the system is up.

## Serialization

Messages are serialized with eProsima fastCDR, driven by the per-type callbacks generated by `rosidl_typesupport_fastrtps_cpp` (with `rosidl_typesupport_fastrtps_c` as a fallback). This is the same CDR encoding that the DDS-based RMW implementations use (`rmw_fastrtps_cpp`, `rmw_connextdds`) and that `rmw_zenoh_cpp` reuses. The consequence is that serialization is not the variable under test in any cross-RMW comparison: every implementation puts the identical bytes on the wire through the identical generated code, so a benchmark measures the discovery and transport design, not the encoder.

The choice also avoids a known failure mode. An earlier version of this RMW walked message fields at runtime with `rosidl_typesupport_introspection_cpp`. That approach broke on the C-versus-C++ introspection ABI split (different `MessageMember` layouts, `std::string` versus `rosidl_runtime_c__String`, `std::vector<bool>` as a bitfield) and corrupted memory on nested production message types. The fastCDR callbacks are compiled per message type, so there is no runtime field walking and no introspection-variant ambiguity to get wrong.

### How it is wired

The encoder is a thin wrapper, not a reimplementation. Two helpers in `serialization.cpp` resolve the type support handle to its fastCDR callbacks: `get_callbacks()` for messages and `get_service_callbacks()` for service request/response pairs. Each tries the C++ identifier first, then falls back to the C identifier, calling `get_message_typesupport_handle()` to pull the correct `message_type_support_callbacks_t` out of the dispatch layer. For services it resolves the request and response member callbacks through the same handle lookup rather than trusting `srv_cb->request_members_` directly, because that field can hold a dispatch trampoline (an indirection stub that forwards to the real callbacks in another typesupport variant) rather than the callbacks themselves.

Resolution happens once, at endpoint creation, not per message. `rmw_create_publisher` and `rmw_create_subscription` call `get_callbacks()` and store the result on the publisher's and subscription's internal data (`callbacks` in `types.hpp`). The hot path then just uses the cached pointer.

Serialization itself is two functions over the cached callbacks:

- `serialize()` asks `callbacks->get_serialized_size()` for the body size, reserves four bytes for the CDR encapsulation header plus that body, constructs a fastCDR `Cdr` in `DDS_CDR` mode, writes the encapsulation, then calls `callbacks->cdr_serialize()`. The buffer is finally trimmed to `get_serialized_data_length()`. On publish, `rmw_publish` calls this once per message to produce the CDR payload, which the transport then sends to each subscriber.
- `deserialize()` is the reverse: it wraps the received bytes in a `FastBuffer`, reads the encapsulation, and calls `callbacks->cdr_deserialize()` into the destination ROS message. `rmw_take` and its variants call this on the subscription side.

Both functions catch every exception. `get_serialized_size()` can throw on string or sequence overflow, the buffer allocation can throw `bad_alloc`, and `cdr_deserialize()` resizes message strings and sequences from wire-supplied lengths, so a malformed or hostile datagram can throw `bad_alloc` or `length_error`. The RMW entry points are `extern "C"`, so nothing may propagate across the C ABI; a thrown exception is converted into a `false` return and surfaced as `RMW_RET_ERROR`.

The public `rmw_serialize()` / `rmw_deserialize()` API (used by rosbag and by serialized-message publish/take) calls the same two helpers, so the byte format is identical whether a message is sent normally or handled as a pre-serialized buffer.

## Wait set and event delivery

The ROS 2 executor finds out that work is ready by calling `rmw_wait()`. It hands in the subscriptions, services, clients, guard conditions, and events it cares about, plus an optional timeout, and the call blocks until at least one of them is ready or the timeout expires. This RMW implements that blocking entirely with one `epoll` instance per wait set, and it does so without any background threads.

### One epoll instance per wait set

`rmw_create_wait_set()` allocates a `UdsWaitSet` and creates a single epoll file descriptor (fd) with `epoll_create1(EPOLL_CLOEXEC)`. That fd is the only kernel object the wait set owns. Everything the executor waits on is reduced to a file descriptor and watched through this one epoll instance:

- A subscription, service, or client waits on its bound `AF_UNIX` receive socket fd. The socket becomes readable when a datagram arrives.
- A guard condition waits on an `eventfd`. `eventfd` is the lightest signaling primitive Linux offers: an 8-byte kernel counter that integrates directly with epoll, with no pipe pair and no second fd. `rmw_trigger_guard_condition()` writes `1` to it, which makes the fd readable and wakes any blocked `epoll_wait()`.

`epoll` is chosen over `poll`/`select` because its cost does not grow with the number of watched fds. A system with hundreds of nodes can register thousands of fds in a single wait set, and `epoll_wait()` still returns only the fds that are actually ready. `poll`/`select` would re-scan the entire fd set on every call.

### What one rmw_wait call does

Each `rmw_wait()` runs the same sequence on the calling thread:

1. **Drain first.** Every subscription, service, and client socket is drained into a per-entity message queue before anything blocks. The receive sockets are `SOCK_DGRAM | SOCK_NONBLOCK`, so the drain loop calls `recv_from` repeatedly and stops cleanly on `EAGAIN` (nothing left to read). This step exists because data may have arrived between the previous `rmw_wait()` and this one; draining up front means such data is not missed.
2. **Check the graph.** The shared-memory registry holds a `generation` counter that is bumped whenever an endpoint is added or removed. The wait reads it once and compares it against the value cached on the context. If it moved, the graph changed, and the context's graph guard condition is triggered. This is a single atomic load from shared memory, which is why a daemon or cross-process push notification is not needed (see the graph guard condition design choice).
3. **Arm the fds.** Every entity fd and guard-condition eventfd is added to the epoll instance with `EPOLL_CTL_ADD`. This is idempotent across calls: a still-live fd returns `EEXIST` and is treated as already armed, and a fd number reused after its previous owner closed gets freshly armed. The kernel removes closed fds from an epoll set automatically, so there is no matching `EPOLL_CTL_DEL` and no per-call teardown.
4. **Check ready, then block only if needed.** If any queue already holds a message, or any guard-condition eventfd reads as triggered, the call skips blocking entirely and reports what is ready. Only when nothing is ready does it call `epoll_wait()` with the computed timeout. The thread is asleep in the kernel here, not spinning. `epoll_wait()` is retried on `EINTR` against a steady-clock deadline so a signal neither returns a false timeout nor busy-loops.
5. **Drain again and report.** After waking, the sockets are drained once more, then the output arrays are pruned: entities with no pending data are set to `NULL`, and the ones that are ready are left in place for the executor to service. If nothing became ready, the call returns `RMW_RET_TIMEOUT`.

### Which guard condition the graph change wakes

There are two graph guard conditions in play, and the path between them matters. Each node creates its own guard condition in `rmw_create_node()` and stores it on the node (`UdsNode::graph_guard_condition`). `rmw_node_get_graph_guard_condition()` hands rcl that per-node object, so the per-node guard condition is what the executor's wait set actually watches for graph changes.

The trigger in step 2 above, however, fires `ctx->graph_guard_condition` (the context-level guard condition), not the per-node one. In the current code these are separate objects, and `UdsContext::graph_guard_condition` is never assigned, so the trigger does not reach the guard condition rcl is waiting on. A graph change is still observed, because every `rmw_wait()` re-reads the registry generation in step 2 and re-drains regardless of guard-condition state, so a wait already in progress or the next wait call picks up the change. The separate context-level trigger is therefore redundant rather than load-bearing. This is a wiring gap worth confirming against intent: if the context guard condition is meant to wake blocked waits on a graph change, it would need to be the node's guard condition (or be linked to it), and it would need to be assigned.

### In-process concurrency

All socket I/O happens inside `rmw_wait()`, but the per-entity data structures are still guarded so a `MultiThreadedExecutor` can call into the RMW from several threads at once. Each subscription, service, and client guards its own message queue with a per-entity `queue_mutex`, each publisher guards its subscriber cache (`sub_cache_mutex`) and its TRANSIENT_LOCAL cache (`cache_mutex`), services and clients guard their caches (`svc_cache_mutex`, `client_cache_mutex`), and each endpoint guards its user-callback pointer with a `callback_mutex`. The locking is per-entity, so concurrent `rmw_take()` and `rmw_publish()` on different entities do not contend; the supported granularity is one thread per entity, not one global lock.

The one shared resource on the send side is the single per-process send socket (one unbound `AF_UNIX` datagram socket shared by every publisher and client in the process). Concurrent `sendmsg()` calls on it are relied upon to be atomic at the datagram level, which the kernel guarantees for `SOCK_DGRAM`: each datagram is written whole, so interleaved senders never produce a torn datagram.

### No background threads, and why

There are no receiver threads draining sockets in the background, no timer threads, and no dispatch threads. All socket I/O happens inside `rmw_wait()`, on the executor thread that called it. This is a deliberate choice.

Background receiver threads would each need a lock-protected handoff queue between themselves and the executor, which adds contention on the hot path and a set of threads to create, shut down, and reason about under crashes. The ROS 2 executor model already calls `rmw_wait()` in a tight loop and then services whatever it reports, so draining the sockets at exactly that point is both natural and sufficient. The kernel does the actual waiting in `epoll_wait()`, so a "no threads" design does not mean polling in user space; it means the only thread involved is the one the executor already runs, and it sleeps in the kernel until there is work.

The consequence to understand is that **delivery only happens while a thread is inside `rmw_wait()`.** A datagram sitting in a socket buffer is not moved into a subscription's queue until the next wait call drains it. In normal operation the executor is almost always in or returning to `rmw_wait()`, so this is invisible. But it means there is no asynchronous delivery: if an application thread blocks the executor and never returns to the wait loop, incoming messages accumulate in kernel socket buffers and are not processed until the executor comes back. The socket buffer is the only backlog that builds up in the meantime, bounded by the kernel's receive buffer size for that socket.

## QoS handling

ROS 2 lets every publisher and subscription declare a QoS profile: reliability, durability, history, depth, and the timing policies (deadline, lifespan, liveliness). The RMW is free to honor a policy, reject an incompatible request, or accept it without enforcing it. This implementation honors the policies that map cleanly onto a copy transport over Unix domain sockets and accepts the rest without acting on them. Accepting-but-not-enforcing rather than rejecting keeps the system plug-and-play: a node that requests an unsupported policy still comes up and communicates, instead of failing to match.

### Normalization at creation

Both `rmw_create_publisher` and `rmw_create_subscription` pass the requested profile through a small `resolve_qos()` step that turns the "system default" and "best available" sentinels into concrete values, so the rest of the code never has to special-case them:

- `history` SYSTEM_DEFAULT becomes KEEP_LAST.
- `depth` of 0 becomes 10.
- `reliability` SYSTEM_DEFAULT or BEST_AVAILABLE becomes RELIABLE.
- `durability` SYSTEM_DEFAULT or BEST_AVAILABLE becomes VOLATILE.

The resolved profile is stored on the endpoint and copied into its registry slot, so other processes can read each endpoint's QoS during discovery. `rmw_publisher_get_actual_qos` and `rmw_subscription_get_actual_qos` return this resolved profile.

### History and depth: enforced

`history = KEEP_LAST` with a bounded `depth` is the only history mode that is actually implemented, and it is enforced on the subscriber side. Incoming datagrams are drained into a per-subscription queue; after each push the queue is trimmed to `depth`, dropping the oldest messages first. An overflow at this point means the application is not calling `take()` fast enough, and it is logged (throttled) as a slow-consumer warning.

`history = KEEP_ALL` is accepted but not honored. There is no unbounded queue. A subscription that requests KEEP_ALL gets KEEP_LAST behavior at whatever depth it resolved to (10 if it asked for the default).

### Durability: enforced

`durability = VOLATILE` is the straightforward case: the publisher does not retain anything, so a subscription that joins after a message was published never sees it.

`durability = TRANSIENT_LOCAL` is implemented on the publisher side. A TRANSIENT_LOCAL publisher keeps its last `depth` messages in a local cache, trimmed the same way the subscriber queue is. When a new subscriber appears (detected because the registry generation counter moved), the publisher replays the cached messages to it. This is the one place `depth` controls a publisher-side structure rather than a subscriber-side one.

### Reliability: accepted, not differentiated

`RELIABLE` and `BEST_EFFORT` are accepted and recorded, but the transport treats them identically. There is no separate acknowledgment-and-retransmit path for RELIABLE. The reason is the nature of the medium. On localhost there is no network in between, so the kernel does not drop `AF_UNIX` datagrams in transit. Once a `sendmsg()` succeeds, the message is sitting in the receiver's socket buffer and will be delivered. In steady state, with a consumer that keeps up, delivery is lossless for both policies, which is why no distinct RELIABLE machinery is needed.

The qualifier is the send side, where a slow consumer can cause a drop that the reliability policy does not prevent. As described in Send failure handling, sends are non-blocking, so a full receive buffer makes the next `sendmsg()` fail and the publisher logs the drop and moves on rather than blocking or retrying. This happens regardless of whether the subscription asked for RELIABLE or BEST_EFFORT. The two policies are the same here: localhost delivery is lossless in transit but bounded by how fast the consumer drains, and a consumer that falls behind drops messages either way.

A second, independent drop point exists even after a datagram is safely in the socket buffer: the per-subscription KEEP_LAST queue described above trims to `depth` when `take()` lags. So "slow consumer drops messages" can mean either a full kernel receive buffer (send fails) or a full application-level queue (oldest popped). Neither is affected by the reliability policy.

### Timing policies: accepted, not enforced

`deadline`, `lifespan`, `liveliness`, and `liveliness_lease_duration` are accepted at endpoint creation and stored, but nothing acts on them. Each would require machinery this RMW deliberately does not have: deadline and liveliness need a periodic timer to detect missed intervals or lapsed heartbeats, and lifespan needs a per-message expiration check. Because there are no background threads (see No background threads, and why), there is nothing to run those checks, and no QoS-event is ever generated for a missed deadline or lost liveliness. A node may request any of these and will come up normally; the values simply have no effect.

### Compatibility checking

`rmw_qos_profile_check_compatible` reports two mismatches as `RMW_QOS_COMPATIBILITY_WARNING`, never as an error:

- A BEST_EFFORT publisher paired with a RELIABLE subscriber. On this RMW the two behave identically, but the pairing signals a likely intent mismatch, so it is surfaced.
- A VOLATILE publisher paired with a TRANSIENT_LOCAL subscriber. The subscriber expects late-joining replay that a VOLATILE publisher does not provide, so it will never receive past messages.

Both checks append to the `reason` buffer rather than overwrite, so when both mismatches are present both reasons are reported. Every other combination returns `RMW_QOS_COMPATIBILITY_OK`. Nothing is reported as incompatible, which keeps matching permissive by design.

### Summary

| Policy | Status |
|---|---|
| `history` = KEEP_LAST | Enforced (subscription queue trimmed to `depth`) |
| `history` = KEEP_ALL | Accepted, not enforced (treated as KEEP_LAST) |
| `depth` | Enforced (subscription queue and TRANSIENT_LOCAL cache size) |
| `durability` = VOLATILE | Enforced (no caching, no replay) |
| `durability` = TRANSIENT_LOCAL | Enforced (publisher caches last `depth`, replays to late joiners) |
| `reliability` = RELIABLE | Accepted, not differentiated from BEST_EFFORT |
| `reliability` = BEST_EFFORT | Accepted, not differentiated from RELIABLE |
| `deadline` | Accepted, not enforced |
| `lifespan` | Accepted, not enforced |
| `liveliness` / `liveliness_lease_duration` | Accepted, not enforced |

## Services, the graph, and GIDs

### Services and clients

Request/response works the same way as topics. A service server and a service client are each just another kind of registry entry with their own receive socket. `rmw_create_service` and `rmw_create_client` follow an identical sequence: generate a GID, bind a `SOCK_DGRAM` socket under `/tmp/ros2_uds/<domain>/` (prefix `srv` for servers, `cli` for clients), then add one slot to the shared-memory registry. The slot's type is `ENTRY_SERVICE` or `ENTRY_CLIENT`, its `topic_name` field holds the service name, and its `socket_path` field holds the address peers send to. Discovery for services is therefore the same registry scan used for pub/sub, filtered by entry type and service name.

The data path uses the one transport for everything. A client serializes the request, builds the wire header with `msg_type = 1` (request), and sends one datagram to the server's socket. The server serializes its reply, builds a header with `msg_type = 2` (response), and sends it back to the client's socket. The `msg_type` byte lets each side ignore datagrams meant for the other direction: `rmw_take_request` skips anything that is not a request, `rmw_take_response` skips anything that is not a response.

Routing a reply back to the right client is the only part that needs a lookup, and it reuses the same generation-keyed cache the publisher uses for subscribers. The reason is identical: re-scanning the registry on every reply would take the registry lock in the hot path. So a server caches the list of its clients as `(GID, socket_path)` pairs keyed on the registry generation counter, and a client caches the single server socket path the same way. When the generation is unchanged, the cached list is used and the lock is never taken. The request carries the client's GID in its wire header, and `rmw_send_response` matches that GID against the cached client list to pick the return socket.

Request and response are correlated by sequence number, and that correlation is left to the rcl layer rather than done in the RMW. The client stamps a per-client monotonic `sequence_number` into each request header (`cli_data->sequence_number.fetch_add(1)`). The server echoes both the client's GID and that same `sequence_number` back in the response header. `rmw_take_response` then returns queued responses first-in-first-out and copies `(writer_guid, sequence_number)` out of the wire header into `rmw_request_id_t`, so rcl can match a response to the request it issued. The RMW itself does no sequence-number matching and no per-request demultiplexing; with several requests in flight, it hands every response up in arrival order and rcl pairs them.

### The graph guard condition

ROS 2 lets a node block until the graph changes (a node, publisher, subscription, service, or client appears or disappears). The mechanism is a graph guard condition: rcl adds it to a wait set, and the RMW arranges for that wait set to wake when the graph moves. This RMW detects graph changes by polling, not by cross-process push, because pushing a notification between processes without a daemon would need per-process signals or pipes, which is exactly the complexity the design avoids.

The signal source is the registry's `generation` counter. Every successful add, remove, and stale-slot reclaim does `generation.fetch_add(1)` after publishing its slot change. A single monotonic counter in shared memory is enough to mean "something in the graph changed" without saying what.

Each node owns its own graph guard condition, created at `rmw_node_create` as an `eventfd` (see the wait section) and stored on the node. `rmw_node_get_graph_guard_condition` hands that per-node guard condition to rcl, so the object rcl waits on is the node's. On every `rmw_wait` the context reads the current `generation` and compares it to the value it cached on the previous call (`UdsContext::last_registry_generation`). When the counter has moved, the context records the new value, so the change is observed exactly once. Because rcl calls `rmw_wait` continuously while a node is spinning, a graph change is observed within one wait cycle, and the cost of the check is a single atomic load from shared memory.

There is a known seam here, the same one described under the wait set. The per-node guard condition is what rcl receives and waits on, but the trigger call in `rmw_wait` targets `UdsContext::graph_guard_condition`, a separate field that is never assigned and so is always null. The detection of the generation change is correct and the cached generation is advanced, but the explicit eventfd write that would wake a node blocked on the graph guard alone does not currently fire through that field. In practice the wait set is driven by its other fds and the per-cycle generation check, so graph queries observe the change; wiring the trigger to the per-node guard condition rcl actually holds is a correctness gap worth closing.

### GID generation

Every entity needs a globally unique identifier in the RMW GID format, which is a fixed `RMW_GID_STORAGE_SIZE` (16) byte blob. `UdsGid::generate()` produces one without any UUID library:

- bytes 0–3: the process PID (`getpid()`)
- bytes 4–7: a per-process atomic counter (`g_gid_counter`, incremented once per entity)
- bytes 8–15: zero

This is unique on a single host by construction. Two different processes have different PIDs, and within one process the monotonic counter guarantees distinct GIDs across all publishers, subscriptions, services, and clients. Sixteen bytes fits the RMW storage size exactly, and the scheme has no external dependency. The GID is what the request/response routing matches on: the client stamps its GID into every request header, and the server matches that GID to choose the reply socket.

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

## Resource usage, limitations, and build

### Resource profile

The implementation runs no daemon and starts no background threads (see No background threads, and why). When no ROS 2 process is using a domain, nothing belonging to this RMW is running. All discovery state lives in a memory-mapped file, and all message I/O happens inside `rmw_wait()` on the executor's own thread. There is no process to supervise, no startup ordering to respect, and no idle CPU cost.

Memory use is dominated by two parts: a fixed shared-memory region per domain, and a small per-endpoint cost that grows linearly with the number of live endpoints.

The shared-memory registry is the fixed ~38 MB region described in Registry layout, created once per domain at `/dev/shm/ros2_uds_<domain_id>` and mapped `MAP_SHARED` so every process sees the same bytes. Its size is constant and does not depend on how many nodes are running. The cost is that the region occupies its full size from the first process onward, so a system using only a few endpoints still maps the whole 38 MB. Because the file is sparse on most filesystems, untouched slots do not consume physical pages until they are written.

The region is also not removed on clean shutdown. `rmw_context_fini` unmaps the registry (via `registry_close`) but never calls `shm_unlink`, so `/dev/shm/ros2_uds_<domain>` persists for the lifetime of the host and is reused by the next process that joins that domain. It is removed only by an incompatible-version reclaim (a later process finding a wrong-size file unlinks and recreates it) or by hand (`rm /dev/shm/ros2_uds_<id>`). For containers this means a fresh container gets a fresh `/dev/shm`, and the host's `/dev/shm` must have room for the ~38 MB region (page-resident only as slots are touched). A default tmpfs `/dev/shm` is usually sized at half of RAM, which is far more than enough.

The per-endpoint cost is what scales with the system. Each node, publisher, subscription, service, and client consumes exactly one registry slot. Each receiving endpoint (subscription, service, client) also owns one bound `AF_UNIX` datagram socket: one file descriptor plus one socket file under `/tmp/ros2_uds/<domain_id>/`. Each process holds a single shared send socket regardless of how many publishers it owns. Each wait set holds one `epoll` file descriptor, and each guard condition holds one `eventfd`. For a system of 200 nodes with ten publishers and ten subscriptions each, this is roughly 4000 registry slots, about 2000 socket files, and on the order of 2000 file descriptors spread across all processes. Memory therefore tracks the live endpoint count linearly on top of the fixed registry region. The slot count is the resource to watch as a system grows, because a large graph can use a meaningful fraction of the 32768 available slots (see Scaling characteristics).

| Resource | Usage |
|----------|-------|
| Shared memory | ~38 MB (fixed, one region per domain) |
| Registry slot | 1 per node, publisher, subscription, service, and client |
| Receive socket | 1 fd + 1 file in `/tmp/ros2_uds/` per subscription, service, and client |
| Send socket | 1 fd per process, shared by all publishers/clients/services |
| epoll fd | 1 per wait set |
| eventfd | 1 per guard condition |
| Background threads | 0 |

### Limitations and unsupported features

#### Localhost only, by design

This RMW communicates only between processes on a single host. `AF_UNIX` sockets are local to the machine by definition, and the shared-memory registry is a single host-local file. There is no network transport, no multi-host discovery, and no DDS interoperability: every node on the machine must use `rmw_unix_socket_cpp`, and it cannot exchange messages with CycloneDDS, FastDDS, Connext, or Zenoh endpoints. There is also no DDS-Security: no authentication, encryption, or access control. Anything on the host that can read `/dev/shm` and `/tmp/ros2_uds/` can observe and inject traffic. For networked communication or secured deployments, a DDS-based RMW is the right choice. The implementation is also Linux-specific, because it relies on `epoll`, `eventfd`, `/dev/shm`, and `AF_UNIX`.

#### The single-datagram size cap (~400 KB)

Each message must fit in a single datagram; the usable cap is roughly 400 KB on a stock kernel. See The per-message size cap (~400 KB) under Transport for the full derivation and the sysctl remedy.

#### Functions that return `RMW_RET_UNSUPPORTED`

These functions are part of the RMW API but cannot be backed by a copy-based Unix-socket transport. Returning `RMW_RET_UNSUPPORTED` is the contract that tells rcl/rclcpp to skip the feature gracefully rather than fail.

- **Loaned (zero-copy) messages**: `rmw_borrow_loaned_message`, `rmw_publish_loaned_message`, `rmw_take_loaned_message`, `rmw_take_loaned_message_with_info`, `rmw_return_loaned_message_from_publisher`, `rmw_return_loaned_message_from_subscription`. Loaned messages require publisher and subscriber to share the same memory region. This transport copies payloads through kernel socket buffers, so there is no shared region to lend.
- **Dynamic message types**: `rmw_take_dynamic_message`, `rmw_take_dynamic_message_with_info`, `rmw_serialization_support_init`. Dynamic typing needs a runtime type-discovery protocol (such as DDS Type Object) to learn remote message layouts. The registry carries only a type name string, not a type description.
- **Network flow endpoints**: `rmw_publisher_get_network_flow_endpoints`, `rmw_subscription_get_network_flow_endpoints`. These report IP addresses and ports for QoS tracing. `AF_UNIX` sockets are addressed by filesystem path, so there is no IP flow to report.
- **Content filtering**: `rmw_subscription_set_content_filter`, `rmw_subscription_get_content_filter`. Filtering requires a per-message expression engine. Every subscriber receives every message on its topic.
- **Event callbacks**: `rmw_event_set_callback`. Events are initialized so rcl can add them to wait sets without crashing, but none are ever generated, and `rmw_event_type_is_supported` returns `false` for all event types.

#### Functions that return `RMW_RET_OK` but are no-ops

These succeed silently and do nothing. The RMW spec permits an implementation to accept a feature without enforcing it.

- `rmw_init_publisher_allocation` / `rmw_fini_publisher_allocation` and `rmw_init_subscription_allocation` / `rmw_fini_subscription_allocation`: serialization allocates per-publish via `fastcdr`, so pre-allocation hints are accepted and ignored.
- `rmw_publisher_assert_liveliness` and `rmw_node_assert_liveliness`: liveliness is not tracked.
- `rmw_publisher_wait_for_all_acked`: localhost delivery is immediate, so there is nothing to wait for.
- `rmw_set_log_severity`: there is no internal logging level to set.

#### QoS policies that are not enforced

Several QoS policies are accepted at creation but not acted on. The QoS handling section and its summary table are the authoritative treatment of which policies are enforced and which are accepted-but-ignored, including the `rmw_qos_profile_check_compatible` warnings.

### Testing and validation

The design rests on subtle concurrency claims (a lock-free seqlock, an ABA guard via the sequence recheck, no torn reads, bounded reader retries), so those claims are backed by tests rather than left as assertions. The suite is wired through `ament_add_gtest` under `BUILD_TESTING` and covers each component with unit tests: registry, transport, serialization, QoS, pub/sub, service/client, wait set, and guard conditions. The load-bearing one for the registry is `test/test_registry_concurrent.cpp`, a stress test that runs concurrent writers and readers against the slot array to exercise the no-torn-read and ABA-guard properties the registry sections claim. A `test/perf` directory holds microbenchmarks. Run the suite with:

```bash
colcon test --packages-select rmw_unix_socket_cpp
colcon test-result --verbose
```

### Build and file structure

The package builds with `ament_cmake`, using `configure_rmw_library()` and `register_rmw_implementation()` to register itself as a selectable RMW. Its only dependencies are standard ROS 2 packages and Linux kernel APIs: `rmw` (the abstract interface), `rcutils` and `rcpputils` (utilities), `fastcdr` with `rosidl_typesupport_fastrtps_c` / `rosidl_typesupport_fastrtps_cpp` (CDR serialization), and `pthread` / `rt` (POSIX threading and shared memory). No DDS or other external middleware is pulled in; everything else is implemented directly on kernel primitives that are always present.

To build and select it:

```bash
colcon build --packages-select rmw_unix_socket_cpp
export RMW_IMPLEMENTATION=rmw_unix_socket_cpp
ros2 run demo_nodes_cpp talker     # one terminal
ros2 run demo_nodes_cpp listener   # another terminal, same env var
```

The source under `src/` is organized so each concern sits in one place:

| File | Purpose |
|------|---------|
| `identifier.hpp` | Implementation identifier string |
| `logging.hpp` | Internal logging helpers |
| `types.hpp` | Internal C++ data types |
| `registry.hpp` / `registry.cpp` | Shared-memory discovery registry |
| `serialization.hpp` / `serialization.cpp` | CDR serialization via `fastcdr` |
| `transport.hpp` / `transport.cpp` | Unix socket send/receive helpers |
| `rmw_init.cpp` | Context init and shutdown |
| `rmw_node.cpp` | Node create/destroy |
| `rmw_publisher.cpp` | Publisher create/destroy/publish |
| `rmw_subscription.cpp` | Subscription create/destroy/take |
| `rmw_service.cpp` | Service server |
| `rmw_client.cpp` | Service client |
| `rmw_guard_condition.cpp` | Guard conditions (`eventfd`) |
| `rmw_wait.cpp` | Wait set (`epoll`) |
| `rmw_graph.cpp` | Graph introspection queries |
| `rmw_event.cpp` | Event stubs |
| `rmw_serialize.cpp` | Public serialize/deserialize API |
| `rmw_features.cpp` | Feature queries, identifier, QoS |
| `rmw_unsupported.cpp` | Unsupported and no-op stubs |
