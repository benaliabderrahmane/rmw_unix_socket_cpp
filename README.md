# rmw_unix_socket_cpp

A lightweight, **localhost-only** RMW (ROS Middleware) implementation for ROS 2,
built on Unix domain sockets and POSIX shared memory instead of DDS.

It targets single-machine deployments with many nodes (150+ in production) and a
small, deterministic resource footprint — no DDS, no daemon, no background
threads. Discovery is a shared-memory registry; data moves over `AF_UNIX`
datagram sockets; the wait set is `epoll` + `eventfd`.

## When to use it (and when not)

**Use it when:**
- Everything runs on **one Linux host** (bare metal or co-located containers).
- You want lower overhead and more predictable resource usage than DDS.
- You don't need network communication, security, or DDS interoperability.

**Don't use it when:**
- Nodes span **multiple machines** — `AF_UNIX` is local-only. Use CycloneDDS or Zenoh.
- You need DDS interop, content filtering, zero-copy loaned messages, or
  deadline/lifespan/liveliness QoS. See [Limitations](#limitations).

## Status

- ROS 2 **Jazzy**, **Kilted**, and **Rolling** — built and tested in CI.
- **Linux / x86_64 only** (uses `epoll`, `eventfd`, `/dev/shm`, `AF_UNIX`).
- Version `0.1.0`, Apache-2.0.

## Quick start

```bash
# Build (from your workspace root, alongside your other packages)
source /opt/ros/jazzy/setup.bash
colcon build --packages-select rmw_unix_socket_cpp

# Select it at runtime — every process that should talk must set this
source install/setup.bash
export RMW_IMPLEMENTATION=rmw_unix_socket_cpp

# Verify
ros2 run demo_nodes_cpp talker
# ...in another terminal (with the same two env exports):
ros2 run demo_nodes_cpp listener
ros2 doctor --report   # confirms the active middleware
```

Without `RMW_IMPLEMENTATION=rmw_unix_socket_cpp`, rcl loads whatever RMW is the
system default (usually FastDDS) and this implementation is never used. **All**
nodes that need to communicate must select the same RMW — there is no DDS bridge.

## Design at a glance

| Concern | Choice |
|---|---|
| **Transport** | `SOCK_DGRAM` Unix domain sockets in `/tmp/ros2_uds/<domain_id>/`. Message boundaries are free; no connection management. |
| **Discovery** | Single shared-memory file `/dev/shm/ros2_uds_<domain_id>`. No daemon, no multicast. |
| **Locking** | Lock-free registry (per-slot seqlock + atomic state machine); process-local caches use a plain mutex. |
| **Wait** | `epoll` over socket fds + `eventfd` guard conditions. **No background threads** — all I/O happens inside `rmw_wait`. |
| **Serialization** | CDR via `fastcdr` + `rosidl_typesupport_fastrtps` (the same path as the DDS RMWs). |
| **Stale cleanup** | Dead processes are detected via `/proc/<pid>`; their registry entries and socket files are reclaimed on init and on every discovery query. |

Every choice — including the ones that were tried and abandoned — is written up
with rationale in **[DESIGN.md](rmw_unix_socket_cpp/DESIGN.md)**.

## Running across multiple containers

Because discovery and cleanup live in shared kernel resources (`/dev/shm`,
`/tmp`, and the PID namespace), co-located containers must **share** those
namespaces to see each other. Each container also needs `RMW_IMPLEMENTATION` set.

Required per container:

| Requirement | Why |
|---|---|
| `RMW_IMPLEMENTATION=rmw_unix_socket_cpp` | Selects this middleware. |
| `--ipc=host` | All containers share `/dev/shm` → the same discovery registry. |
| `--pid=host` | Stale-PID cleanup uses `/proc/<pid>`; without a shared PID namespace one container reclaims another's live entries and corrupts the registry. |
| `-v /tmp/ros2_uds:/tmp/ros2_uds` | All containers share the socket files data actually flows over. |
| same `ROS_DOMAIN_ID` | The registry file and socket directory are per-domain. |

`docker compose` example:

```yaml
services:
  talker:
    image: my_ros_image
    environment:
      - RMW_IMPLEMENTATION=rmw_unix_socket_cpp
      - ROS_DOMAIN_ID=0
    ipc: host
    pid: host
    volumes:
      - /tmp/ros2_uds:/tmp/ros2_uds
    command: ros2 run demo_nodes_cpp talker

  listener:
    image: my_ros_image
    environment:
      - RMW_IMPLEMENTATION=rmw_unix_socket_cpp
      - ROS_DOMAIN_ID=0
    ipc: host
    pid: host
    volumes:
      - /tmp/ros2_uds:/tmp/ros2_uds
    command: ros2 run demo_nodes_cpp listener
```

`docker run` equivalent:

```bash
docker run --ipc=host --pid=host \
  -e RMW_IMPLEMENTATION=rmw_unix_socket_cpp -e ROS_DOMAIN_ID=0 \
  -v /tmp/ros2_uds:/tmp/ros2_uds \
  my_ros_image ros2 run demo_nodes_cpp talker
```

### Host kernel tuning

Set these on the **host** (they apply to all containers). Without them you get
silent buffer clamps and transient drops under load — fine for small control
messages, but images and point clouds will be dropped.

| Knob | Default | Recommended | Why |
|---|--:|--:|---|
| `net.core.rmem_max` | ~212 KB | 48 MB+ | Caps `SO_RCVBUF`; large messages drop below this. |
| `net.core.wmem_max` | ~212 KB | 48 MB+ | Caps `SO_SNDBUF`; `EMSGSIZE` on send below this. |
| `net.unix.max_dgram_qlen` | 512 | 4096 | Per-socket pending-datagram cap; bursty fan-out overflows 512. |
| `ulimit -n` (`nofile`) | 1024 | 65536 | Each pub/sub/service/client uses 1–2 fds. |

```bash
sudo sysctl -w net.core.rmem_max=48000000 net.core.wmem_max=48000000 \
                net.unix.max_dgram_qlen=4096
```

See [DESIGN.md → Operational requirements](rmw_unix_socket_cpp/DESIGN.md#operational-requirements)
for the full rationale.

## Limitations

Localhost only; Linux only; no DDS interoperability. Loaned (zero-copy) messages,
dynamic message types, network flow endpoints, and content filtering return
`RMW_RET_UNSUPPORTED`. `deadline`, `lifespan`, and `liveliness` QoS are accepted
but not enforced. The full list and the reasoning behind each is in
[DESIGN.md → Limitations and Unsupported Features](rmw_unix_socket_cpp/DESIGN.md#limitations-and-unsupported-features).

## Documentation

| Document | What it is |
|---|---|
| **[DESIGN.md](rmw_unix_socket_cpp/DESIGN.md)** | Self-contained design rationale: every wire format, sync primitive, QoS decision, and operational quirk. Start here. |
| **[test/README.md](rmw_unix_socket_cpp/test/README.md)** | Every test in the suite, what it validates, and why it exists. |
| **[test/perf/README.md](rmw_unix_socket_cpp/test/perf/README.md)** | Standalone latency/scale benchmarks (not run by `colcon test`). |

## License

Apache-2.0.
