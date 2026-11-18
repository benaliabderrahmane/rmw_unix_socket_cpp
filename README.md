# rmw_unix_socket_cpp

A ROS 2 middleware (RMW) implementation for systems that run on one Linux machine.
It uses Unix domain sockets and POSIX shared memory instead of DDS.

It was written for a robot with many nodes on one computer (150+ in production).
There is no DDS, no daemon, and no background thread unless your executor asks
for callbacks. Discovery is a table in shared memory, messages travel over
`AF_UNIX` datagram sockets, large messages go through shared memory, and waiting
is `epoll` plus `eventfd`.

## When to use it

Use it when:
- everything runs on one Linux host (bare metal, or containers on the same host);
- you want less overhead and steadier resource use than DDS;
- you don't need networking, security or DDS interoperability.

Don't use it when:
- your nodes run on several machines: `AF_UNIX` is local only, so use Cyclone DDS,
  Fast DDS or Zenoh;
- you need DDS interoperability, content filtering, loaned messages, or deadline,
  lifespan and liveliness QoS (see [Limitations](#limitations)).

## Status

- Built and tested in CI on ROS 2 Jazzy, Rolling and Lyrical.
- Linux on x86_64 and aarch64 (it uses `epoll`, `eventfd`, `/dev/shm` and `AF_UNIX`).
- Version 0.6.0, Apache-2.0.

## Quick start

```bash
mkdir -p ~/ws/src && cd ~/ws
git clone https://github.com/benaliabderrahmane/rmw_unix_socket_cpp.git src/rmw_unix_socket_cpp
source /opt/ros/jazzy/setup.bash        # or rolling, lyrical
rosdep install --from-paths src --ignore-src -y
colcon build --packages-select rmw_unix_socket_cpp

# In every terminal that should use it:
source ~/ws/install/setup.bash
export RMW_IMPLEMENTATION=rmw_unix_socket_cpp

ros2 run demo_nodes_cpp talker
ros2 run demo_nodes_cpp listener       # in a second terminal
ros2 doctor --report                   # shows the active middleware
```

Every process that should talk to the others needs
`RMW_IMPLEMENTATION=rmw_unix_socket_cpp`. Without it, rcl loads the system default
(usually Fast DDS), and there is no bridge between the two.

## Design at a glance

| Part | How it works |
|---|---|
| Transport | `SOCK_DGRAM` Unix domain sockets in `/tmp/ros2_uds/<domain_id>/`. Datagrams keep message boundaries, and there are no connections to manage. |
| Large messages | Messages of 64 KiB and up are written once into shared memory, and each receiver gets a 32-byte descriptor over its socket. A publisher's ring holds at least four of its largest messages (1 MiB minimum), and KEEP_LAST subscriptions decode messages of 512 KiB and up straight from it. |
| Discovery | One shared-memory file per domain, `/dev/shm/ros2_uds_<domain_id>`. No daemon, no multicast. |
| Locking | The registry is lock-free (a seqlock per slot and an atomic state machine); caches inside a process use plain mutexes. |
| Waiting | `epoll` over the socket fds, and an `eventfd` per guard condition. Messages are read inside `rmw_wait`, so an executor that waits gets no background thread. |
| Callbacks | When an endpoint registers a listener callback, one listener thread per context starts and delivers for it. The `EventsExecutor` needs this, since it never calls `rmw_wait`. |
| Serialization | CDR through Fast CDR and `rosidl_typesupport_fastrtps`, the same path as the DDS RMWs. |
| Cleanup | Dead processes are found through `/proc/<pid>`. Their registry entries and socket files are reclaimed at init and during discovery queries, and their shared-memory segments at init. |
| Tracing | The standard `tracetools` rmw tracepoints (`rmw_publish`, `rmw_take`, `rmw_publisher_init`, `rmw_subscription_init`), so `ros2 trace`, Trace Compass and CARET follow messages across processes as they do on the DDS RMWs. |

[DESIGN.md](rmw_unix_socket_cpp/DESIGN.md) explains each of these choices,
including the ones that were tried and dropped.

## Running in containers

Discovery and cleanup use shared kernel resources (`/dev/shm`, `/tmp` and the PID
namespace), so containers on the same host must share them to see each other.

| Setting | Why |
|---|---|
| `RMW_IMPLEMENTATION=rmw_unix_socket_cpp` | Selects this middleware. |
| `--ipc=host` | Shares `/dev/shm`, so every container uses the same discovery registry and the same large-message rings. |
| `--pid=host` | Stale-entry cleanup checks `/proc/<pid>`. Without a shared PID namespace, one container would reclaim another's live entries as dead. |
| `-v /tmp/ros2_uds:/tmp/ros2_uds` | Shares the socket files the data flows through. |
| `--sysctl net.unix.max_dgram_qlen=4096` | This limit is per network namespace, so a container starts at the kernel default of 10, whatever the host is set to. Not needed with `--network=host`. |
| Same `ROS_DOMAIN_ID` | The registry file and the socket directory are per domain. |

With `docker compose`:

```yaml
services:
  talker:
    image: my_ros_image
    environment:
      - RMW_IMPLEMENTATION=rmw_unix_socket_cpp
      - ROS_DOMAIN_ID=0
    ipc: host
    pid: host
    sysctls:
      - net.unix.max_dgram_qlen=4096
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
    sysctls:
      - net.unix.max_dgram_qlen=4096
    volumes:
      - /tmp/ros2_uds:/tmp/ros2_uds
    command: ros2 run demo_nodes_cpp listener
```

With `docker run`:

```bash
docker run --ipc=host --pid=host --sysctl net.unix.max_dgram_qlen=4096 \
  -e RMW_IMPLEMENTATION=rmw_unix_socket_cpp -e ROS_DOMAIN_ID=0 \
  -v /tmp/ros2_uds:/tmp/ros2_uds \
  my_ros_image ros2 run demo_nodes_cpp talker
```

### Host kernel settings

Without these, the kernel silently clamps the socket buffers and bursts of
messages get dropped under load. Messages of 64 KiB and up go through shared
memory and don't depend on them, unless `/dev/shm` is unavailable.

| Setting | Default | Recommended | Why |
|---|--:|--:|---|
| `net.core.rmem_max` | ~212 KB | 48 MB+ | Caps `SO_RCVBUF`, the room a subscriber has for a burst. |
| `net.core.wmem_max` | ~212 KB | 48 MB+ | Caps `SO_SNDBUF`. A Unix datagram counts against the sender until it is read, so a small send buffer drops sooner when subscribers lag. |
| `net.unix.max_dgram_qlen` | 512 on most hosts | 4096 | How many datagrams a socket can queue; bursty fan-out overflows 512. |
| `ulimit -n` (`nofile`) | 1024 | 65536 | Each publisher, subscription, service and client uses one or two fds. |

```bash
sudo sysctl -w net.core.rmem_max=50331648 net.core.wmem_max=50331648 \
               net.unix.max_dgram_qlen=4096
```

`rmem_max` and `wmem_max` apply to the whole host, containers included.
`max_dgram_qlen` does not: set it in each container (see above).
[DESIGN.md → Operational requirements](rmw_unix_socket_cpp/DESIGN.md#operational-requirements)
has the details.

## Limitations

- One host and Linux only; no DDS interoperability.
- Loaned messages, dynamic message types, network flow endpoints and content
  filtering return `RMW_RET_UNSUPPORTED`.
- `deadline`, `lifespan` and `liveliness` QoS are accepted but not enforced.
- Of the QoS status events, `message_lost` and both `matched` events are reported.
  Deadline, liveliness, incompatible QoS and incompatible type are refused with
  `RMW_RET_UNSUPPORTED`: deadline and liveliness need a clock that nothing here
  runs, and every subscription on a topic receives its messages whatever its QoS
  or type, so an incompatible-QoS event would report a failure that does not
  happen (#92 plans QoS- and type-aware matching). `rclcpp` catches the refusal
  for the handlers it installs by default, with a warning per handler on Rolling
  and Lyrical; on Jazzy, a handler the application registers itself for a refused
  event throws.
- The listener callbacks the `EventsExecutor` needs are supported.

[DESIGN.md → Limitations and unsupported features](rmw_unix_socket_cpp/DESIGN.md#limitations-and-unsupported-features)
lists everything, with the reasons.

## Documentation

| Document | Contents |
|---|---|
| [DESIGN.md](rmw_unix_socket_cpp/DESIGN.md) | How it works and why: wire formats, synchronization, QoS, operations. Start here. |
| [test/README.md](rmw_unix_socket_cpp/test/README.md) | Every test, what it checks and why. |
| [test/perf/README.md](rmw_unix_socket_cpp/test/perf/README.md) | Standalone benchmarks, not run by `colcon test`. |
| [RELEASING.md](RELEASING.md) | How releases are cut and published with bloom. |

## License

Apache-2.0, see [LICENSE](LICENSE).
