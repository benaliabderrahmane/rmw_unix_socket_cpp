# Performance Tests — `rmw_unix_socket_cpp`

These are **standalone benchmarks**, not unit tests. They measure real
end-to-end latency under realistic loads. They are not run by `colcon test`.

## Tests

| Script | What it measures | Default scale |
|---|---|---|
| `perf_pubsub.py` | Pub/sub latency, in a single process (N pairs on N topics) | 100 pairs × 200 messages |
| `perf_services.py` | Service round-trip latency, in a single process | 50 pairs × 200 calls |
| `scale_100_nodes.py` | Cross-process pub/sub with N **separate processes** | 100 talkers × 10s @ 50 Hz |

## Why two flavors

- **In-process** (`perf_pubsub.py`, `perf_services.py`) — isolates the RMW
  hot path. No process startup, no executor scheduling between processes.
  Tells you the cost of a single publish/take cycle.
- **Cross-process** (`scale_100_nodes.py`) — the real deployment shape.
  Includes registry discovery, kernel context switches between processes,
  and the cache effects of many distinct PIDs hitting the shared registry.
  Tells you what your fleet actually pays.

## Running

Build and source the workspace first:

```bash
source /opt/ros/jazzy/setup.bash
cd /home/abe/projects/drixo_ws_ros2/rmw
colcon build --packages-select rmw_unix_socket_cpp
source install/setup.bash
```

Then run any test under the UDS rmw:

```bash
cd rmw_unix_socket_cpp/test/perf
RMW_IMPLEMENTATION=rmw_unix_socket_cpp python3 perf_pubsub.py
RMW_IMPLEMENTATION=rmw_unix_socket_cpp python3 perf_services.py
RMW_IMPLEMENTATION=rmw_unix_socket_cpp python3 scale_100_nodes.py
```

Each script accepts CLI overrides — see the docstring at the top of each file.

### Comparing against another RMW

Same scripts work against any RMW. Run again with a different
`RMW_IMPLEMENTATION` to compare:

```bash
RMW_IMPLEMENTATION=rmw_fastrtps_cpp python3 perf_pubsub.py
RMW_IMPLEMENTATION=rmw_cyclonedds_cpp python3 perf_pubsub.py
```

## Reading the output

Each script prints aggregate stats over all samples:

```
  aggregate (all pairs): n=20000  min=42.1us  med=78.3us  mean=91.4us
                         p95=180.2us  p99=312.0us  max=1842.0us
```

Plus a per-pair p99 distribution — useful for spotting tail-latency
outliers caused by individual stragglers (e.g. one publisher whose
subscriber cache keeps invalidating).

**Exit code:** `0` if every expected message arrived, `2` if any were
lost. The in-process tests should never lose messages on UDS — if they
do, something is wrong with the queue or with cache invalidation.

## Caveats

- **Python overhead is real.** rclpy adds ~100–200 µs per take on top of
  what the RMW costs. To isolate the RMW itself, port the script to C++
  using rclcpp. The Python numbers are still useful for comparison
  *between* RMWs, since the overhead is constant.
- **First message is slower** because of discovery + cache misses. The
  scripts include a 2 s warmup before measurement.
- **`scale_100_nodes.py` needs file descriptors.** Each process holds a
  socket per endpoint plus the registry mmap. On a default `ulimit -n
  1024`, 100 processes is fine; 500+ may need `ulimit -n 4096`.
