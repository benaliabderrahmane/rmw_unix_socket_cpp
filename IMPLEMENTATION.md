# Deploying `rmw_unix_socket_cpp` to production

This document covers the **operational** side of running our RMW: what each Docker Compose setting is for, why it's required, and how to roll out a new build to the DRIX containers.

For the design rationale of the RMW itself, see [`rmw_unix_socket_cpp/DESIGN.md`](rmw_unix_socket_cpp/DESIGN.md).

---

## 1. Required Docker Compose configuration

Every container that runs ROS 2 nodes using this RMW needs the settings below. Skipping any one of them causes a specific, predictable failure.

### Reference snippet

```yaml
services:
  drix_node:
    image: drix/cortix:latest

    # 1. Tell ROS 2 which RMW to load
    environment:
      - RMW_IMPLEMENTATION=rmw_unix_socket_cpp
      - ROS_DOMAIN_ID=200

    # 2. Share the kernel resources our RMW depends on
    ipc: host
    pid: host

    # 3. Share the socket directory across containers
    volumes:
      - /tmp/ros2_uds:/tmp/ros2_uds

    # 4. Raise the per-process file descriptor limit
    ulimits:
      nofile:
        soft: 65536
        hard: 65536

    # 5. Allow the container to tune kernel buffers (only the one that runs sysctl)
    sysctls:
      net.core.rmem_max: 50331648
      net.core.wmem_max: 50331648
```

Each of these maps to a concrete RMW behavior. Details below.

---

### 1.1 `RMW_IMPLEMENTATION=rmw_unix_socket_cpp`

**What:** ROS 2 selects its middleware at runtime by reading this environment variable.

**Why:** Without it, rcl falls back to whatever RMW is the system default (FastDDS on most ROS 2 distros). The container would then run DDS instead of our Unix-socket RMW — silently, with no error.

**How to verify after start:**
```bash
docker exec <container> bash -c 'source /opt/ros/humble/setup.bash && ros2 doctor --report' | grep -i rmw
```

### 1.2 `ROS_DOMAIN_ID=200`

**What:** Selects which `/dev/shm/ros2_uds_<domain>` registry file the process attaches to.

**Why:** Our production domain is **200**, not the default `0`. Containers on different domain IDs cannot see each other's nodes. Make sure every DRIX container uses the same value.

### 1.3 `ipc: host`

**What:** Removes Docker's default per-container IPC namespace and puts the container in the host's IPC namespace.

**Why:** Our discovery registry lives at `/dev/shm/ros2_uds_<domain>`. By default, every Docker container has its own private `/dev/shm` (a tmpfs mount). Without `ipc: host`, **container A and container B each create their own registry and never see each other's nodes** — every container becomes an isolated ROS 2 graph.

With `ipc: host`, all containers and the host share one `/dev/shm`, so the registry is the single source of truth for discovery.

**Symptom of forgetting it:** `ros2 node list` from inside the container only shows that container's nodes, never the others.

### 1.4 `pid: host`

**What:** Removes Docker's default per-container PID namespace; the container sees the host's full process tree.

**Why:** Our stale-entry cleanup uses `stat("/proc/<pid>")` to detect dead processes ([registry.cpp:146-174](rmw_unix_socket_cpp/src/registry.cpp#L146-L174), [transport.cpp:172](rmw_unix_socket_cpp/src/transport.cpp#L172)). Without `pid: host`, container A would see container B's PIDs as **non-existent** (different namespace) and would happily delete container B's registry slots and unlink its `.sock` files — corrupting the live system.

**Symptom of forgetting it:** Random "registry full" or "subscriber not found" errors as containers wipe each other's entries on `rmw_init`.

This is documented in [DESIGN.md §Limitations](rmw_unix_socket_cpp/DESIGN.md#L277).

### 1.5 Bind mount `/tmp/ros2_uds`

**What:** `volumes: ['/tmp/ros2_uds:/tmp/ros2_uds']` exposes the host's socket directory inside the container.

**Why:** Topics/services don't flow through shared memory — they flow through Unix domain sockets in `/tmp/ros2_uds/<domain>/<prefix>_<pid>_<id>.sock`. Each container creates its own socket files there. Without the bind mount, container A's publisher cannot `sendmsg()` to container B's subscriber socket because the socket file simply doesn't exist in A's filesystem view.

**Symptom of forgetting it:** Discovery works (because the registry is shared via `ipc: host`), but no actual messages get delivered. Publisher's `sendmsg()` returns `ENOENT`.

### 1.6 `ulimits.nofile: 65536`

**What:** Raises the per-process file descriptor limit from the Docker default (often 1024) to 65536.

**Why:** Every subscription, service server, and service client in this RMW holds **one socket fd**. At our scale:
- 210 nodes × ~10 endpoints/node ≈ **2100 fds for the system**
- A single CLI tool like `ros2 param list` opens ~210 client fds at once (one per node)
- The default limit of 1024 is exceeded routinely

**Symptom of forgetting it:** `failed to create client socket` or `failed to create subscription socket` errors at apparently random points (when the per-process count crosses 1024).

This is the failure we hit with `ros2 param list` against 210 nodes.

### 1.7 `sysctls: net.core.{r,w}mem_max`

**What:** Increases the kernel's maximum allowed receive and send socket buffer sizes to 48 MiB.

**Why:** [transport.cpp:18-19](rmw_unix_socket_cpp/src/transport.cpp#L18-L19) requests 4 MiB buffers via `setsockopt(SO_RCVBUF / SO_SNDBUF, ...)`. The kernel caps the actual size at `net.core.rmem_max` / `wmem_max`, which default to ~212 KB on most distros. Without raising the sysctl, our 4 MiB request is silently clipped down — and then a 1 MiB point cloud fails to send with `EMSGSIZE`, or fills up immediately and triggers `EAGAIN` drops.

48 MiB is the ceiling for any future per-message size tuning. To actually use messages larger than 4 MiB, the `RECV_BUF_SIZE` / `SEND_BUF_SIZE` constants in [transport.cpp:18-19](rmw_unix_socket_cpp/src/transport.cpp#L18-L19) must also be raised — the sysctl alone only lifts the cap, it does not change what the RMW requests.

**Note:** `sysctls` in Docker Compose only sets it inside the container's network namespace. Because we use `ipc: host` (not `network: host`), this is per-container. **Set it on every container that publishes large messages**, or set it once on the host's `/etc/sysctl.conf` and skip the Compose sysctl entry.

**Symptom of forgetting it:** Large messages (camera frames, point clouds) get dropped silently or fail with `EMSGSIZE`.

---

## 2. Why `network: host` is NOT in the list

DDS-based RMWs typically need `network: host` because their multicast discovery depends on it. **We don't.**

Discovery: shared memory (`ipc: host`).
Data: Unix domain sockets (bind mount).

There is no IP, no port, no multicast group involved anywhere in this RMW. The container can stay on its default bridge network and still participate fully in the ROS 2 graph. This is one of the operational wins over DDS — Docker network isolation is preserved.

---

## 3. Deploying a new build to production

The RMW is a shared library: `librmw_unix_socket_cpp.so`. After building locally, the binary needs to land in **every workspace** that the production containers source.

### 3.1 Build locally

```bash
cd /home/abe/projects/drixo_ws_ros2
colcon build --packages-select rmw_unix_socket_cpp --cmake-args -DCMAKE_BUILD_TYPE=Release
```

The output goes to `install/rmw_unix_socket_cpp/`.

### 3.2 Copy to the dependent workspaces

Production has multiple ROS 2 workspaces overlaid on top of each other. The RMW must exist in any workspace whose `setup.bash` is sourced — otherwise rcl fails to load the `.so`.

```bash
# Source workspace (where you build)
DRIXO_WS=/home/abe/projects/drixo_ws_ros2

# Target workspaces (where production runs)
DRIX_WS=/path/to/drix_ws
CORTIX_SENSE_WS=/path/to/cortix_sense

rsync -avzL \
  $DRIXO_WS/install/rmw_unix_socket_cpp/ \
  $DRIX_WS/install/rmw_unix_socket_cpp/

rsync -avzL \
  $DRIXO_WS/install/rmw_unix_socket_cpp/ \
  $CORTIX_SENSE_WS/install/rmw_unix_socket_cpp/
```

**Why `-L` (follow symlinks):** colcon installs use symlinked directories during development. `rsync -L` resolves them so the destination has actual files, not dangling links.

**Why both workspaces:** `drix_ws` and `cortix_sense` are independent overlays. ROS 2's `AMENT_PREFIX_PATH` searches each prefix in order and loads the first matching package. The RMW must exist where rcl looks first — copying to one but not the other leads to a confusing "works in some containers, fails in others" state.

### 3.3 Sync to remote target (mind-box-1)

Same `rsync -avzL` pattern, but to the deployment host:

```bash
rsync -avzL \
  $DRIXO_WS/install/rmw_unix_socket_cpp/ \
  drix@mind-box-1:/workspace/install/rmw_unix_socket_cpp/
```

### 3.4 Restart the containers

**Critical:** Docker caches the `.so` in memory once it's been `dlopen`ed. Just rsyncing the new file is not enough — running processes keep using the old version.

```bash
docker compose restart   # or docker compose down && docker compose up -d
```

### 3.5 Clean stale state on first deploy after a schema change

If you bumped `REGISTRY_VERSION` in [registry.hpp](rmw_unix_socket_cpp/src/registry.hpp#L18), changed the `RegistryEntry` layout, or are recovering from a partial deployment, wipe the shared state before starting:

```bash
# On the host, before docker compose up
sudo rm -f /dev/shm/ros2_uds_*
sudo rm -rf /tmp/ros2_uds/
```

Otherwise the new RMW will read the old shared memory layout, misinterpret it, and either ignore everything (if version mismatches) or read garbage strings.

---

## 4. Quick verification checklist

After deploying, run these on a representative container to confirm everything is wired up:

```bash
# 1. Right RMW loaded
ros2 doctor --report | grep -i rmw

# 2. Right domain
echo $ROS_DOMAIN_ID                      # → 200

# 3. Registry visible
ls -la /dev/shm/ros2_uds_200             # should exist

# 4. Sockets visible
ls /tmp/ros2_uds/200/ | head             # should show .sock files

# 5. Cross-container discovery works
ros2 node list | wc -l                   # should match the global count

# 6. Fd headroom
ulimit -n                                # should be 65536

# 7. Kernel buffer cap
sysctl net.core.rmem_max                 # should be 50331648
```

If all seven pass, the deployment is healthy.

---

## 5. Failure-mode cheat sheet

| Symptom | Likely cause |
|---|---|
| `ros2 node list` only shows local container's nodes | Missing `ipc: host` |
| Discovery works but messages don't arrive | Missing `/tmp/ros2_uds` bind mount |
| Random "registry full" / orphaned entries | Missing `pid: host` (containers cleaning each other) |
| `failed to create client socket` during `ros2 param list` | Missing/low `ulimits.nofile` |
| Big messages dropped silently | `rmem_max`/`wmem_max` not raised |
| RMW runs but uses DDS | `RMW_IMPLEMENTATION` env var not set or misspelled |
| New `.so` rsynced but old behavior persists | Container not restarted; Docker held old `.so` in memory |
| Works in some containers, not others | RMW only copied to one of `drix_ws` / `cortix_sense` |

---

## 6. Open improvements

These would make operations cleaner but aren't strictly required:

- **Errno-aware error messages.** [transport.cpp](rmw_unix_socket_cpp/src/transport.cpp) currently swallows `errno` from `socket()` and `bind()`. Surfacing it (`"failed to create client socket: EMFILE"`) would have made the `ros2 param list` failure obvious immediately.
- **Periodic orphan-socket cleanup.** Today cleanup runs only at `rmw_init`. Long-running deployments with frequent node restarts can accumulate files.
- **Health probe.** A small `ros2_uds_health` binary that runs the verification checklist non-interactively, suitable for `docker compose healthcheck`.
