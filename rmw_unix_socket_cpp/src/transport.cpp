// Copyright 2026 Abderahmane BENALI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "transport.hpp"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>

#include <dirent.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include "logging.hpp"

namespace rmw_uds
{

static constexpr int RECV_BUF_SIZE = 48 * 1024 * 1024;  // 48 MB
static constexpr int SEND_BUF_SIZE = 48 * 1024 * 1024;  // 48 MB

std::string ensure_socket_dir(size_t domain_id)
{
  char dir[128];
  std::snprintf(dir, sizeof(dir), "/tmp/ros2_uds/%zu", domain_id);

  // Create parent
  mkdir("/tmp/ros2_uds", 0777);
  mkdir(dir, 0777);

  return std::string(dir);
}

int create_bound_socket(const std::string & path)
{
  int fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    RMW_UDS_LOG_ERROR(
      "socket(AF_UNIX, SOCK_DGRAM) failed: %s (errno=%d) — out of file descriptors?",
      std::strerror(errno), errno);
    return -1;
  }

  // Remove stale socket file
  unlink(path.c_str());

  struct sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

  if (bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) != 0) {
    RMW_UDS_LOG_ERROR(
      "bind('%s') failed: %s (errno=%d)",
      path.c_str(), std::strerror(errno), errno);
    close(fd);
    return -1;
  }

  // Set large receive buffer. Kernel may silently cap at net.core.rmem_max;
  // log the effective size when it falls short so a slow-subscriber drop
  // later in the day can be traced back to an undersized recv buffer.
  int buf = RECV_BUF_SIZE;
  if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf)) != 0) {
    RMW_UDS_LOG_WARN(
      "setsockopt(SO_RCVBUF=%d) on '%s' failed: %s — recv buffer left at kernel default",
      RECV_BUF_SIZE, path.c_str(), std::strerror(errno));
  }

  return fd;
}

int create_send_socket()
{
  int fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    RMW_UDS_LOG_ERROR(
      "send socket(AF_UNIX, SOCK_DGRAM) failed: %s (errno=%d)",
      std::strerror(errno), errno);
    return -1;
  }

  int buf = SEND_BUF_SIZE;
  if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf)) != 0) {
    RMW_UDS_LOG_WARN(
      "setsockopt(SO_SNDBUF=%d) failed: %s — send buffer left at kernel default",
      SEND_BUF_SIZE, std::strerror(errno));
  }

  return fd;
}

SendResult send_to(
  int send_fd,
  const std::string & dest_path,
  const WireHeader & header,
  const uint8_t * payload,
  size_t payload_size)
{
  struct sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, dest_path.c_str(), sizeof(addr.sun_path) - 1);

  struct iovec iov[2];
  iov[0].iov_base = const_cast<WireHeader *>(&header);
  iov[0].iov_len = sizeof(WireHeader);
  iov[1].iov_base = const_cast<uint8_t *>(payload);
  iov[1].iov_len = payload_size;

  struct msghdr msg;
  std::memset(&msg, 0, sizeof(msg));
  msg.msg_name = &addr;
  msg.msg_namelen = sizeof(addr);
  msg.msg_iov = iov;
  msg.msg_iovlen = (payload_size > 0) ? 2 : 1;

  ssize_t sent = sendmsg(send_fd, &msg, MSG_DONTWAIT | MSG_NOSIGNAL);
  if (sent >= 0) {
    return SendResult::Ok;
  }

  // Classify the failure. Hot path — every category is throttled so a
  // persistently-failing peer can't drown the log. The destination path
  // encodes the peer's prefix+pid (see make_socket_path) which is enough
  // to identify the offending subscriber from the message alone.
  const int err = errno;
  const size_t total = sizeof(WireHeader) + payload_size;
  SendResult result = SendResult::SoftDrop;
  switch (err) {
    case EMSGSIZE:
      // Message exceeds the kernel's per-datagram cap (typically the send
      // or recv buffer size). This is a configuration-level problem, not
      // transient backpressure — log every 5s with full size context.
      RMW_UDS_LOG_ERROR_THROTTLE(
        5000,
        "UDS send to '%s' failed: message too big (%zu bytes incl. header). "
        "Raise net.core.{wmem_max,rmem_max} or split the message.",
        dest_path.c_str(), total);
      result = SendResult::ConfigError;
      break;
    case EAGAIN:
#if defined(EWOULDBLOCK) && EWOULDBLOCK != EAGAIN
    case EWOULDBLOCK:
#endif
    case ENOBUFS:
      // Peer's receive queue is full (or our send queue, briefly). This
      // is the classic "slow subscriber" symptom — the subscriber isn't
      // calling rmw_take fast enough to drain its socket.
      RMW_UDS_LOG_WARN_THROTTLE(
        1000,
        "UDS send to '%s' dropped: subscriber recv buffer full (%zu bytes, errno=%s). "
        "Slow subscriber or undersized SO_RCVBUF.",
        dest_path.c_str(), total, std::strerror(err));
      break;
    case ENOENT:
    case ECONNREFUSED:
      // Peer's socket file is gone (ENOENT) or unbound (ECONNREFUSED).
      // Happens routinely during graceful shutdown — the publisher's
      // cached subscriber list lags one graph-generation behind reality.
      // Demote to debug to avoid scaring users at every node teardown.
      RMW_UDS_LOG_DEBUG(
        "UDS send to '%s' skipped: peer gone (%s) — likely subscriber teardown",
        dest_path.c_str(), std::strerror(err));
      break;
    default:
      RMW_UDS_LOG_WARN_THROTTLE(
        1000,
        "UDS send to '%s' failed: %s (errno=%d, %zu bytes)",
        dest_path.c_str(), std::strerror(err), err, total);
      break;
  }
  return result;
}

bool recv_from(
  int socket_fd,
  WireHeader & header_out,
  std::vector<uint8_t> & payload_out)
{
  // Probe the size of the next datagram without copying any of it: with a
  // zero-length buffer, MSG_PEEK leaves the datagram queued and MSG_TRUNC
  // makes recv return its true on-wire length. This is what lets us read the
  // datagram exactly once below, straight into its destination — the old
  // peek-into-a-scratch-buffer approach copied every payload three times
  // (peek, real recv, then assign into payload_out).
  ssize_t n = recv(socket_fd, nullptr, 0, MSG_DONTWAIT | MSG_PEEK | MSG_TRUNC);

  if (n < 0) {
    // EAGAIN is the steady-state "nothing to read" case driven by wait()
    // loops; staying silent is intentional. Anything else (EBADF, EINVAL,
    // EINTR-after-shutdown) is worth surfacing.
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      RMW_UDS_LOG_WARN_THROTTLE(
        1000,
        "UDS recv (probe) failed: %s (errno=%d)",
        std::strerror(errno), errno);
    }
    return false;
  }
  if (n < static_cast<ssize_t>(sizeof(WireHeader))) {
    // Runt (or zero-length) datagram — not one of ours. Consume it so it
    // doesn't sit at the head of the FIFO blocking real messages and waking
    // a level-triggered epoll forever; a zero-length read discards exactly
    // one datagram on SOCK_DGRAM. Safe only because callers hold the
    // entity's recv_mutex, so the head cannot change under us.
    RMW_UDS_LOG_WARN_THROTTLE(
      5000,
      "UDS recv (probe): runt datagram (%zd bytes, expected >= %zu) — consumed and dropped",
      n, sizeof(WireHeader));
    recv(socket_fd, nullptr, 0, MSG_DONTWAIT);
    return false;
  }

  // Scatter-read the datagram in one syscall: header into header_out, payload
  // into payload_out — the single copy out of the kernel, mirroring send_to's
  // gather-write. The resize's zero-fill is overwritten by recvmsg right
  // after; that is still far cheaper than the two extra copies it replaces.
  payload_out.resize(static_cast<size_t>(n) - sizeof(WireHeader));

  struct iovec iov[2];
  iov[0].iov_base = &header_out;
  iov[0].iov_len = sizeof(WireHeader);
  iov[1].iov_base = payload_out.data();
  iov[1].iov_len = payload_out.size();

  struct msghdr msg;
  std::memset(&msg, 0, sizeof(msg));
  msg.msg_iov = iov;
  msg.msg_iovlen = payload_out.empty() ? 1 : 2;

  n = recvmsg(socket_fd, &msg, MSG_DONTWAIT);
  if (n < 0) {
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      RMW_UDS_LOG_WARN_THROTTLE(
        1000, "UDS recv failed: %s (errno=%d)",
        std::strerror(errno), errno);
    }
    return false;
  }
  if (msg.msg_flags & MSG_TRUNC) {
    // The datagram read was bigger than the buffer sized from the probe and
    // the kernel discarded its tail. Callers serialize receives per fd
    // (recv_mutex), so this should be impossible; kept as defense because a
    // truncated read must never be delivered as a valid message. Recovery is
    // the level-triggered epoll: the fd stays readable while datagrams
    // remain, so the next wait re-drains.
    RMW_UDS_LOG_WARN_THROTTLE(
      5000,
      "UDS recv: datagram changed size between probe and read — dropped");
    return false;
  }
  if (n < static_cast<ssize_t>(sizeof(WireHeader))) {
    RMW_UDS_LOG_WARN_THROTTLE(
      5000,
      "UDS recv: runt datagram (%zd bytes, expected >= %zu) — dropped",
      n, sizeof(WireHeader));
    return false;
  }

  size_t payload_len = static_cast<size_t>(n) - sizeof(WireHeader);
  if (payload_len != header_out.payload_size) {
    // Mismatch — the sender wrote a payload_size that doesn't match what
    // actually arrived (corrupt or foreign datagram). Clamp so we never hand
    // the caller more bytes than the wire really carried.
    RMW_UDS_LOG_WARN_THROTTLE(
      5000,
      "UDS recv: payload size mismatch (got %zu, header says %u) — truncating",
      payload_len, header_out.payload_size);
    payload_len = std::min(payload_len, static_cast<size_t>(header_out.payload_size));
  }
  // In the common case this is a no-op; it only shrinks (never reallocates)
  // when the read raced with another consumer or the header disagreed.
  payload_out.resize(payload_len);

  return true;
}

std::string make_socket_path(size_t domain_id, const char * prefix)
{
  // Use PID + a globally unique counter to avoid path collisions.
  // The counter persists across the process lifetime and is unique per-process.
  // Combined with PID, this guarantees uniqueness even across process restarts
  // (different PID) and within a single process (incrementing counter).
  // Adding a time component avoids collisions when PIDs are reused by the OS.
  static std::atomic<uint32_t> counter{0};
  auto now = std::chrono::steady_clock::now().time_since_epoch();
  uint32_t time_component = static_cast<uint32_t>(
    std::chrono::duration_cast<std::chrono::microseconds>(now).count() & 0xFFFF);
  uint32_t unique_id = (counter.fetch_add(1, std::memory_order_relaxed) << 16) | time_component;
  char path[128];
  std::snprintf(path, sizeof(path), "/tmp/ros2_uds/%zu/%s_%d_%x.sock",
    domain_id, prefix, getpid(), unique_id);
  return std::string(path);
}

void close_socket(int fd, const std::string & path)
{
  if (fd >= 0) {
    close(fd);
  }
  if (!path.empty()) {
    unlink(path.c_str());
  }
}

void cleanup_orphan_socket_files(size_t domain_id)
{
  // File name format (see make_socket_path): <prefix>_<pid>_<unique>.sock
  // Parse the PID out of the filename and check /proc/<pid>. If the PID is
  // not alive in our namespace, the file is an orphan from a dead process
  // and we unlink it. Docker-safe for the same reason as registry_cleanup_stale:
  // /proc only shows PIDs visible in our PID namespace; invisible PIDs can't
  // reach this socket anyway, so unlinking is safe.
  char dir_path[128];
  std::snprintf(dir_path, sizeof(dir_path), "/tmp/ros2_uds/%zu", domain_id);

  DIR * dir = opendir(dir_path);
  if (!dir) {
    return;
  }

  struct dirent * ent;
  while ((ent = readdir(dir)) != nullptr) {
    if (ent->d_name[0] == '.') {
      continue;
    }

    const char * first_us = std::strchr(ent->d_name, '_');
    if (!first_us) {
      continue;
    }
    const char * pid_start = first_us + 1;
    char * end = nullptr;
    long pid = std::strtol(pid_start, &end, 10);
    if (end == pid_start || *end != '_' || pid <= 0) {
      continue;
    }

    char proc_path[32];
    std::snprintf(proc_path, sizeof(proc_path), "/proc/%ld", pid);
    struct stat st;
    if (stat(proc_path, &st) == -1 && errno == ENOENT) {
      char full_path[sizeof(dir_path) + sizeof(ent->d_name) + 2];
      std::snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, ent->d_name);
      unlink(full_path);
    }
  }

  closedir(dir);
}

void warn_if_sysctl_buffers_undersized()
{
  static std::once_flag once;
  std::call_once(once, []() {
      auto read_proc_int = [](const char * path) -> long {
          FILE * f = std::fopen(path, "r");
          if (!f) {
            return -1;
          }
          long v = -1;
          if (std::fscanf(f, "%ld", &v) != 1) {
            v = -1;
          }
          std::fclose(f);
          return v;
        };

      const long wmax = read_proc_int("/proc/sys/net/core/wmem_max");
      const long rmax = read_proc_int("/proc/sys/net/core/rmem_max");

      if (wmax > 0 && wmax < SEND_BUF_SIZE) {
        RMW_UDS_LOG_WARN(
          "net.core.wmem_max=%ld < requested SO_SNDBUF=%d. Large outbound "
          "datagrams will fail with EMSGSIZE. Run: sudo sysctl -w "
          "net.core.wmem_max=%d (or larger) and persist in /etc/sysctl.d/.",
          wmax, SEND_BUF_SIZE, SEND_BUF_SIZE);
      }
      if (rmax > 0 && rmax < RECV_BUF_SIZE) {
        RMW_UDS_LOG_WARN(
          "net.core.rmem_max=%ld < requested SO_RCVBUF=%d. Subscribers may "
          "drop large datagrams. Run: sudo sysctl -w net.core.rmem_max=%d "
          "(or larger) and persist in /etc/sysctl.d/.",
          rmax, RECV_BUF_SIZE, RECV_BUF_SIZE);
      }
    });
}

}  // namespace rmw_uds
