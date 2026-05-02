#include "transport.hpp"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <dirent.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

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
    return -1;
  }

  // Remove stale socket file
  unlink(path.c_str());

  struct sockaddr_un addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

  if (bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) != 0) {
    close(fd);
    return -1;
  }

  // Set large receive buffer
  int buf = RECV_BUF_SIZE;
  setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));

  return fd;
}

int create_send_socket()
{
  int fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return -1;
  }

  int buf = SEND_BUF_SIZE;
  setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));

  return fd;
}

bool send_to(
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
  return sent >= 0;
}

bool recv_from(
  int socket_fd,
  WireHeader & header_out,
  std::vector<uint8_t> & payload_out)
{
  // Peek to get message size
  // Use a fixed buffer approach: header + payload up to recv buffer
  static thread_local std::vector<uint8_t> recv_buf(256 * 1024);  // 256KB initial

  ssize_t n = recv(socket_fd, recv_buf.data(), recv_buf.size(),
    MSG_DONTWAIT | MSG_PEEK | MSG_TRUNC);

  if (n <= 0) {
    return false;
  }

  // Resize if needed
  if (static_cast<size_t>(n) > recv_buf.size()) {
    recv_buf.resize(static_cast<size_t>(n));
  }

  // Actually receive the message
  n = recv(socket_fd, recv_buf.data(), recv_buf.size(), MSG_DONTWAIT);
  if (n < static_cast<ssize_t>(sizeof(WireHeader))) {
    return false;
  }

  std::memcpy(&header_out, recv_buf.data(), sizeof(WireHeader));

  size_t payload_len = static_cast<size_t>(n) - sizeof(WireHeader);
  if (payload_len != header_out.payload_size) {
    // Mismatch — use actual received size
    payload_len = std::min(payload_len, static_cast<size_t>(header_out.payload_size));
  }

  payload_out.assign(
    recv_buf.data() + sizeof(WireHeader),
    recv_buf.data() + sizeof(WireHeader) + payload_len);

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

}  // namespace rmw_uds
