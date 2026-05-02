#ifndef RMW_UNIX_SOCKET_CPP__TRANSPORT_HPP_
#define RMW_UNIX_SOCKET_CPP__TRANSPORT_HPP_

#include <cstdint>
#include <string>
#include <vector>

#include "types.hpp"

namespace rmw_uds
{

// Ensure the socket directory exists for the given domain_id.
// Returns the directory path.
std::string ensure_socket_dir(size_t domain_id);

// Create a bound SOCK_DGRAM Unix domain socket at the given path.
// Returns fd >= 0 on success, -1 on failure.
int create_bound_socket(const std::string & path);

// Create an unbound SOCK_DGRAM Unix domain socket for sending.
// Returns fd >= 0 on success, -1 on failure.
int create_send_socket();

// Send a datagram (header + payload) to the given socket path.
// Returns true on success.
bool send_to(
  int send_fd,
  const std::string & dest_path,
  const WireHeader & header,
  const uint8_t * payload,
  size_t payload_size);

// Receive a single datagram from a non-blocking socket.
// Returns true if a message was received, false if EAGAIN/EWOULDBLOCK or error.
bool recv_from(
  int socket_fd,
  WireHeader & header_out,
  std::vector<uint8_t> & payload_out);

// Generate a unique socket path for the given prefix and domain_id.
std::string make_socket_path(size_t domain_id, const char * prefix);

// Close a socket and unlink its path.
void close_socket(int fd, const std::string & path);

// Scan /tmp/ros2_uds/<domain_id>/ for .sock files whose owner PID (encoded
// in the filename) is no longer alive in our PID namespace, and unlink them.
// Safe to call at startup to clean up after ungraceful shutdowns.
void cleanup_orphan_socket_files(size_t domain_id);

}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__TRANSPORT_HPP_
