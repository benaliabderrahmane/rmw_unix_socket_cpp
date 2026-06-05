#ifndef RMW_UNIX_SOCKET_CPP__TYPES_HPP_
#define RMW_UNIX_SOCKET_CPP__TYPES_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <unistd.h>

#include "rmw/event_callback_type.h"
#include "rmw/types.h"
#include "rosidl_typesupport_fastrtps_cpp/message_type_support.h"

namespace rmw_uds
{

// Global atomic counter for unique GID generation
inline std::atomic<uint32_t> g_gid_counter{1};

// Convert introspection namespace to ROS type name ("pkg/msg/Name")
// C++ introspection uses "::" separator ("pkg::msg")
// C introspection uses "__" separator ("pkg__msg")
inline std::string make_ros_type_name(const char * ns, const char * name)
{
  std::string result(ns);
  // Replace "::" with "/" (C++ introspection)
  std::string::size_type pos = 0;
  while ((pos = result.find("::", pos)) != std::string::npos) {
    result.replace(pos, 2, "/");
  }
  // Replace "__" with "/" (C introspection)
  pos = 0;
  while ((pos = result.find("__", pos)) != std::string::npos) {
    result.replace(pos, 2, "/");
  }
  result += "/";
  result += name;
  return result;
}

struct UdsGid
{
  uint8_t data[RMW_GID_STORAGE_SIZE] = {};

  void generate()
  {
    std::memset(data, 0, sizeof(data));
    pid_t pid = getpid();
    uint32_t cnt = g_gid_counter.fetch_add(1, std::memory_order_relaxed);
    std::memcpy(data, &pid, sizeof(pid));
    std::memcpy(data + sizeof(pid), &cnt, sizeof(cnt));
  }
};

// Wire header prepended to every datagram
struct __attribute__((packed)) WireHeader
{
  uint8_t gid[RMW_GID_STORAGE_SIZE];  // 16 bytes: sender GID
  int64_t sequence_number;              // 8 bytes
  int64_t source_timestamp_ns;          // 8 bytes: ns since epoch
  uint32_t payload_size;                // 4 bytes
  uint8_t msg_type;                     // 1 byte: 0=topic, 1=request, 2=response
};

// Wire-format guard: WireHeader is blitted onto the datagram, so its packed
// layout is a cross-process / cross-build contract. DESIGN pins 16+8+8+4+1.
// A field reorder or type change must be deliberate (and bump the protocol).
static_assert(RMW_GID_STORAGE_SIZE == 16, "WireHeader assumes a 16-byte GID");
static_assert(sizeof(WireHeader) == 37, "WireHeader wire layout changed");
static_assert(offsetof(WireHeader, gid) == 0, "WireHeader layout changed");
static_assert(offsetof(WireHeader, sequence_number) == 16, "WireHeader layout changed");
static_assert(offsetof(WireHeader, source_timestamp_ns) == 24, "WireHeader layout changed");
static_assert(offsetof(WireHeader, payload_size) == 32, "WireHeader layout changed");
static_assert(offsetof(WireHeader, msg_type) == 36, "WireHeader layout changed");

// Message stored in subscription/service/client queues
struct ReceivedMessage
{
  WireHeader header;
  std::vector<uint8_t> payload;
  int64_t received_timestamp_ns;
};

// Forward declaration: publisher type defined below.
struct UdsPublisher;

// Per-context implementation data
struct UdsContext
{
  size_t domain_id = 0;
  int registry_fd = -1;
  void * registry_ptr = nullptr;
  size_t registry_size = 0;
  int send_socket_fd = -1;
  std::atomic<bool> is_shutdown{false};
  std::atomic<uint64_t> last_registry_generation{0};
  rmw_guard_condition_t * graph_guard_condition = nullptr;

  // TRANSIENT_LOCAL publishers, for wait-side cache replay on graph change.
  std::mutex transient_local_pubs_mutex;
  std::vector<UdsPublisher *> transient_local_pubs;
};

// Node data
struct UdsNode
{
  std::string name;
  std::string ns;
  UdsContext * context = nullptr;
  rmw_guard_condition_t * graph_guard_condition = nullptr;
  int32_t registry_index = -1;
};

// Cached message for TRANSIENT_LOCAL replay
struct CachedMessage
{
  WireHeader header;
  std::vector<uint8_t> payload;
};

// Publisher data
struct UdsPublisher
{
  UdsGid gid;
  std::string topic_name;
  std::string type_name;
  rmw_qos_profile_t qos;
  const rosidl_message_type_support_t * type_support = nullptr;
  const message_type_support_callbacks_t * callbacks = nullptr;
  std::atomic<int64_t> sequence_number{1};
  int32_t registry_index = -1;
  UdsContext * context = nullptr;
  UdsNode * node = nullptr;

  // PERFORMANCE: cache the matching subscriber socket paths to avoid locking
  // the registry on every publish. We re-query only when the registry's
  // generation counter changes (graph topology actually changed).
  std::mutex sub_cache_mutex;
  uint64_t cached_generation = 0;
  std::vector<std::string> cached_subscriber_paths;

  // TRANSIENT_LOCAL: cache of last N messages for late-joining subscribers
  std::mutex cache_mutex;
  std::deque<CachedMessage> message_cache;
  std::set<std::string> known_subscriber_paths;  // subs we've already replayed to
};

// Subscription data
struct UdsSubscription
{
  UdsGid gid;
  std::string topic_name;
  std::string type_name;
  rmw_qos_profile_t qos;
  const rosidl_message_type_support_t * type_support = nullptr;
  const message_type_support_callbacks_t * callbacks = nullptr;
  int socket_fd = -1;
  std::string socket_path;
  std::mutex queue_mutex;
  std::deque<ReceivedMessage> message_queue;
  size_t queue_depth = 10;
  std::atomic<uint64_t> reception_seq{1};
  int32_t registry_index = -1;
  UdsContext * context = nullptr;
  UdsNode * node = nullptr;
  // Callback support
  std::mutex callback_mutex;
  rmw_event_callback_t on_new_message_cb = nullptr;
  const void * on_new_message_user_data = nullptr;
};

// Cached client routing entry for service responses (path + GID)
struct CachedClient
{
  std::string socket_path;
  uint8_t gid[RMW_GID_STORAGE_SIZE];
};

// Service server data
struct UdsService
{
  UdsGid gid;
  std::string service_name;
  std::string type_name;
  rmw_qos_profile_t qos;
  const message_type_support_callbacks_t * request_callbacks = nullptr;
  const message_type_support_callbacks_t * response_callbacks = nullptr;
  int socket_fd = -1;
  std::string socket_path;
  std::mutex queue_mutex;
  std::deque<ReceivedMessage> request_queue;
  int32_t registry_index = -1;
  UdsContext * context = nullptr;
  UdsNode * node = nullptr;

  // PERFORMANCE: cache client (GID -> socket path) so send_response doesn't
  // hit the registry mutex per response.
  std::mutex client_cache_mutex;
  uint64_t cached_generation = 0;
  std::vector<CachedClient> cached_clients;

  // Callback support
  std::mutex callback_mutex;
  rmw_event_callback_t on_new_request_cb = nullptr;
  const void * on_new_request_user_data = nullptr;
};

// Service client data
struct UdsClient
{
  UdsGid gid;
  std::string service_name;
  std::string type_name;
  rmw_qos_profile_t qos;
  const message_type_support_callbacks_t * request_callbacks = nullptr;
  const message_type_support_callbacks_t * response_callbacks = nullptr;
  int socket_fd = -1;
  std::string socket_path;
  std::mutex queue_mutex;
  std::deque<ReceivedMessage> response_queue;
  std::atomic<int64_t> sequence_number{1};
  int32_t registry_index = -1;
  UdsContext * context = nullptr;
  UdsNode * node = nullptr;

  // PERFORMANCE: cache the service socket path so send_request doesn't
  // hit the registry mutex on every call.
  std::mutex svc_cache_mutex;
  uint64_t cached_generation = 0;
  std::string cached_service_path;
  bool cached_is_available = false;

  // Callback support
  std::mutex callback_mutex;
  rmw_event_callback_t on_new_response_cb = nullptr;
  const void * on_new_response_user_data = nullptr;
};

// Guard condition data
struct UdsGuardCondition
{
  int eventfd_fd = -1;
};

// Wait set data
struct UdsWaitSet
{
  int epoll_fd = -1;
};

}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__TYPES_HPP_
