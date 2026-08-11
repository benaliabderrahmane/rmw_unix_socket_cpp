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

#ifndef RMW_UNIX_SOCKET_CPP__TYPES_HPP_
#define RMW_UNIX_SOCKET_CPP__TYPES_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "rmw/event_callback_type.h"
#include "rmw/types.h"
#include "rosidl_typesupport_fastrtps_cpp/message_type_support.h"

#include "shm_transport.hpp"

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

// Process-wide monotonic id, stamped on every entity that can be armed in a
// wait set. Never reused, so a wait set can tell "the same fd armed for the
// same entity" from "the same fd number handed to a different entity after a
// close".
inline uint64_t next_entity_uid()
{
  static std::atomic<uint64_t> counter{1};
  return counter.fetch_add(1, std::memory_order_relaxed);
}

struct UdsGid
{
  uint8_t data[RMW_GID_STORAGE_SIZE] = {};

  // bytes 0-3: process PID. bytes 4-7: monotonic counter, unique per entity
  // within this process. bytes 8-15: `context_id`, identifying the
  // rmw_context_t that owns the entity this GID is generated for (see
  // UdsContext::context_id), so any receiver can tell — purely from the
  // bytes already on the wire — whether a message came from a publisher
  // living in the same context as itself (ignore_local_publications).
  void generate(uint64_t context_id)
  {
    std::memset(data, 0, sizeof(data));
    static_assert(RMW_GID_STORAGE_SIZE == 16, "UdsGid assumes a 16-byte GID");
    static_assert(sizeof(pid_t) == 4, "UdsGid assumes a 32-bit pid_t");
    const uint32_t pid = static_cast<uint32_t>(getpid());
    const uint32_t cnt = g_gid_counter.fetch_add(1, std::memory_order_relaxed);
    std::memcpy(data + 0, &pid, sizeof(pid));
    std::memcpy(data + 4, &cnt, sizeof(cnt));
    std::memcpy(data + 8, &context_id, sizeof(context_id));
  }
};

// A context id unique to this rmw_context_t. Deliberately NOT derived from
// getpid(): PIDs are only unique within a PID namespace, so two containers
// (e.g. separate docker-compose services) sharing the same machine/IPC can
// observe the same pid at the same time, which would make a pid-based id
// collide. Instead draw the id from the kernel's entropy source — with 64
// random bits, an accidental collision between concurrently running contexts
// is astronomically unlikely regardless of process/container topology.
inline uint64_t generate_context_id()
{
  static std::random_device rd;
  uint64_t hi = rd();
  uint64_t lo = rd();
  return (hi << 32) | lo;
}

// Wire header prepended to every datagram
struct __attribute__((packed)) WireHeader
{
  uint8_t gid[RMW_GID_STORAGE_SIZE];  // 16 bytes: sender GID
  int64_t sequence_number;              // 8 bytes
  int64_t source_timestamp_ns;          // 8 bytes: ns since epoch
  uint32_t payload_size;                // 4 bytes
  uint8_t msg_type;                     // 1 byte: 0=topic, 1=request, 2=response
                                        //   high bit = SHM_PAYLOAD_FLAG
                                        //   (see shm_transport.hpp)
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

// True if `hdr` was sent by a publisher created from `context_id` — i.e. the
// rmw_context_t that owns the comparing subscription. UdsGid::generate()
// embeds the sender's context id in the trailing 8 bytes of the GID, which is
// copied verbatim into WireHeader::gid on every datagram, so this implements
// the rmw "same context" contract (ignore_local_publications) exactly, with
// no extra wire traffic.
inline bool is_same_context(const WireHeader & hdr, uint64_t context_id)
{
  uint64_t sender_context_id;
  std::memcpy(&sender_context_id, hdr.gid + sizeof(pid_t) + sizeof(uint32_t), sizeof(sender_context_id));
  return sender_context_id == context_id;
}

// Forward declaration: publisher type defined below.
struct UdsPublisher;

// Per-context implementation data
struct UdsContext
{
  size_t domain_id = 0;
  // Unique id for this rmw_context_t (see generate_context_id()), embedded in
  // every GID generated by entities created under it. Assigned once in rmw_init.
  uint64_t context_id = 0;
  int registry_fd = -1;
  void * registry_ptr = nullptr;
  size_t registry_size = 0;
  int send_socket_fd = -1;
  std::atomic<bool> is_shutdown{false};
  std::atomic<uint64_t> last_registry_generation{0};

  // Doorbell: a bound datagram socket other processes ring (one octet) after
  // any registry mutation, so a blocked rmw_wait re-checks the registry
  // without polling. Registered in the registry as ENTRY_DOORBELL; the slot's
  // teardown unlinks the socket file (graceful or via the stale-PID reaper).
  int doorbell_fd = -1;
  int32_t doorbell_registry_index = -1;

  // Per-node graph guard conditions (see rmw_node_get_graph_guard_condition),
  // triggered from rmw_wait when the registry generation changes. Guarded by
  // the mutex; rmw_destroy_node removes its entry before destroying the GC.
  std::mutex graph_gcs_mutex;
  std::vector<rmw_guard_condition_t *> graph_gcs;

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

// Cached message for TRANSIENT_LOCAL replay. For large payloads, `payload`
// holds a ShmPayloadDescriptor (header.msg_type carries SHM_PAYLOAD_FLAG) and
// `shm_seg` owns the durable segment the descriptor points at; the segment
// lives exactly as long as this cache entry, so it is replayable to late
// joiners and unlinked when the entry is evicted. Small payloads keep the
// inline bytes and leave shm_seg null.
struct CachedMessage
{
  WireHeader header;
  std::vector<uint8_t> payload;
  std::unique_ptr<DurableShmSegment> shm_seg;
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
  // Copy-on-write: swapped wholesale on graph-generation change so the publish/
  // wait hot path copies one refcount instead of N strings. Null until first refresh.
  std::shared_ptr<const std::vector<std::string>> cached_subscriber_paths;

  // TRANSIENT_LOCAL: cache of last N messages for late-joining subscribers
  std::mutex cache_mutex;
  std::deque<CachedMessage> message_cache;
  std::set<std::string> known_subscriber_paths;  // subs we've already replayed to

  // Large payloads: per-publisher /dev/shm ring (created lazily on the first
  // payload >= SHM_PAYLOAD_THRESHOLD). shm_mutex serializes staging when
  // several threads publish on the same publisher.
  std::mutex shm_mutex;
  ShmRingWriter shm_ring;
};

// Subscription data
struct UdsSubscription
{
  uint64_t uid = next_entity_uid();
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
  // rmw_subscription_options_t::ignore_local_publications, copied at creation
  // time (used by drain_subscription()/drain_socket()).
  bool ignore_local_publications = false;
  // Callback support
  std::mutex callback_mutex;
  rmw_event_callback_t on_new_message_cb = nullptr;
  const void * on_new_message_user_data = nullptr;

  // Large payloads: mapped publisher rings this subscription reads from
  // (internally locked; see ShmReaderCache).
  ShmReaderCache shm_cache;
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
  uint64_t uid = next_entity_uid();
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

  // Large request/response payloads (>= SHM_PAYLOAD_THRESHOLD): shm_ring stages
  // outgoing responses; shm_cache resolves descriptors on incoming requests.
  std::mutex shm_mutex;
  ShmRingWriter shm_ring;
  ShmReaderCache shm_cache;

  // Callback support
  std::mutex callback_mutex;
  rmw_event_callback_t on_new_request_cb = nullptr;
  const void * on_new_request_user_data = nullptr;
};

// Service client data
struct UdsClient
{
  uint64_t uid = next_entity_uid();
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

  // Large request/response payloads (>= SHM_PAYLOAD_THRESHOLD): shm_ring stages
  // outgoing requests; shm_cache resolves descriptors on incoming responses.
  std::mutex shm_mutex;
  ShmRingWriter shm_ring;
  ShmReaderCache shm_cache;

  // Callback support
  std::mutex callback_mutex;
  rmw_event_callback_t on_new_response_cb = nullptr;
  const void * on_new_response_user_data = nullptr;
};

// Guard condition data
struct UdsGuardCondition
{
  // Wait-set arming identity; see next_entity_uid().
  uint64_t uid = next_entity_uid();
  int eventfd_fd = -1;
};

// What a given epoll fd was armed for. Lets rmw_wait map an epoll result back
// to its owner without scanning the wait set, and skip the epoll_ctl when the
// fd is already armed for the same entity.
enum ArmedKind : uint8_t
{
  ARMED_SUBSCRIPTION = 0,
  ARMED_SERVICE,
  ARMED_CLIENT,
  ARMED_GUARD_CONDITION,
  ARMED_DOORBELL,
};

struct ArmedEntry
{
  uint8_t kind = ARMED_SUBSCRIPTION;  // ArmedKind
  void * entity = nullptr;            // UdsSubscription * etc, for the drain
  uint64_t uid = 0;                   // 0 for the doorbell, which has no entity
};

// Wait set data
struct UdsWaitSet
{
  int epoll_fd = -1;
  // Set at rmw_create_wait_set. The top-of-wait replay/graph check needs the
  // context even when the wait set holds only guard conditions (rclcpp's
  // GraphListener), so it cannot be scavenged from the waited-on entities.
  UdsContext * context = nullptr;
  // fd -> what it was armed for. Survives across rmw_wait calls, which is what
  // lets the arming pass cost no syscall in the steady state. Entries for
  // destroyed entities are harmless: closing the fd removes it from the epoll
  // set, so it can never be reported again, and a fd number reused by a new
  // entity is re-armed because the uid differs.
  std::unordered_map<int, ArmedEntry> armed;
};

}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__TYPES_HPP_
