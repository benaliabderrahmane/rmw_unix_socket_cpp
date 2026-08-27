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

#include "identifier.hpp"
#include "logging.hpp"
#include "registry.hpp"
#include "serialization.hpp"
#include "transport.hpp"
#include "types.hpp"

#include <atomic>
#include <chrono>
#include <cstring>

#include "rcutils/strdup.h"
#include "rmw/allocators.h"
#include "rmw/check_type_identifiers_match.h"
#include "rmw/error_handling.h"
#include "rmw/rmw.h"

static rmw_qos_profile_t resolve_qos(const rmw_qos_profile_t * qos)
{
  rmw_qos_profile_t resolved = *qos;
  if (resolved.history == RMW_QOS_POLICY_HISTORY_SYSTEM_DEFAULT) {
    resolved.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
  }
  if (resolved.depth == 0) {
    resolved.depth = 10;
  }
  if (resolved.reliability == RMW_QOS_POLICY_RELIABILITY_SYSTEM_DEFAULT ||
    resolved.reliability == RMW_QOS_POLICY_RELIABILITY_BEST_AVAILABLE)
  {
    resolved.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
  }
  if (resolved.durability == RMW_QOS_POLICY_DURABILITY_SYSTEM_DEFAULT ||
    resolved.durability == RMW_QOS_POLICY_DURABILITY_BEST_AVAILABLE)
  {
    resolved.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;
  }
  return resolved;
}

static int64_t system_now_ns()
{
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    now.time_since_epoch()).count();
}

extern "C"
{

rmw_publisher_t * rmw_create_publisher(
  const rmw_node_t * node,
  const rosidl_message_type_support_t * type_support,
  const char * topic_name,
  const rmw_qos_profile_t * qos_profile,
  const rmw_publisher_options_t * publisher_options)
{
  (void)publisher_options;
  RMW_CHECK_ARGUMENT_FOR_NULL(node, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(type_support, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(topic_name, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(qos_profile, nullptr);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return nullptr);

  auto * node_data = static_cast<rmw_uds::UdsNode *>(node->data);
  auto * ctx = node_data->context;

  const auto * callbacks = rmw_uds::get_callbacks(type_support);
  if (!callbacks) {
    RMW_SET_ERROR_MSG("failed to get fastrtps type support callbacks");
    return nullptr;
  }

  auto * pub_data = new (std::nothrow) rmw_uds::UdsPublisher();
  if (!pub_data) {
    RMW_SET_ERROR_MSG("failed to allocate publisher data");
    return nullptr;
  }

  pub_data->gid.generate(ctx->context_id);
  pub_data->topic_name = topic_name;
  pub_data->type_name = rmw_uds::make_ros_type_name(
    callbacks->message_namespace_, callbacks->message_name_);
  pub_data->qos = resolve_qos(qos_profile);
  pub_data->type_support = type_support;
  pub_data->callbacks = callbacks;
  pub_data->context = ctx;
  pub_data->node = node_data;

  // TRANSIENT_LOCAL: create the latched cache STRICTLY BEFORE registry_add,
  // so a slot visible to any subscriber always names a mappable, validated
  // segment. On failure the publisher runs latch-less (live delivery is
  // unaffected, late joiners get no history) and the slot publishes an empty
  // name, which pullers skip.
  if (pub_data->qos.durability == RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL) {
    if (!rmw_uds::tl_ring_create(
        pub_data->tl_ring, ctx->domain_id, pub_data->gid.data,
        pub_data->qos.depth))
    {
      RMW_UDS_LOG_ERROR(
        "latched cache unavailable for TRANSIENT_LOCAL topic '%s' — late "
        "joiners will not receive retained samples", topic_name);
    }
  }

  // Register in shared memory
  auto * header = rmw_uds::registry_header(ctx->registry_ptr);
  rmw_uds::RegistryEntry entry;
  std::memset(&entry, 0, sizeof(entry));
  entry.type = rmw_uds::ENTRY_PUBLISHER;
  entry.pid = getpid();
  std::memcpy(entry.gid, pub_data->gid.data, sizeof(entry.gid));
  std::strncpy(entry.node_name, node_data->name.c_str(), sizeof(entry.node_name) - 1);
  std::strncpy(entry.node_namespace, node_data->ns.c_str(), sizeof(entry.node_namespace) - 1);
  std::strncpy(entry.topic_name, topic_name, sizeof(entry.topic_name) - 1);
  std::strncpy(entry.type_name, pub_data->type_name.c_str(), sizeof(entry.type_name) - 1);
  // Publisher slots carried no socket_path until now; a TRANSIENT_LOCAL
  // publisher publishes its latched-cache shm name here so late joiners can
  // locate and pull the history themselves (see tl_ring_pull).
  std::strncpy(entry.socket_path, pub_data->tl_ring.shm_name.c_str(),
    sizeof(entry.socket_path) - 1);
  entry.qos_reliability = static_cast<uint8_t>(pub_data->qos.reliability);
  entry.qos_durability = static_cast<uint8_t>(pub_data->qos.durability);
  entry.qos_history = static_cast<uint8_t>(pub_data->qos.history);
  entry.qos_depth = static_cast<uint32_t>(pub_data->qos.depth);

  pub_data->registry_index = rmw_uds::registry_add(header, entry);

  if (pub_data->registry_index < 0) {
    RMW_UDS_LOG_ERROR(
      "registry full — cannot create publisher for topic '%s' (node=%s%s). "
      "Increase REGISTRY_MAX_ENTRIES or check for slot leaks.",
      topic_name,
      node_data->ns.c_str(), node_data->name.c_str());
    rmw_uds::tl_ring_close(pub_data->tl_ring);
    delete pub_data;
    RMW_SET_ERROR_MSG("registry full — cannot create publisher");
    return nullptr;
  }

  auto * pub = rmw_publisher_allocate();
  if (!pub) {
    rmw_uds::registry_remove(header, pub_data->registry_index);
    rmw_uds::tl_ring_close(pub_data->tl_ring);
    delete pub_data;
    RMW_SET_ERROR_MSG("failed to allocate rmw_publisher_t");
    return nullptr;
  }

  pub->implementation_identifier = rmw_uds::identifier;
  pub->data = pub_data;
  pub->topic_name = rcutils_strdup(topic_name, node->context->options.allocator);
  pub->options = publisher_options ? *publisher_options : rmw_get_default_publisher_options();
  pub->can_loan_messages = false;

  return pub;
}

rmw_ret_t rmw_destroy_publisher(rmw_node_t * node, rmw_publisher_t * publisher)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher, publisher->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(publisher->data);
  if (pub_data) {
    if (pub_data->context && pub_data->registry_index >= 0) {
      auto * header = rmw_uds::registry_header(pub_data->context->registry_ptr);
      rmw_uds::registry_remove(header, pub_data->registry_index);
    }
    // Slot first, cache second: a puller that raced the removal either saw
    // the slot and pulled a still-valid segment, or saw no slot at all.
    rmw_uds::tl_ring_close(pub_data->tl_ring);
    rmw_uds::shm_writer_close(pub_data->shm_ring);
    delete pub_data;
  }

  rcutils_allocator_t alloc = node->context ?
    node->context->options.allocator : rcutils_get_default_allocator();
  if (publisher->topic_name) {
    alloc.deallocate(const_cast<char *>(publisher->topic_name), alloc.state);
  }
  rmw_publisher_free(publisher);
  return RMW_RET_OK;
}

// Refresh the generation-keyed subscriber-path cache if `current_gen` moved,
// and return the (possibly shared) path list. The memoization invariant that
// keeps the pull-based replay proof sound: cached_generation only ever
// advances to a value loaded BEFORE the query ran, under sub_cache_mutex —
// so current_gen == cached_generation implies the cache already saw every
// subscriber whose registration that generation covers.
static std::shared_ptr<const std::vector<std::string>> refresh_sub_paths(
  rmw_uds::UdsPublisher * pub_data,
  rmw_uds::RegistryHeader * header,
  uint64_t current_gen)
{
  std::lock_guard<std::mutex> lock(pub_data->sub_cache_mutex);
  // Monotonic guard (not !=): a thread that loaded an older generation and
  // lost the race must not move cached_generation backward — the invariant
  // above is what the pull-based replay reasoning cites. A lower current_gen
  // means the cached list was built by a newer query and is already a
  // superset of the subscribers that generation covers.
  if (current_gen > pub_data->cached_generation) {
    auto subs = rmw_uds::registry_query(
      header, rmw_uds::ENTRY_SUBSCRIPTION, pub_data->topic_name.c_str(),
      nullptr, nullptr);
    auto fresh = std::make_shared<std::vector<std::string>>();
    fresh->reserve(subs.size());
    for (const auto & s : subs) {
      if (!s.socket_path.empty()) {
        fresh->push_back(s.socket_path);
      }
    }
    pub_data->cached_generation = current_gen;
    pub_data->cached_subscriber_paths = std::move(fresh);
  }
  return pub_data->cached_subscriber_paths;
}

// Latch a TRANSIENT_LOCAL sample into the pull cache and fan it out live.
// Shared by rmw_publish and rmw_publish_serialized_message.
//
// Ordering is the store-buffering pair that makes late-joiner replay
// lossless with no publisher-side wakeup (mirrored by rmw_create_subscription,
// which does registry_add -> seq_cst fence -> pull):
//   1. write the ring record (sequence number assigned under cache_mutex, so
//      slot commit order == sequence order and the puller's watermark dedup
//      is sound),
//   2. seq_cst fence,
//   3. FRESH generation load -> refresh the subscriber cache if stale,
//   4. fan out.
// If this publish misses a joining subscriber's registration, the fence pair
// guarantees the subscriber's pull sees the record; if it sees the
// registration, the sample goes out as a datagram; the overlap is deduped by
// the subscriber against its pull watermark. Reusing a generation loaded
// before step 1 would reopen the lost-sample window — never do that here.
static rmw_ret_t transient_local_publish(
  rmw_uds::UdsPublisher * pub_data,
  rmw_uds::WireHeader hdr,
  const std::vector<uint8_t> & payload)
{
  // Stage an over-cap payload BEFORE taking cache_mutex: staging is
  // shm_open/fallocate/mmap (up to milliseconds), reads only the payload,
  // the immutable domain id, and a global atomic counter — nothing
  // cache_mutex guards. A staging failure just means this sample is not
  // latched; the live path is unaffected.
  rmw_uds::ShmPayloadDescriptor staged_desc;
  std::unique_ptr<rmw_uds::DurableShmSegment> staged_seg;
  if (payload.size() > rmw_uds::TL_EMBED_CAP) {
    staged_seg = rmw_uds::shm_stage_durable(
      pub_data->context->domain_id, payload.data(), payload.size(),
      staged_desc);
    if (!staged_seg) {
      RMW_UDS_LOG_WARN_THROTTLE(
        5000,
        "latched sample (%zu bytes) could not be staged in shared memory — "
        "not replayable to late joiners",
        payload.size());
    }
  }
  const bool staged = static_cast<bool>(staged_seg);

  // Latch, fence, refresh, and fan out under cache_mutex, so concurrent
  // publishes on this publisher hit the wire in sequence order (the deleted
  // push code held the same lock across its sends). The evicted slot's
  // durable segment destructs (munmap + shm_unlink) after the lock drops.
  std::unique_ptr<rmw_uds::DurableShmSegment> evicted;
  bool latched = false;
  bool config_error = false;
  {
    std::lock_guard<std::mutex> lock(pub_data->cache_mutex);
    hdr.sequence_number =
      pub_data->sequence_number.fetch_add(1, std::memory_order_relaxed);
    latched = rmw_uds::tl_ring_latch(
      pub_data->tl_ring, hdr, payload.data(), payload.size(),
      staged ? &staged_desc : nullptr, std::move(staged_seg), evicted);
    std::atomic_thread_fence(std::memory_order_seq_cst);

    auto * header = rmw_uds::registry_header(pub_data->context->registry_ptr);
    const uint64_t current_gen = rmw_uds::registry_generation(header);
    auto sub_paths = refresh_sub_paths(pub_data, header, current_gen);

    // Live fan-out: payloads at or above the datagram threshold reuse the
    // durable segment the latch just committed (same bytes, same
    // descriptor). If the latch did NOT commit, the segment is already
    // destroyed, so fall back to the inline send, which surfaces EMSGSIZE.
    const uint8_t * wire_data = payload.data();
    size_t wire_size = payload.size();
    if (staged && latched && payload.size() >= rmw_uds::SHM_PAYLOAD_THRESHOLD) {
      hdr.msg_type |= rmw_uds::SHM_PAYLOAD_FLAG;
      hdr.payload_size = static_cast<uint32_t>(sizeof(staged_desc));
      wire_data = reinterpret_cast<const uint8_t *>(&staged_desc);
      wire_size = sizeof(staged_desc);
    }

    if (sub_paths) {
      for (const auto & path : *sub_paths) {
        if (rmw_uds::send_to(
            pub_data->context->send_socket_fd,
            path, hdr, wire_data, wire_size) == rmw_uds::SendResult::ConfigError)
        {
          config_error = true;
        }
      }
    }
  }
  if (config_error) {
    RMW_SET_ERROR_MSG(
      "UDS send: TRANSIENT_LOCAL message too large for kernel send buffer "
      "(EMSGSIZE). Raise net.core.wmem_max or reduce message size.");
    return RMW_RET_ERROR;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_publish(
  const rmw_publisher_t * publisher,
  const void * ros_message,
  rmw_publisher_allocation_t * allocation)
{
  (void)allocation;
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(ros_message, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher, publisher->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(publisher->data);

  // Build wire header. payload_size is filled per-path below; the sequence
  // number is assigned per-path too — the TRANSIENT_LOCAL path assigns it
  // inside its latch critical section so slot commit order equals sequence
  // order (see transient_local_publish).
  rmw_uds::WireHeader hdr;
  std::memset(&hdr, 0, sizeof(hdr));
  std::memcpy(hdr.gid, pub_data->gid.data, sizeof(hdr.gid));
  hdr.source_timestamp_ns = system_now_ns();
  hdr.msg_type = 0;  // topic message

  // TRANSIENT_LOCAL: latch into the pull cache, then fan out (ordering
  // documented at transient_local_publish). Serializes to a heap payload —
  // the latch needs the bytes.
  if (pub_data->qos.durability == RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL) {
    std::vector<uint8_t> payload;
    if (!rmw_uds::serialize(ros_message, pub_data->callbacks, payload)) {
      // Throttled — a broken type/serializer would otherwise log per-publish.
      RMW_UDS_LOG_ERROR_THROTTLE(
        1000,
        "rmw_publish: CDR serialization failed for topic '%s' (type '%s')",
        pub_data->topic_name.c_str(), pub_data->type_name.c_str());
      RMW_SET_ERROR_MSG("failed to serialize message");
      return RMW_RET_ERROR;
    }
    hdr.payload_size = static_cast<uint32_t>(payload.size());
    return transient_local_publish(pub_data, hdr, payload);
  }

  hdr.sequence_number =
    pub_data->sequence_number.fetch_add(1, std::memory_order_relaxed);

  // PERFORMANCE: only lock the registry when the graph generation has changed
  // since we last cached the subscriber list. The hot path is purely local.
  auto * header = rmw_uds::registry_header(pub_data->context->registry_ptr);
  uint64_t current_gen = rmw_uds::registry_generation(header);
  auto sub_paths = refresh_sub_paths(pub_data, header, current_gen);

  // Non-latched path: serialize once, choosing the destination by size — a
  // large payload is written directly into the reserved ring record (no heap
  // payload, no staging copy) and only a descriptor crosses the socket; a
  // small one (or any shm failure) is serialized into `payload` and sent
  // inline.
  std::vector<uint8_t> payload;
  rmw_uds::ShmPayloadDescriptor desc;
  rmw_uds::OutboundPayload wire{nullptr, 0};
  if (!rmw_uds::shm_serialize_prepare_send(
      pub_data->shm_ring, pub_data->shm_mutex, pub_data->context->domain_id,
      ros_message, pub_data->callbacks, hdr, desc, payload, wire))
  {
    RMW_UDS_LOG_ERROR_THROTTLE(
      1000,
      "rmw_publish: CDR serialization failed for topic '%s' (type '%s')",
      pub_data->topic_name.c_str(), pub_data->type_name.c_str());
    RMW_SET_ERROR_MSG("failed to serialize message");
    return RMW_RET_ERROR;
  }

  // Surface EMSGSIZE; soft drops (EAGAIN/ENOENT) are logged in send_to.
  bool config_error = false;
  if (sub_paths) {
    for (const auto & path : *sub_paths) {
      if (rmw_uds::send_to(
          pub_data->context->send_socket_fd,
          path, hdr, wire.data, wire.size) == rmw_uds::SendResult::ConfigError)
      {
        config_error = true;
      }
    }
  }
  if (config_error) {
    RMW_SET_ERROR_MSG(
      "UDS send: message too large for kernel send buffer (EMSGSIZE). "
      "Raise net.core.wmem_max or reduce message size.");
    return RMW_RET_ERROR;
  }

  return RMW_RET_OK;
}

rmw_ret_t rmw_publish_serialized_message(
  const rmw_publisher_t * publisher,
  const rmw_serialized_message_t * serialized_message,
  rmw_publisher_allocation_t * allocation)
{
  (void)allocation;
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(serialized_message, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher, publisher->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(publisher->data);

  rmw_uds::WireHeader hdr;
  std::memset(&hdr, 0, sizeof(hdr));
  std::memcpy(hdr.gid, pub_data->gid.data, sizeof(hdr.gid));
  hdr.source_timestamp_ns = system_now_ns();
  hdr.payload_size = static_cast<uint32_t>(serialized_message->buffer_length);
  hdr.msg_type = 0;

  // TRANSIENT_LOCAL: latch + fan out through the same pull-cache path as
  // rmw_publish, so a serialized latched payload (including one above the
  // datagram cap) reaches late joiners too. Sequence number assigned inside
  // the latch critical section (see transient_local_publish).
  if (pub_data->qos.durability == RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL) {
    return transient_local_publish(
      pub_data, hdr,
      std::vector<uint8_t>(
        serialized_message->buffer,
        serialized_message->buffer + serialized_message->buffer_length));
  }

  hdr.sequence_number =
    pub_data->sequence_number.fetch_add(1, std::memory_order_relaxed);

  // Reuse the cached subscriber list (refresh only on graph change)
  auto * header = rmw_uds::registry_header(pub_data->context->registry_ptr);
  uint64_t current_gen = rmw_uds::registry_generation(header);
  auto sub_paths = refresh_sub_paths(pub_data, header, current_gen);

  // Large non-TL payloads: stage once into the cycling ring, fan out descriptors.
  rmw_uds::ShmPayloadDescriptor desc;
  auto wire = rmw_uds::shm_prepare_send(
    pub_data->shm_ring, pub_data->shm_mutex, pub_data->context->domain_id,
    serialized_message->buffer, serialized_message->buffer_length, hdr, desc);

  bool config_error = false;
  if (sub_paths) {
    for (const auto & path : *sub_paths) {
      if (rmw_uds::send_to(
          pub_data->context->send_socket_fd,
          path, hdr, wire.data, wire.size) == rmw_uds::SendResult::ConfigError)
      {
        config_error = true;
      }
    }
  }
  if (config_error) {
    RMW_SET_ERROR_MSG(
      "UDS send: serialized message too large for kernel send buffer (EMSGSIZE). "
      "Raise net.core.wmem_max or reduce message size.");
    return RMW_RET_ERROR;
  }

  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_count_matched_subscriptions(
  const rmw_publisher_t * publisher,
  size_t * subscription_count)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(subscription_count, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher, publisher->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(publisher->data);
  auto * header = rmw_uds::registry_header(pub_data->context->registry_ptr);

  auto subs = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_SUBSCRIPTION, pub_data->topic_name.c_str(), nullptr, nullptr);

  *subscription_count = subs.size();
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_get_actual_qos(
  const rmw_publisher_t * publisher,
  rmw_qos_profile_t * qos)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(qos, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher, publisher->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(publisher->data);
  *qos = pub_data->qos;
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_gid_for_publisher(
  const rmw_publisher_t * publisher,
  rmw_gid_t * gid)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(gid, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher, publisher->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(publisher->data);
  gid->implementation_identifier = rmw_uds::identifier;
  std::memcpy(gid->data, pub_data->gid.data, RMW_GID_STORAGE_SIZE);
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_assert_liveliness(const rmw_publisher_t * publisher)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_wait_for_all_acked(
  const rmw_publisher_t * publisher,
  rmw_time_t wait_timeout)
{
  (void)publisher;
  (void)wait_timeout;
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_serialized_message_size(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds,
  size_t * size)
{
  (void)type_support;
  (void)message_bounds;
  (void)size;
  RMW_SET_ERROR_MSG("rmw_get_serialized_message_size not supported");
  return RMW_RET_UNSUPPORTED;
}

}  // extern "C"
