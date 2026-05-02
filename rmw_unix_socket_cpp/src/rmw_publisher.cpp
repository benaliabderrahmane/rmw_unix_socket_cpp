#include "identifier.hpp"
#include "registry.hpp"
#include "serialization.hpp"
#include "transport.hpp"
#include "types.hpp"

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

static int64_t now_ns()
{
  auto now = std::chrono::steady_clock::now();
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

  pub_data->gid.generate();
  pub_data->topic_name = topic_name;
  pub_data->type_name = rmw_uds::make_ros_type_name(
    callbacks->message_namespace_, callbacks->message_name_);
  pub_data->qos = resolve_qos(qos_profile);
  pub_data->type_support = type_support;
  pub_data->callbacks = callbacks;
  pub_data->context = ctx;
  pub_data->node = node_data;

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
  entry.qos_reliability = static_cast<uint8_t>(pub_data->qos.reliability);
  entry.qos_durability = static_cast<uint8_t>(pub_data->qos.durability);
  entry.qos_history = static_cast<uint8_t>(pub_data->qos.history);
  entry.qos_depth = static_cast<uint32_t>(pub_data->qos.depth);

  rmw_uds::registry_lock(header);
  pub_data->registry_index = rmw_uds::registry_add(header, entry);
  rmw_uds::registry_unlock(header);

  if (pub_data->registry_index < 0) {
    delete pub_data;
    RMW_SET_ERROR_MSG("registry full — cannot create publisher");
    return nullptr;
  }

  auto * pub = rmw_publisher_allocate();
  if (!pub) {
    rmw_uds::registry_lock(header);
    rmw_uds::registry_remove(header, pub_data->registry_index);
    rmw_uds::registry_unlock(header);
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
      rmw_uds::registry_lock(header);
      rmw_uds::registry_remove(header, pub_data->registry_index);
      rmw_uds::registry_unlock(header);
    }
    delete pub_data;
  }

  rcutils_allocator_t alloc = node->context->options.allocator;
  if (publisher->topic_name) {
    alloc.deallocate(const_cast<char *>(publisher->topic_name), alloc.state);
  }
  rmw_publisher_free(publisher);
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

  // Serialize the message using CDR
  std::vector<uint8_t> payload;
  if (!rmw_uds::serialize(ros_message, pub_data->callbacks, payload)) {
    RMW_SET_ERROR_MSG("failed to serialize message");
    return RMW_RET_ERROR;
  }

  // Build wire header
  rmw_uds::WireHeader hdr;
  std::memset(&hdr, 0, sizeof(hdr));
  std::memcpy(hdr.gid, pub_data->gid.data, sizeof(hdr.gid));
  hdr.sequence_number = pub_data->sequence_number.fetch_add(1, std::memory_order_relaxed);
  hdr.source_timestamp_ns = now_ns();
  hdr.payload_size = static_cast<uint32_t>(payload.size());
  hdr.msg_type = 0;  // topic message

  // Find all subscribers for this topic
  auto * header = rmw_uds::registry_header(pub_data->context->registry_ptr);
  rmw_uds::registry_lock(header);
  auto subs = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_SUBSCRIPTION, pub_data->topic_name.c_str(), nullptr, nullptr);
  rmw_uds::registry_unlock(header);

  // TRANSIENT_LOCAL: cache message and replay to late-joining subscribers
  if (pub_data->qos.durability == RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL) {
    std::lock_guard<std::mutex> lock(pub_data->cache_mutex);

    rmw_uds::CachedMessage cached;
    cached.header = hdr;
    cached.payload = payload;
    pub_data->message_cache.push_back(std::move(cached));
    while (pub_data->message_cache.size() > pub_data->qos.depth) {
      pub_data->message_cache.pop_front();
    }

    for (const auto & sub : subs) {
      if (sub.socket_path.empty()) {continue;}
      if (pub_data->known_subscriber_paths.count(sub.socket_path) == 0) {
        pub_data->known_subscriber_paths.insert(sub.socket_path);
        for (size_t i = 0; i + 1 < pub_data->message_cache.size(); ++i) {
          const auto & cm = pub_data->message_cache[i];
          rmw_uds::send_to(
            pub_data->context->send_socket_fd,
            sub.socket_path, cm.header,
            cm.payload.data(), cm.payload.size());
        }
      }
    }
  }

  // Send current message to each subscriber
  for (const auto & sub : subs) {
    if (!sub.socket_path.empty()) {
      rmw_uds::send_to(
        pub_data->context->send_socket_fd,
        sub.socket_path, hdr,
        payload.data(), payload.size());
    }
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
  hdr.sequence_number = pub_data->sequence_number.fetch_add(1, std::memory_order_relaxed);
  hdr.source_timestamp_ns = now_ns();
  hdr.payload_size = static_cast<uint32_t>(serialized_message->buffer_length);
  hdr.msg_type = 0;

  auto * header = rmw_uds::registry_header(pub_data->context->registry_ptr);
  rmw_uds::registry_lock(header);
  auto subs = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_SUBSCRIPTION, pub_data->topic_name.c_str(), nullptr, nullptr);
  rmw_uds::registry_unlock(header);

  for (const auto & sub : subs) {
    if (!sub.socket_path.empty()) {
      rmw_uds::send_to(
        pub_data->context->send_socket_fd,
        sub.socket_path, hdr,
        serialized_message->buffer, serialized_message->buffer_length);
    }
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

  rmw_uds::registry_lock(header);
  auto subs = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_SUBSCRIPTION, pub_data->topic_name.c_str(), nullptr, nullptr);
  rmw_uds::registry_unlock(header);

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
