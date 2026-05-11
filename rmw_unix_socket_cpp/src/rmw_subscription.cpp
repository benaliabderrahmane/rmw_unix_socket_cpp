#include "identifier.hpp"
#include "logging.hpp"
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
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Drain socket into message queue
static void drain_subscription(rmw_uds::UdsSubscription * sub)
{
  rmw_uds::WireHeader hdr;
  std::vector<uint8_t> payload;

  while (rmw_uds::recv_from(sub->socket_fd, hdr, payload)) {
    if (hdr.msg_type != 0) {continue;}  // Not a topic message

    rmw_uds::ReceivedMessage msg;
    msg.header = hdr;
    msg.payload = std::move(payload);
    msg.received_timestamp_ns = now_ns();

    bool overflow = false;
    {
      std::lock_guard<std::mutex> lock(sub->queue_mutex);
      sub->message_queue.push_back(std::move(msg));
      // Enforce queue depth — any pops here mean we're dropping messages
      // the kernel already delivered to us because the take() side isn't
      // keeping up. This is the rcl-layer equivalent of "slow subscriber".
      while (sub->message_queue.size() > sub->queue_depth) {
        sub->message_queue.pop_front();
        overflow = true;
      }
    }
    if (overflow) {
      RMW_UDS_LOG_WARN_THROTTLE(
        1000,
        "subscription queue overflow on topic '%s' (depth=%zu) — "
        "dropping oldest. Application is not calling take() fast enough.",
        sub->topic_name.c_str(), sub->queue_depth);
    }

    // Trigger callback if set
    {
      std::lock_guard<std::mutex> lock(sub->callback_mutex);
      if (sub->on_new_message_cb) {
        sub->on_new_message_cb(sub->on_new_message_user_data, 1);
      }
    }

    payload.clear();
  }
}

extern "C"
{

rmw_subscription_t * rmw_create_subscription(
  const rmw_node_t * node,
  const rosidl_message_type_support_t * type_support,
  const char * topic_name,
  const rmw_qos_profile_t * qos_policies,
  const rmw_subscription_options_t * subscription_options)
{
  (void)subscription_options;
  RMW_CHECK_ARGUMENT_FOR_NULL(node, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(type_support, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(topic_name, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(qos_policies, nullptr);
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

  auto * sub_data = new (std::nothrow) rmw_uds::UdsSubscription();
  if (!sub_data) {
    RMW_SET_ERROR_MSG("failed to allocate subscription data");
    return nullptr;
  }

  sub_data->gid.generate();
  sub_data->topic_name = topic_name;
  sub_data->type_name = rmw_uds::make_ros_type_name(
    callbacks->message_namespace_, callbacks->message_name_);
  sub_data->qos = resolve_qos(qos_policies);
  sub_data->queue_depth = sub_data->qos.depth;
  sub_data->type_support = type_support;
  sub_data->callbacks = callbacks;
  sub_data->context = ctx;
  sub_data->node = node_data;

  // Create and bind socket
  sub_data->socket_path = rmw_uds::make_socket_path(ctx->domain_id, "sub");
  sub_data->socket_fd = rmw_uds::create_bound_socket(sub_data->socket_path);
  if (sub_data->socket_fd < 0) {
    RMW_UDS_LOG_ERROR(
      "failed to create subscription socket for topic '%s' at '%s'",
      topic_name, sub_data->socket_path.c_str());
    delete sub_data;
    RMW_SET_ERROR_MSG("failed to create subscription socket");
    return nullptr;
  }

  // Register in shared memory
  auto * header = rmw_uds::registry_header(ctx->registry_ptr);
  rmw_uds::RegistryEntry entry;
  std::memset(&entry, 0, sizeof(entry));
  entry.type = rmw_uds::ENTRY_SUBSCRIPTION;
  entry.pid = getpid();
  std::memcpy(entry.gid, sub_data->gid.data, sizeof(entry.gid));
  std::strncpy(entry.node_name, node_data->name.c_str(), sizeof(entry.node_name) - 1);
  std::strncpy(entry.node_namespace, node_data->ns.c_str(), sizeof(entry.node_namespace) - 1);
  std::strncpy(entry.topic_name, topic_name, sizeof(entry.topic_name) - 1);
  std::strncpy(entry.type_name, sub_data->type_name.c_str(), sizeof(entry.type_name) - 1);
  std::strncpy(entry.socket_path, sub_data->socket_path.c_str(), sizeof(entry.socket_path) - 1);
  entry.qos_reliability = static_cast<uint8_t>(sub_data->qos.reliability);
  entry.qos_durability = static_cast<uint8_t>(sub_data->qos.durability);
  entry.qos_history = static_cast<uint8_t>(sub_data->qos.history);
  entry.qos_depth = static_cast<uint32_t>(sub_data->qos.depth);

  sub_data->registry_index = rmw_uds::registry_add(header, entry);

  if (sub_data->registry_index < 0) {
    RMW_UDS_LOG_ERROR(
      "registry full — cannot create subscription for topic '%s' (node=%s%s). "
      "Increase REGISTRY_MAX_ENTRIES or check for slot leaks.",
      topic_name, node_data->ns.c_str(), node_data->name.c_str());
    rmw_uds::close_socket(sub_data->socket_fd, sub_data->socket_path);
    delete sub_data;
    RMW_SET_ERROR_MSG("registry full — cannot create subscription");
    return nullptr;
  }

  auto * sub = rmw_subscription_allocate();
  if (!sub) {
    rmw_uds::registry_remove(header, sub_data->registry_index);
    rmw_uds::close_socket(sub_data->socket_fd, sub_data->socket_path);
    delete sub_data;
    RMW_SET_ERROR_MSG("failed to allocate rmw_subscription_t");
    return nullptr;
  }

  sub->implementation_identifier = rmw_uds::identifier;
  sub->data = sub_data;
  sub->topic_name = rcutils_strdup(topic_name, node->context->options.allocator);
  sub->options = subscription_options ?
    *subscription_options : rmw_get_default_subscription_options();
  sub->can_loan_messages = false;
  sub->is_cft_enabled = false;
  return sub;
}

rmw_ret_t rmw_destroy_subscription(
  rmw_node_t * node,
  rmw_subscription_t * subscription)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(subscription, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    subscription, subscription->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(subscription->data);
  if (sub_data) {
    if (sub_data->context && sub_data->registry_index >= 0) {
      auto * header = rmw_uds::registry_header(sub_data->context->registry_ptr);
      rmw_uds::registry_remove(header, sub_data->registry_index);
    }
    rmw_uds::close_socket(sub_data->socket_fd, sub_data->socket_path);
    delete sub_data;
  }

  rcutils_allocator_t alloc = node->context->options.allocator;
  if (subscription->topic_name) {
    alloc.deallocate(const_cast<char *>(subscription->topic_name), alloc.state);
  }
  rmw_subscription_free(subscription);
  return RMW_RET_OK;
}

rmw_ret_t rmw_take(
  const rmw_subscription_t * subscription,
  void * ros_message,
  bool * taken,
  rmw_subscription_allocation_t * allocation)
{
  (void)allocation;
  RMW_CHECK_ARGUMENT_FOR_NULL(subscription, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(ros_message, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(taken, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    subscription, subscription->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  *taken = false;
  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(subscription->data);

  // Drain any pending messages
  drain_subscription(sub_data);

  std::lock_guard<std::mutex> lock(sub_data->queue_mutex);
  if (sub_data->message_queue.empty()) {
    return RMW_RET_OK;
  }

  auto msg = std::move(sub_data->message_queue.front());
  sub_data->message_queue.pop_front();

  if (!rmw_uds::deserialize(msg.payload.data(), msg.payload.size(),
    sub_data->callbacks, ros_message))
  {
    // Surface to the log too: rcl only prints the SET_ERROR_MSG string if
    // the caller checks the return code, but a corrupted/truncated payload
    // (e.g. silently truncated by the kernel on send) is something the
    // operator needs to see regardless. Throttle to bound the rate.
    RMW_UDS_LOG_ERROR_THROTTLE(
      1000,
      "rmw_take: CDR deserialization failed on topic '%s' (payload=%zu bytes) "
      "— likely truncated or type mismatch with publisher",
      sub_data->topic_name.c_str(), msg.payload.size());
    char err_buf[512];
    std::snprintf(err_buf, sizeof(err_buf),
      "failed to deserialize message on topic '%s' (payload=%zu bytes)",
      sub_data->topic_name.c_str(),
      msg.payload.size());
    RMW_SET_ERROR_MSG(err_buf);
    return RMW_RET_ERROR;
  }

  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_with_info(
  const rmw_subscription_t * subscription,
  void * ros_message,
  bool * taken,
  rmw_message_info_t * message_info,
  rmw_subscription_allocation_t * allocation)
{
  (void)allocation;
  RMW_CHECK_ARGUMENT_FOR_NULL(subscription, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(ros_message, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(taken, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(message_info, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    subscription, subscription->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  *taken = false;
  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(subscription->data);

  drain_subscription(sub_data);

  std::lock_guard<std::mutex> lock(sub_data->queue_mutex);
  if (sub_data->message_queue.empty()) {
    return RMW_RET_OK;
  }

  auto msg = std::move(sub_data->message_queue.front());
  sub_data->message_queue.pop_front();

  if (!rmw_uds::deserialize(msg.payload.data(), msg.payload.size(),
    sub_data->callbacks, ros_message))
  {
    // Surface to the log too: rcl only prints the SET_ERROR_MSG string if
    // the caller checks the return code, but a corrupted/truncated payload
    // (e.g. silently truncated by the kernel on send) is something the
    // operator needs to see regardless. Throttle to bound the rate.
    RMW_UDS_LOG_ERROR_THROTTLE(
      1000,
      "rmw_take: CDR deserialization failed on topic '%s' (payload=%zu bytes) "
      "— likely truncated or type mismatch with publisher",
      sub_data->topic_name.c_str(), msg.payload.size());
    char err_buf[512];
    std::snprintf(err_buf, sizeof(err_buf),
      "failed to deserialize message on topic '%s' (payload=%zu bytes)",
      sub_data->topic_name.c_str(),
      msg.payload.size());
    RMW_SET_ERROR_MSG(err_buf);
    return RMW_RET_ERROR;
  }

  message_info->source_timestamp = msg.header.source_timestamp_ns;
  message_info->received_timestamp = msg.received_timestamp_ns;
  message_info->publication_sequence_number =
    static_cast<uint64_t>(msg.header.sequence_number);
  message_info->reception_sequence_number =
    sub_data->reception_seq.fetch_add(1, std::memory_order_relaxed);
  std::memcpy(
    message_info->publisher_gid.data, msg.header.gid, RMW_GID_STORAGE_SIZE);
  message_info->publisher_gid.implementation_identifier = rmw_uds::identifier;
  message_info->from_intra_process = false;

  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_sequence(
  const rmw_subscription_t * subscription,
  size_t count,
  rmw_message_sequence_t * message_sequence,
  rmw_message_info_sequence_t * message_info_sequence,
  size_t * taken,
  rmw_subscription_allocation_t * allocation)
{
  (void)allocation;
  RMW_CHECK_ARGUMENT_FOR_NULL(subscription, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(message_sequence, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(message_info_sequence, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(taken, RMW_RET_INVALID_ARGUMENT);

  *taken = 0;

  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(subscription->data);
  drain_subscription(sub_data);

  std::lock_guard<std::mutex> lock(sub_data->queue_mutex);

  size_t to_take = count;
  if (message_sequence->capacity < to_take) {to_take = message_sequence->capacity;}
  if (message_info_sequence->capacity < to_take) {to_take = message_info_sequence->capacity;}
  if (sub_data->message_queue.size() < to_take) {to_take = sub_data->message_queue.size();}

  for (size_t i = 0; i < to_take; ++i) {
    auto msg = std::move(sub_data->message_queue.front());
    sub_data->message_queue.pop_front();

    if (!rmw_uds::deserialize(msg.payload.data(), msg.payload.size(),
      sub_data->callbacks, message_sequence->data[i]))
    {
      continue;
    }

    auto & info = message_info_sequence->data[i];
    info.source_timestamp = msg.header.source_timestamp_ns;
    info.received_timestamp = msg.received_timestamp_ns;
    info.publication_sequence_number = static_cast<uint64_t>(msg.header.sequence_number);
    info.reception_sequence_number =
      sub_data->reception_seq.fetch_add(1, std::memory_order_relaxed);
    std::memcpy(info.publisher_gid.data, msg.header.gid, RMW_GID_STORAGE_SIZE);
    info.publisher_gid.implementation_identifier = rmw_uds::identifier;
    info.from_intra_process = false;

    (*taken)++;
  }

  message_sequence->size = *taken;
  message_info_sequence->size = *taken;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_serialized_message(
  const rmw_subscription_t * subscription,
  rmw_serialized_message_t * serialized_message,
  bool * taken,
  rmw_subscription_allocation_t * allocation)
{
  (void)allocation;
  RMW_CHECK_ARGUMENT_FOR_NULL(subscription, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(serialized_message, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(taken, RMW_RET_INVALID_ARGUMENT);

  *taken = false;
  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(subscription->data);
  drain_subscription(sub_data);

  std::lock_guard<std::mutex> lock(sub_data->queue_mutex);
  if (sub_data->message_queue.empty()) {
    return RMW_RET_OK;
  }

  auto msg = std::move(sub_data->message_queue.front());
  sub_data->message_queue.pop_front();

  auto ret = rmw_serialized_message_resize(serialized_message, msg.payload.size());
  if (ret != RMW_RET_OK) {return ret;}
  std::memcpy(serialized_message->buffer, msg.payload.data(), msg.payload.size());
  serialized_message->buffer_length = msg.payload.size();

  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_serialized_message_with_info(
  const rmw_subscription_t * subscription,
  rmw_serialized_message_t * serialized_message,
  bool * taken,
  rmw_message_info_t * message_info,
  rmw_subscription_allocation_t * allocation)
{
  (void)allocation;
  RMW_CHECK_ARGUMENT_FOR_NULL(subscription, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(serialized_message, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(taken, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(message_info, RMW_RET_INVALID_ARGUMENT);

  *taken = false;
  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(subscription->data);
  drain_subscription(sub_data);

  std::lock_guard<std::mutex> lock(sub_data->queue_mutex);
  if (sub_data->message_queue.empty()) {
    return RMW_RET_OK;
  }

  auto msg = std::move(sub_data->message_queue.front());
  sub_data->message_queue.pop_front();

  auto ret = rmw_serialized_message_resize(serialized_message, msg.payload.size());
  if (ret != RMW_RET_OK) {return ret;}
  std::memcpy(serialized_message->buffer, msg.payload.data(), msg.payload.size());
  serialized_message->buffer_length = msg.payload.size();

  message_info->source_timestamp = msg.header.source_timestamp_ns;
  message_info->received_timestamp = msg.received_timestamp_ns;
  message_info->publication_sequence_number =
    static_cast<uint64_t>(msg.header.sequence_number);
  message_info->reception_sequence_number =
    sub_data->reception_seq.fetch_add(1, std::memory_order_relaxed);
  std::memcpy(message_info->publisher_gid.data, msg.header.gid, RMW_GID_STORAGE_SIZE);
  message_info->publisher_gid.implementation_identifier = rmw_uds::identifier;
  message_info->from_intra_process = false;

  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_count_matched_publishers(
  const rmw_subscription_t * subscription,
  size_t * publisher_count)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(subscription, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher_count, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    subscription, subscription->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(subscription->data);
  auto * header = rmw_uds::registry_header(sub_data->context->registry_ptr);

  auto pubs = rmw_uds::registry_query(
    header, rmw_uds::ENTRY_PUBLISHER, sub_data->topic_name.c_str(), nullptr, nullptr);

  *publisher_count = pubs.size();
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_get_actual_qos(
  const rmw_subscription_t * subscription,
  rmw_qos_profile_t * qos)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(subscription, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(qos, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    subscription, subscription->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(subscription->data);
  *qos = sub_data->qos;
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_set_content_filter(
  rmw_subscription_t * subscription,
  const rmw_subscription_content_filter_options_t * options)
{
  (void)subscription;
  (void)options;
  RMW_SET_ERROR_MSG("content filtering not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_subscription_get_content_filter(
  const rmw_subscription_t * subscription,
  rcutils_allocator_t * allocator,
  rmw_subscription_content_filter_options_t * options)
{
  (void)subscription;
  (void)allocator;
  (void)options;
  RMW_SET_ERROR_MSG("content filtering not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_subscription_set_on_new_message_callback(
  rmw_subscription_t * subscription,
  rmw_event_callback_t callback,
  const void * user_data)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(subscription, RMW_RET_INVALID_ARGUMENT);

  auto * sub_data = static_cast<rmw_uds::UdsSubscription *>(subscription->data);
  std::lock_guard<std::mutex> lock(sub_data->callback_mutex);
  sub_data->on_new_message_cb = callback;
  sub_data->on_new_message_user_data = user_data;

  // If there are already messages queued, notify
  if (callback) {
    std::lock_guard<std::mutex> qlock(sub_data->queue_mutex);
    if (!sub_data->message_queue.empty()) {
      callback(user_data, sub_data->message_queue.size());
    }
  }
  return RMW_RET_OK;
}

}  // extern "C"
