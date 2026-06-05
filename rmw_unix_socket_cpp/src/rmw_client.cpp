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
  if (resolved.depth == 0) {resolved.depth = 10;}
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

extern "C"
{

rmw_client_t * rmw_create_client(
  const rmw_node_t * node,
  const rosidl_service_type_support_t * type_support,
  const char * service_name,
  const rmw_qos_profile_t * qos_policies)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(type_support, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(service_name, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(qos_policies, nullptr);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return nullptr);

  auto * node_data = static_cast<rmw_uds::UdsNode *>(node->data);
  auto * ctx = node_data->context;

  auto sc = rmw_uds::get_service_callbacks(type_support);
  if (!sc.request || !sc.response) {
    RMW_SET_ERROR_MSG("failed to get service type support callbacks");
    return nullptr;
  }

  auto * cli_data = new (std::nothrow) rmw_uds::UdsClient();
  if (!cli_data) {
    RMW_SET_ERROR_MSG("failed to allocate client data");
    return nullptr;
  }

  cli_data->gid.generate();
  cli_data->service_name = service_name;
  cli_data->type_name = rmw_uds::make_ros_type_name(sc.service_namespace, sc.service_name);
  cli_data->qos = resolve_qos(qos_policies);
  cli_data->request_callbacks = sc.request;
  cli_data->response_callbacks = sc.response;
  cli_data->context = ctx;
  cli_data->node = node_data;

  // Create and bind socket for receiving responses
  cli_data->socket_path = rmw_uds::make_socket_path(ctx->domain_id, "cli");
  cli_data->socket_fd = rmw_uds::create_bound_socket(cli_data->socket_path);
  if (cli_data->socket_fd < 0) {
    delete cli_data;
    RMW_SET_ERROR_MSG("failed to create client socket");
    return nullptr;
  }

  // Register in shared memory
  auto * header = rmw_uds::registry_header(ctx->registry_ptr);
  rmw_uds::RegistryEntry entry;
  std::memset(&entry, 0, sizeof(entry));
  entry.type = rmw_uds::ENTRY_CLIENT;
  entry.pid = getpid();
  std::memcpy(entry.gid, cli_data->gid.data, sizeof(entry.gid));
  std::strncpy(entry.node_name, node_data->name.c_str(), sizeof(entry.node_name) - 1);
  std::strncpy(entry.node_namespace, node_data->ns.c_str(), sizeof(entry.node_namespace) - 1);
  std::strncpy(entry.topic_name, service_name, sizeof(entry.topic_name) - 1);
  std::strncpy(entry.type_name, cli_data->type_name.c_str(), sizeof(entry.type_name) - 1);
  std::strncpy(entry.socket_path, cli_data->socket_path.c_str(), sizeof(entry.socket_path) - 1);
  entry.qos_reliability = static_cast<uint8_t>(cli_data->qos.reliability);
  entry.qos_durability = static_cast<uint8_t>(cli_data->qos.durability);
  entry.qos_depth = static_cast<uint32_t>(cli_data->qos.depth);

  cli_data->registry_index = rmw_uds::registry_add(header, entry);

  if (cli_data->registry_index < 0) {
    rmw_uds::close_socket(cli_data->socket_fd, cli_data->socket_path);
    delete cli_data;
    RMW_SET_ERROR_MSG("registry full");
    return nullptr;
  }

  auto * cli = rmw_client_allocate();
  if (!cli) {
    rmw_uds::registry_remove(header, cli_data->registry_index);
    rmw_uds::close_socket(cli_data->socket_fd, cli_data->socket_path);
    delete cli_data;
    return nullptr;
  }

  cli->implementation_identifier = rmw_uds::identifier;
  cli->data = cli_data;
  cli->service_name = rcutils_strdup(service_name, node->context->options.allocator);
  return cli;
}

rmw_ret_t rmw_destroy_client(rmw_node_t * node, rmw_client_t * client)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(client, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    client, client->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * cli_data = static_cast<rmw_uds::UdsClient *>(client->data);
  if (cli_data) {
    if (cli_data->context && cli_data->registry_index >= 0) {
      auto * header = rmw_uds::registry_header(cli_data->context->registry_ptr);
      rmw_uds::registry_remove(header, cli_data->registry_index);
    }
    rmw_uds::close_socket(cli_data->socket_fd, cli_data->socket_path);
    delete cli_data;
  }

  rcutils_allocator_t alloc = node->context->options.allocator;
  if (client->service_name) {
    alloc.deallocate(const_cast<char *>(client->service_name), alloc.state);
  }
  rmw_client_free(client);
  return RMW_RET_OK;
}

rmw_ret_t rmw_send_request(
  const rmw_client_t * client,
  const void * ros_request,
  int64_t * sequence_id)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(client, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(ros_request, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(sequence_id, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    client, client->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * cli_data = static_cast<rmw_uds::UdsClient *>(client->data);

  // Serialize request
  std::vector<uint8_t> payload;
  if (!rmw_uds::serialize(ros_request, cli_data->request_callbacks, payload)) {
    RMW_SET_ERROR_MSG("failed to serialize request");
    return RMW_RET_ERROR;
  }

  *sequence_id = cli_data->sequence_number.fetch_add(1, std::memory_order_relaxed);

  // Build wire header
  rmw_uds::WireHeader hdr;
  std::memset(&hdr, 0, sizeof(hdr));
  std::memcpy(hdr.gid, cli_data->gid.data, sizeof(hdr.gid));
  hdr.sequence_number = *sequence_id;
  hdr.source_timestamp_ns = now_ns();
  hdr.payload_size = static_cast<uint32_t>(payload.size());
  hdr.msg_type = 1;  // request

  // PERFORMANCE: cache the service path; only re-query the registry on
  // graph generation change.
  auto * header = rmw_uds::registry_header(cli_data->context->registry_ptr);
  uint64_t current_gen = rmw_uds::registry_generation(header);
  std::string service_path;
  {
    std::lock_guard<std::mutex> lock(cli_data->svc_cache_mutex);
    if (current_gen != cli_data->cached_generation) {
      auto services = rmw_uds::registry_query(
        header, rmw_uds::ENTRY_SERVICE, cli_data->service_name.c_str(),
        nullptr, nullptr);
      cli_data->cached_generation = current_gen;
      cli_data->cached_service_path.clear();
      for (const auto & srv : services) {
        if (!srv.socket_path.empty()) {
          cli_data->cached_service_path = srv.socket_path;
          break;
        }
      }
      cli_data->cached_is_available = !services.empty();
    }
    service_path = cli_data->cached_service_path;
  }

  if (!service_path.empty()) {
    rmw_uds::send_to(
      cli_data->context->send_socket_fd,
      service_path, hdr, payload.data(), payload.size());
  }
  // No service found yet — still return OK (request will be sent on retry)
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_response(
  const rmw_client_t * client,
  rmw_service_info_t * request_header,
  void * ros_response,
  bool * taken)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(client, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(request_header, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(ros_response, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(taken, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    client, client->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  *taken = false;
  auto * cli_data = static_cast<rmw_uds::UdsClient *>(client->data);

  // Drain socket
  rmw_uds::WireHeader hdr;
  std::vector<uint8_t> payload;
  while (rmw_uds::recv_from(cli_data->socket_fd, hdr, payload)) {
    if (hdr.msg_type != 2) {payload.clear(); continue;}
    rmw_uds::ReceivedMessage msg;
    msg.header = hdr;
    msg.payload = std::move(payload);
    msg.received_timestamp_ns = now_ns();
    std::lock_guard<std::mutex> lock(cli_data->queue_mutex);
    cli_data->response_queue.push_back(std::move(msg));
    payload.clear();
  }

  std::lock_guard<std::mutex> lock(cli_data->queue_mutex);
  // Drop-garbage-and-continue: one corrupt datagram must not destroy a
  // legitimate in-flight response and wedge the call into a timeout.
  while (!cli_data->response_queue.empty()) {
    auto msg = std::move(cli_data->response_queue.front());
    cli_data->response_queue.pop_front();

    if (!rmw_uds::deserialize(msg.payload.data(), msg.payload.size(),
      cli_data->response_callbacks, ros_response))
    {
      RMW_UDS_LOG_ERROR_THROTTLE(
        1000,
        "rmw_take_response: CDR deserialization failed on service '%s' "
        "(payload=%zu bytes) — dropping datagram",
        cli_data->service_name.c_str(), msg.payload.size());
      continue;
    }

    std::memcpy(request_header->request_id.writer_guid, msg.header.gid,
      RMW_GID_STORAGE_SIZE);
    request_header->request_id.sequence_number = msg.header.sequence_number;
    request_header->source_timestamp = msg.header.source_timestamp_ns;
    request_header->received_timestamp = msg.received_timestamp_ns;

    *taken = true;
    return RMW_RET_OK;
  }

  std::memset(request_header, 0, sizeof(rmw_service_info_t));
  return RMW_RET_OK;
}

rmw_ret_t rmw_client_request_publisher_get_actual_qos(
  const rmw_client_t * client,
  rmw_qos_profile_t * qos)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(client, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(qos, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    client, client->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  auto * cli_data = static_cast<rmw_uds::UdsClient *>(client->data);
  *qos = cli_data->qos;
  return RMW_RET_OK;
}

rmw_ret_t rmw_client_response_subscription_get_actual_qos(
  const rmw_client_t * client,
  rmw_qos_profile_t * qos)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(client, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(qos, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    client, client->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  auto * cli_data = static_cast<rmw_uds::UdsClient *>(client->data);
  *qos = cli_data->qos;
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_gid_for_client(const rmw_client_t * client, rmw_gid_t * gid)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(client, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(gid, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    client, client->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  auto * cli_data = static_cast<rmw_uds::UdsClient *>(client->data);
  gid->implementation_identifier = rmw_uds::identifier;
  std::memcpy(gid->data, cli_data->gid.data, RMW_GID_STORAGE_SIZE);
  return RMW_RET_OK;
}

rmw_ret_t rmw_service_server_is_available(
  const rmw_node_t * node,
  const rmw_client_t * client,
  bool * is_available)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(client, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(is_available, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    client, client->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * cli_data = static_cast<rmw_uds::UdsClient *>(client->data);
  auto * header = rmw_uds::registry_header(cli_data->context->registry_ptr);

  // PERFORMANCE: only re-query the registry on graph generation change;
  // shares the generation/cache with rmw_send_request (one scan per change).
  uint64_t current_gen = rmw_uds::registry_generation(header);
  std::lock_guard<std::mutex> lock(cli_data->svc_cache_mutex);
  if (current_gen != cli_data->cached_generation) {
    auto services = rmw_uds::registry_query(
      header, rmw_uds::ENTRY_SERVICE, cli_data->service_name.c_str(), nullptr, nullptr);
    cli_data->cached_generation = current_gen;
    cli_data->cached_service_path.clear();
    for (const auto & srv : services) {
      if (!srv.socket_path.empty()) {
        cli_data->cached_service_path = srv.socket_path;
        break;
      }
    }
    cli_data->cached_is_available = !services.empty();
  }
  *is_available = cli_data->cached_is_available;
  return RMW_RET_OK;
}

rmw_ret_t rmw_client_set_on_new_response_callback(
  rmw_client_t * client,
  rmw_event_callback_t callback,
  const void * user_data)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(client, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    client, client->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  auto * cli_data = static_cast<rmw_uds::UdsClient *>(client->data);
  std::lock_guard<std::mutex> lock(cli_data->callback_mutex);
  cli_data->on_new_response_cb = callback;
  cli_data->on_new_response_user_data = user_data;

  if (callback) {
    std::lock_guard<std::mutex> qlock(cli_data->queue_mutex);
    if (!cli_data->response_queue.empty()) {
      callback(user_data, cli_data->response_queue.size());
    }
  }
  return RMW_RET_OK;
}

}  // extern "C"
