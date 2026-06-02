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

rmw_service_t * rmw_create_service(
  const rmw_node_t * node,
  const rosidl_service_type_support_t * type_support,
  const char * service_name,
  const rmw_qos_profile_t * qos_profile)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(type_support, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(service_name, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(qos_profile, nullptr);
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

  auto * srv_data = new (std::nothrow) rmw_uds::UdsService();
  if (!srv_data) {
    RMW_SET_ERROR_MSG("failed to allocate service data");
    return nullptr;
  }

  srv_data->gid.generate();
  srv_data->service_name = service_name;
  srv_data->type_name = rmw_uds::make_ros_type_name(sc.service_namespace, sc.service_name);
  srv_data->qos = resolve_qos(qos_profile);
  srv_data->request_callbacks = sc.request;
  srv_data->response_callbacks = sc.response;
  srv_data->context = ctx;
  srv_data->node = node_data;

  // Create and bind socket
  srv_data->socket_path = rmw_uds::make_socket_path(ctx->domain_id, "srv");
  srv_data->socket_fd = rmw_uds::create_bound_socket(srv_data->socket_path);
  if (srv_data->socket_fd < 0) {
    delete srv_data;
    RMW_SET_ERROR_MSG("failed to create service socket");
    return nullptr;
  }

  // Register in shared memory
  auto * header = rmw_uds::registry_header(ctx->registry_ptr);
  rmw_uds::RegistryEntry entry;
  std::memset(&entry, 0, sizeof(entry));
  entry.type = rmw_uds::ENTRY_SERVICE;
  entry.pid = getpid();
  std::memcpy(entry.gid, srv_data->gid.data, sizeof(entry.gid));
  std::strncpy(entry.node_name, node_data->name.c_str(), sizeof(entry.node_name) - 1);
  std::strncpy(entry.node_namespace, node_data->ns.c_str(), sizeof(entry.node_namespace) - 1);
  std::strncpy(entry.topic_name, service_name, sizeof(entry.topic_name) - 1);
  std::strncpy(entry.type_name, srv_data->type_name.c_str(), sizeof(entry.type_name) - 1);
  std::strncpy(entry.socket_path, srv_data->socket_path.c_str(), sizeof(entry.socket_path) - 1);
  entry.qos_reliability = static_cast<uint8_t>(srv_data->qos.reliability);
  entry.qos_durability = static_cast<uint8_t>(srv_data->qos.durability);
  entry.qos_depth = static_cast<uint32_t>(srv_data->qos.depth);

  srv_data->registry_index = rmw_uds::registry_add(header, entry);

  if (srv_data->registry_index < 0) {
    rmw_uds::close_socket(srv_data->socket_fd, srv_data->socket_path);
    delete srv_data;
    RMW_SET_ERROR_MSG("registry full");
    return nullptr;
  }

  auto * srv = rmw_service_allocate();
  if (!srv) {
    rmw_uds::registry_remove(header, srv_data->registry_index);
    rmw_uds::close_socket(srv_data->socket_fd, srv_data->socket_path);
    delete srv_data;
    return nullptr;
  }

  srv->implementation_identifier = rmw_uds::identifier;
  srv->data = srv_data;
  srv->service_name = rcutils_strdup(service_name, node->context->options.allocator);
  return srv;
}

rmw_ret_t rmw_destroy_service(rmw_node_t * node, rmw_service_t * service)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(service, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    service, service->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * srv_data = static_cast<rmw_uds::UdsService *>(service->data);
  if (srv_data) {
    if (srv_data->context && srv_data->registry_index >= 0) {
      auto * header = rmw_uds::registry_header(srv_data->context->registry_ptr);
      rmw_uds::registry_remove(header, srv_data->registry_index);
    }
    rmw_uds::close_socket(srv_data->socket_fd, srv_data->socket_path);
    delete srv_data;
  }

  rcutils_allocator_t alloc = node->context->options.allocator;
  if (service->service_name) {
    alloc.deallocate(const_cast<char *>(service->service_name), alloc.state);
  }
  rmw_service_free(service);
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_request(
  const rmw_service_t * service,
  rmw_service_info_t * request_header,
  void * ros_request,
  bool * taken)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(service, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(request_header, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(ros_request, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(taken, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    service, service->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  *taken = false;
  auto * srv_data = static_cast<rmw_uds::UdsService *>(service->data);

  // Drain socket
  rmw_uds::WireHeader hdr;
  std::vector<uint8_t> payload;
  while (rmw_uds::recv_from(srv_data->socket_fd, hdr, payload)) {
    if (hdr.msg_type != 1) {payload.clear(); continue;}
    rmw_uds::ReceivedMessage msg;
    msg.header = hdr;
    msg.payload = std::move(payload);
    msg.received_timestamp_ns = now_ns();
    std::lock_guard<std::mutex> lock(srv_data->queue_mutex);
    srv_data->request_queue.push_back(std::move(msg));
    payload.clear();
  }

  std::lock_guard<std::mutex> lock(srv_data->queue_mutex);
  if (srv_data->request_queue.empty()) {
    std::memset(request_header, 0, sizeof(rmw_service_info_t));
    return RMW_RET_OK;
  }

  auto msg = std::move(srv_data->request_queue.front());
  srv_data->request_queue.pop_front();

  if (!rmw_uds::deserialize(msg.payload.data(), msg.payload.size(),
    srv_data->request_callbacks, ros_request))
  {
    RMW_SET_ERROR_MSG("failed to deserialize request");
    return RMW_RET_ERROR;
  }

  // Fill request header
  std::memcpy(request_header->request_id.writer_guid, msg.header.gid,
    RMW_GID_STORAGE_SIZE);
  request_header->request_id.sequence_number = msg.header.sequence_number;
  request_header->source_timestamp = msg.header.source_timestamp_ns;
  request_header->received_timestamp = msg.received_timestamp_ns;

  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_send_response(
  const rmw_service_t * service,
  rmw_request_id_t * request_header,
  void * ros_response)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(service, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(request_header, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(ros_response, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    service, service->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * srv_data = static_cast<rmw_uds::UdsService *>(service->data);

  // Serialize response
  std::vector<uint8_t> payload;
  if (!rmw_uds::serialize(ros_response, srv_data->response_callbacks, payload)) {
    RMW_SET_ERROR_MSG("failed to serialize response");
    return RMW_RET_ERROR;
  }

  // Build wire header
  rmw_uds::WireHeader hdr;
  std::memset(&hdr, 0, sizeof(hdr));
  std::memcpy(hdr.gid, request_header->writer_guid, sizeof(hdr.gid));
  hdr.sequence_number = request_header->sequence_number;
  hdr.source_timestamp_ns = now_ns();
  hdr.payload_size = static_cast<uint32_t>(payload.size());
  hdr.msg_type = 2;  // response

  // PERFORMANCE: cache the (GID -> path) client list; only refresh on graph change.
  auto * header = rmw_uds::registry_header(srv_data->context->registry_ptr);
  uint64_t current_gen = rmw_uds::registry_generation(header);
  std::vector<rmw_uds::CachedClient> clients_local;
  {
    std::lock_guard<std::mutex> lock(srv_data->client_cache_mutex);
    if (current_gen != srv_data->cached_generation) {
      auto clients = rmw_uds::registry_query(
        header, rmw_uds::ENTRY_CLIENT, srv_data->service_name.c_str(),
        nullptr, nullptr);
      srv_data->cached_generation = rmw_uds::registry_generation(header);
      srv_data->cached_clients.clear();
      srv_data->cached_clients.reserve(clients.size());
      for (const auto & c : clients) {
        if (c.socket_path.empty()) {continue;}
        rmw_uds::CachedClient cc;
        cc.socket_path = c.socket_path;
        std::memcpy(cc.gid, c.gid, sizeof(cc.gid));
        srv_data->cached_clients.push_back(std::move(cc));
      }
    }
    clients_local = srv_data->cached_clients;
  }

  for (const auto & c : clients_local) {
    if (std::memcmp(c.gid, request_header->writer_guid, sizeof(c.gid)) == 0) {
      rmw_uds::send_to(
        srv_data->context->send_socket_fd,
        c.socket_path, hdr, payload.data(), payload.size());
      return RMW_RET_OK;
    }
  }
  // Client not found — might have disconnected
  return RMW_RET_OK;
}

rmw_ret_t rmw_service_request_subscription_get_actual_qos(
  const rmw_service_t * service,
  rmw_qos_profile_t * qos)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(service, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(qos, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    service, service->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  auto * srv_data = static_cast<rmw_uds::UdsService *>(service->data);
  *qos = srv_data->qos;
  return RMW_RET_OK;
}

rmw_ret_t rmw_service_response_publisher_get_actual_qos(
  const rmw_service_t * service,
  rmw_qos_profile_t * qos)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(service, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(qos, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    service, service->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  auto * srv_data = static_cast<rmw_uds::UdsService *>(service->data);
  *qos = srv_data->qos;
  return RMW_RET_OK;
}

rmw_ret_t rmw_service_set_on_new_request_callback(
  rmw_service_t * service,
  rmw_event_callback_t callback,
  const void * user_data)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(service, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    service, service->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  auto * srv_data = static_cast<rmw_uds::UdsService *>(service->data);
  std::lock_guard<std::mutex> lock(srv_data->callback_mutex);
  srv_data->on_new_request_cb = callback;
  srv_data->on_new_request_user_data = user_data;

  if (callback) {
    std::lock_guard<std::mutex> qlock(srv_data->queue_mutex);
    if (!srv_data->request_queue.empty()) {
      callback(user_data, srv_data->request_queue.size());
    }
  }
  return RMW_RET_OK;
}

}  // extern "C"
