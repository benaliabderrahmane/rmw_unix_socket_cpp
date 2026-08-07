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
#include "registry.hpp"
#include "types.hpp"

#include <chrono>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "rcutils/strdup.h"
#include "rmw/check_type_identifiers_match.h"
#include "rmw/error_handling.h"
#include "rmw/get_node_info_and_types.h"
#include "rmw/get_service_names_and_types.h"
#include "rmw/get_topic_endpoint_info.h"
#include "rmw/get_topic_names_and_types.h"
#include "rmw/names_and_types.h"
#include "rmw/rmw.h"
#include "rmw/topic_endpoint_info_array.h"
#include "rmw/topic_endpoint_info.h"

static rmw_uds::UdsContext * get_context(const rmw_node_t * node)
{
  if (!node->data) {
    return nullptr;
  }
  auto * nd = static_cast<rmw_uds::UdsNode *>(node->data);
  return nd->context;
}

// How often the graph read path sweeps for slots left behind by processes that
// died without deregistering. The sweep is O(live entities) in syscalls, so at
// a 10 Hz discovery poll an unconditional sweep dominates every query. This
// only bounds how long a crashed node's slots stay visible: rmw_init still
// sweeps eagerly at startup, and registry_add still sweeps when the table fills.
static constexpr uint64_t GRAPH_CLEANUP_INTERVAL_NS = 1000000000ull;  // 1 s

static uint64_t steady_now_ns()
{
  return static_cast<uint64_t>(
    std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count());
}

static void maybe_cleanup_stale(rmw_uds::UdsContext * ctx, rmw_uds::RegistryHeader * header)
{
  const uint64_t now = steady_now_ns();
  uint64_t last = ctx->last_graph_cleanup_ns.load(std::memory_order_relaxed);
  if (last != 0 && now - last < GRAPH_CLEANUP_INTERVAL_NS) {
    return;
  }
  // Whoever wins the exchange sweeps; concurrent callers skip rather than pile
  // up on the same scan.
  if (!ctx->last_graph_cleanup_ns.compare_exchange_strong(
      last, now, std::memory_order_relaxed, std::memory_order_relaxed))
  {
    return;
  }
  // Lock-free: cleanup_stale + query both use the per-slot seqlock protocol
  // and may safely run concurrently with other readers/writers.
  rmw_uds::registry_cleanup_stale(header);
}

// Key a cached query on everything that selects its result set. The leading
// marker per field keeps a null filter distinct from an empty-string one, and
// the unit separator cannot appear in a ROS name.
static std::string graph_cache_key(
  rmw_uds::RegistryEntryType type,
  const char * topic,
  const char * node_name,
  const char * node_ns)
{
  std::string key;
  key.reserve(96);
  key += static_cast<char>('0' + static_cast<int>(type));
  for (const char * field : {topic, node_name, node_ns}) {
    key += '\x1f';
    key += field ? '+' : '-';
    if (field) {
      key += field;
    }
  }
  return key;
}

static std::vector<rmw_uds::RegistryQueryResult> query_all(
  rmw_uds::UdsContext * ctx,
  rmw_uds::RegistryEntryType type,
  const char * topic = nullptr,
  const char * node_name = nullptr,
  const char * node_ns = nullptr)
{
  auto * header = rmw_uds::registry_header(ctx->registry_ptr);
  maybe_cleanup_stale(ctx, header);

  // Read the generation BEFORE querying. A mutation racing the scan then lands
  // under the old generation and the next call re-queries; reading it after
  // would let a torn result be cached as if it were current.
  const uint64_t generation = rmw_uds::registry_generation(header);
  const std::string key = graph_cache_key(type, topic, node_name, node_ns);

  {
    std::lock_guard<std::mutex> lock(ctx->graph_cache_mutex);
    if (ctx->graph_cache_generation == generation) {
      auto it = ctx->graph_cache.find(key);
      if (it != ctx->graph_cache.end()) {
        return *it->second;
      }
    }
  }

  auto results = rmw_uds::registry_query(header, type, topic, node_name, node_ns);

  {
    std::lock_guard<std::mutex> lock(ctx->graph_cache_mutex);
    if (ctx->graph_cache_generation != generation) {
      ctx->graph_cache.clear();
      ctx->graph_cache_generation = generation;
    }
    ctx->graph_cache[key] =
      std::make_shared<const std::vector<rmw_uds::RegistryQueryResult>>(results);
  }
  return results;
}

// Helper: build names_and_types from a map of topic -> set<type>
static rmw_ret_t fill_names_and_types(
  const std::map<std::string, std::set<std::string>> & topic_types,
  rcutils_allocator_t * allocator,
  rmw_names_and_types_t * names_and_types)
{
  auto ret = rmw_names_and_types_init(names_and_types, topic_types.size(), allocator);
  if (ret != RMW_RET_OK) {return ret;}

  size_t idx = 0;
  for (const auto & [topic, types] : topic_types) {
    names_and_types->names.data[idx] = rcutils_strdup(topic.c_str(), *allocator);
    if (!names_and_types->names.data[idx]) {
      auto _r [[maybe_unused]] = rmw_names_and_types_fini(names_and_types);
      RMW_SET_ERROR_MSG("failed to allocate topic name");
      return RMW_RET_BAD_ALLOC;
    }

    auto ret2 = rcutils_string_array_init(
      &names_and_types->types[idx], types.size(), allocator);
    if (ret2 != RCUTILS_RET_OK) {
      auto _r [[maybe_unused]] = rmw_names_and_types_fini(names_and_types);
      return RMW_RET_ERROR;
    }

    size_t tidx = 0;
    for (const auto & t : types) {
      names_and_types->types[idx].data[tidx] = rcutils_strdup(t.c_str(), *allocator);
      if (!names_and_types->types[idx].data[tidx]) {
        auto _r [[maybe_unused]] = rmw_names_and_types_fini(names_and_types);
        RMW_SET_ERROR_MSG("failed to allocate type name");
        return RMW_RET_BAD_ALLOC;
      }
      tidx++;
    }
    idx++;
  }
  return RMW_RET_OK;
}

extern "C"
{

rmw_ret_t rmw_get_node_names(
  const rmw_node_t * node,
  rcutils_string_array_t * node_names,
  rcutils_string_array_t * node_namespaces)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(node_names, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(node_namespaces, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = get_context(node);
  auto nodes = query_all(ctx, rmw_uds::ENTRY_NODE);

  rcutils_allocator_t alloc = rcutils_get_default_allocator();
  auto ret = rcutils_string_array_init(node_names, nodes.size(), &alloc);
  if (ret != RCUTILS_RET_OK) {return RMW_RET_ERROR;}
  ret = rcutils_string_array_init(node_namespaces, nodes.size(), &alloc);
  if (ret != RCUTILS_RET_OK) {
    auto _r [[maybe_unused]] = rcutils_string_array_fini(node_names);
    return RMW_RET_ERROR;
  }

  for (size_t i = 0; i < nodes.size(); ++i) {
    node_names->data[i] = rcutils_strdup(nodes[i].node_name.c_str(), alloc);
    node_namespaces->data[i] = rcutils_strdup(nodes[i].node_namespace.c_str(), alloc);
    if (!node_names->data[i] || !node_namespaces->data[i]) {
      auto _rn [[maybe_unused]] = rcutils_string_array_fini(node_names);
      auto _rns [[maybe_unused]] = rcutils_string_array_fini(node_namespaces);
      RMW_SET_ERROR_MSG("failed to allocate node name/namespace");
      return RMW_RET_BAD_ALLOC;
    }
  }

  return RMW_RET_OK;
}

rmw_ret_t rmw_get_node_names_with_enclaves(
  const rmw_node_t * node,
  rcutils_string_array_t * node_names,
  rcutils_string_array_t * node_namespaces,
  rcutils_string_array_t * enclaves)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(enclaves, RMW_RET_INVALID_ARGUMENT);

  auto ret = rmw_get_node_names(node, node_names, node_namespaces);
  if (ret != RMW_RET_OK) {return ret;}

  rcutils_allocator_t alloc = rcutils_get_default_allocator();
  auto ret2 = rcutils_string_array_init(enclaves, node_names->size, &alloc);
  if (ret2 != RCUTILS_RET_OK) {
    auto _rn [[maybe_unused]] = rcutils_string_array_fini(node_names);
    auto _rns [[maybe_unused]] = rcutils_string_array_fini(node_namespaces);
    return RMW_RET_ERROR;
  }

  for (size_t i = 0; i < node_names->size; ++i) {
    enclaves->data[i] = rcutils_strdup("/", alloc);
    if (!enclaves->data[i]) {
      auto _rn [[maybe_unused]] = rcutils_string_array_fini(node_names);
      auto _rns [[maybe_unused]] = rcutils_string_array_fini(node_namespaces);
      auto _re [[maybe_unused]] = rcutils_string_array_fini(enclaves);
      RMW_SET_ERROR_MSG("failed to allocate enclave");
      return RMW_RET_BAD_ALLOC;
    }
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_count_publishers(
  const rmw_node_t * node,
  const char * topic_name,
  size_t * count)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(topic_name, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(count, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = get_context(node);
  auto pubs = query_all(ctx, rmw_uds::ENTRY_PUBLISHER, topic_name);
  *count = pubs.size();
  return RMW_RET_OK;
}

rmw_ret_t rmw_count_subscribers(
  const rmw_node_t * node,
  const char * topic_name,
  size_t * count)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(topic_name, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(count, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = get_context(node);
  auto subs = query_all(ctx, rmw_uds::ENTRY_SUBSCRIPTION, topic_name);
  *count = subs.size();
  return RMW_RET_OK;
}

rmw_ret_t rmw_count_clients(
  const rmw_node_t * node,
  const char * service_name,
  size_t * count)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(service_name, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(count, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = get_context(node);
  auto clients = query_all(ctx, rmw_uds::ENTRY_CLIENT, service_name);
  *count = clients.size();
  return RMW_RET_OK;
}

rmw_ret_t rmw_count_services(
  const rmw_node_t * node,
  const char * service_name,
  size_t * count)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(service_name, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(count, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = get_context(node);
  auto services = query_all(ctx, rmw_uds::ENTRY_SERVICE, service_name);
  *count = services.size();
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_topic_names_and_types(
  const rmw_node_t * node,
  rcutils_allocator_t * allocator,
  bool no_demangle,
  rmw_names_and_types_t * topic_names_and_types)
{
  (void)no_demangle;
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(allocator, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(topic_names_and_types, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = get_context(node);
  auto pubs = query_all(ctx, rmw_uds::ENTRY_PUBLISHER);
  auto subs = query_all(ctx, rmw_uds::ENTRY_SUBSCRIPTION);

  std::map<std::string, std::set<std::string>> topic_types;
  for (const auto & p : pubs) {
    topic_types[p.topic_name].insert(p.type_name);
  }
  for (const auto & s : subs) {
    topic_types[s.topic_name].insert(s.type_name);
  }

  return fill_names_and_types(topic_types, allocator, topic_names_and_types);
}

rmw_ret_t rmw_get_service_names_and_types(
  const rmw_node_t * node,
  rcutils_allocator_t * allocator,
  rmw_names_and_types_t * service_names_and_types)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(allocator, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(service_names_and_types, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = get_context(node);
  auto services = query_all(ctx, rmw_uds::ENTRY_SERVICE);
  auto clients = query_all(ctx, rmw_uds::ENTRY_CLIENT);

  std::map<std::string, std::set<std::string>> service_types;
  for (const auto & s : services) {
    service_types[s.topic_name].insert(s.type_name);
  }
  for (const auto & c : clients) {
    service_types[c.topic_name].insert(c.type_name);
  }

  return fill_names_and_types(service_types, allocator, service_names_and_types);
}

// Helper for get_*_names_and_types_by_node
static rmw_ret_t get_names_types_by_node(
  const rmw_node_t * node,
  rcutils_allocator_t * allocator,
  const char * node_name,
  const char * node_namespace,
  rmw_names_and_types_t * names_and_types,
  rmw_uds::RegistryEntryType entry_type)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(allocator, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(node_name, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(node_namespace, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(names_and_types, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = get_context(node);
  auto results = query_all(ctx, entry_type, nullptr, node_name, node_namespace);

  std::map<std::string, std::set<std::string>> topic_types;
  for (const auto & r : results) {
    topic_types[r.topic_name].insert(r.type_name);
  }

  return fill_names_and_types(topic_types, allocator, names_and_types);
}

rmw_ret_t rmw_get_subscriber_names_and_types_by_node(
  const rmw_node_t * node,
  rcutils_allocator_t * allocator,
  const char * node_name,
  const char * node_namespace,
  bool no_demangle,
  rmw_names_and_types_t * topic_names_and_types)
{
  (void)no_demangle;
  return get_names_types_by_node(
    node, allocator, node_name, node_namespace,
    topic_names_and_types, rmw_uds::ENTRY_SUBSCRIPTION);
}

rmw_ret_t rmw_get_publisher_names_and_types_by_node(
  const rmw_node_t * node,
  rcutils_allocator_t * allocator,
  const char * node_name,
  const char * node_namespace,
  bool no_demangle,
  rmw_names_and_types_t * topic_names_and_types)
{
  (void)no_demangle;
  return get_names_types_by_node(
    node, allocator, node_name, node_namespace,
    topic_names_and_types, rmw_uds::ENTRY_PUBLISHER);
}

rmw_ret_t rmw_get_service_names_and_types_by_node(
  const rmw_node_t * node,
  rcutils_allocator_t * allocator,
  const char * node_name,
  const char * node_namespace,
  rmw_names_and_types_t * service_names_and_types)
{
  return get_names_types_by_node(
    node, allocator, node_name, node_namespace,
    service_names_and_types, rmw_uds::ENTRY_SERVICE);
}

rmw_ret_t rmw_get_client_names_and_types_by_node(
  const rmw_node_t * node,
  rcutils_allocator_t * allocator,
  const char * node_name,
  const char * node_namespace,
  rmw_names_and_types_t * service_names_and_types)
{
  return get_names_types_by_node(
    node, allocator, node_name, node_namespace,
    service_names_and_types, rmw_uds::ENTRY_CLIENT);
}

rmw_ret_t rmw_get_publishers_info_by_topic(
  const rmw_node_t * node,
  rcutils_allocator_t * allocator,
  const char * topic_name,
  bool no_mangle,
  rmw_topic_endpoint_info_array_t * publishers_info)
{
  (void)no_mangle;
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(allocator, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(topic_name, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(publishers_info, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = get_context(node);
  auto pubs = query_all(ctx, rmw_uds::ENTRY_PUBLISHER, topic_name);

  auto ret = rmw_topic_endpoint_info_array_init_with_size(publishers_info, pubs.size(), allocator);
  if (ret != RMW_RET_OK) {return ret;}

  for (size_t i = 0; i < pubs.size(); ++i) {
    auto & info = publishers_info->info_array[i];
    rmw_ret_t r;
    r = rmw_topic_endpoint_info_set_node_name(&info, pubs[i].node_name.c_str(), allocator);
    if (r != RMW_RET_OK) {goto fail_pub;}
    r = rmw_topic_endpoint_info_set_node_namespace(&info, pubs[i].node_namespace.c_str(), allocator);
    if (r != RMW_RET_OK) {goto fail_pub;}
    r = rmw_topic_endpoint_info_set_topic_type(&info, pubs[i].type_name.c_str(), allocator);
    if (r != RMW_RET_OK) {goto fail_pub;}
    r = rmw_topic_endpoint_info_set_endpoint_type(&info, RMW_ENDPOINT_PUBLISHER);
    if (r != RMW_RET_OK) {goto fail_pub;}
    r = rmw_topic_endpoint_info_set_qos_profile(&info, &pubs[i].qos);
    if (r != RMW_RET_OK) {goto fail_pub;}
    r = rmw_topic_endpoint_info_set_gid(&info, pubs[i].gid, RMW_GID_STORAGE_SIZE);
    if (r != RMW_RET_OK) {goto fail_pub;}
  }

  return RMW_RET_OK;

fail_pub:
  auto _rp [[maybe_unused]] = rmw_topic_endpoint_info_array_fini(publishers_info, allocator);
  return RMW_RET_ERROR;
}

rmw_ret_t rmw_get_subscriptions_info_by_topic(
  const rmw_node_t * node,
  rcutils_allocator_t * allocator,
  const char * topic_name,
  bool no_mangle,
  rmw_topic_endpoint_info_array_t * subscriptions_info)
{
  (void)no_mangle;
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(allocator, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(topic_name, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(subscriptions_info, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = get_context(node);
  auto subs = query_all(ctx, rmw_uds::ENTRY_SUBSCRIPTION, topic_name);

  auto ret = rmw_topic_endpoint_info_array_init_with_size(
    subscriptions_info, subs.size(), allocator);
  if (ret != RMW_RET_OK) {return ret;}

  for (size_t i = 0; i < subs.size(); ++i) {
    auto & info = subscriptions_info->info_array[i];
    rmw_ret_t r;
    r = rmw_topic_endpoint_info_set_node_name(&info, subs[i].node_name.c_str(), allocator);
    if (r != RMW_RET_OK) {goto fail_sub;}
    r = rmw_topic_endpoint_info_set_node_namespace(&info, subs[i].node_namespace.c_str(), allocator);
    if (r != RMW_RET_OK) {goto fail_sub;}
    r = rmw_topic_endpoint_info_set_topic_type(&info, subs[i].type_name.c_str(), allocator);
    if (r != RMW_RET_OK) {goto fail_sub;}
    r = rmw_topic_endpoint_info_set_endpoint_type(&info, RMW_ENDPOINT_SUBSCRIPTION);
    if (r != RMW_RET_OK) {goto fail_sub;}
    r = rmw_topic_endpoint_info_set_qos_profile(&info, &subs[i].qos);
    if (r != RMW_RET_OK) {goto fail_sub;}
    r = rmw_topic_endpoint_info_set_gid(&info, subs[i].gid, RMW_GID_STORAGE_SIZE);
    if (r != RMW_RET_OK) {goto fail_sub;}
  }

  return RMW_RET_OK;

fail_sub:
  auto _rs [[maybe_unused]] = rmw_topic_endpoint_info_array_fini(subscriptions_info, allocator);
  return RMW_RET_ERROR;
}

rmw_ret_t rmw_compare_gids_equal(
  const rmw_gid_t * gid1,
  const rmw_gid_t * gid2,
  bool * result)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(gid1, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(gid2, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(result, RMW_RET_INVALID_ARGUMENT);
  *result = (std::memcmp(gid1->data, gid2->data, RMW_GID_STORAGE_SIZE) == 0);
  return RMW_RET_OK;
}

}  // extern "C"
