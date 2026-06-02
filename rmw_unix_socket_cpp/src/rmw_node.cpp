#include "identifier.hpp"
#include "registry.hpp"
#include "types.hpp"

#include <cstring>

#include "rcutils/strdup.h"
#include "rmw/allocators.h"
#include "rmw/check_type_identifiers_match.h"
#include "rmw/error_handling.h"
#include "rmw/rmw.h"

extern "C"
{

rmw_node_t * rmw_create_node(
  rmw_context_t * context,
  const char * name,
  const char * namespace_)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(context, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(name, nullptr);
  RMW_CHECK_ARGUMENT_FOR_NULL(namespace_, nullptr);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    context, context->implementation_identifier,
    rmw_uds::identifier, return nullptr);

  auto * ctx = reinterpret_cast<rmw_uds::UdsContext *>(context->impl);

  // Create graph guard condition for this node
  rmw_guard_condition_t * graph_gc = rmw_create_guard_condition(context);
  if (!graph_gc) {
    return nullptr;
  }

  auto * node_data = new (std::nothrow) rmw_uds::UdsNode();
  if (!node_data) {
    auto _r [[maybe_unused]] = rmw_destroy_guard_condition(graph_gc);
    RMW_SET_ERROR_MSG("failed to allocate node data");
    return nullptr;
  }

  node_data->name = name;
  node_data->ns = namespace_;
  node_data->context = ctx;
  node_data->graph_guard_condition = graph_gc;

  // Register node in the shared memory registry
  auto * header = rmw_uds::registry_header(ctx->registry_ptr);
  rmw_uds::RegistryEntry entry;
  std::memset(&entry, 0, sizeof(entry));
  entry.type = rmw_uds::ENTRY_NODE;
  entry.pid = getpid();
  std::strncpy(entry.node_name, name, sizeof(entry.node_name) - 1);
  std::strncpy(entry.node_namespace, namespace_, sizeof(entry.node_namespace) - 1);

  node_data->registry_index = rmw_uds::registry_add(header, entry);

  if (node_data->registry_index < 0) {
    auto _r [[maybe_unused]] = rmw_destroy_guard_condition(graph_gc);
    delete node_data;
    RMW_SET_ERROR_MSG("registry full — cannot create node");
    return nullptr;
  }

  auto * node = rmw_node_allocate();
  if (!node) {
    rmw_uds::registry_remove(header, node_data->registry_index);
    auto _r [[maybe_unused]] = rmw_destroy_guard_condition(graph_gc);
    delete node_data;
    RMW_SET_ERROR_MSG("failed to allocate rmw_node_t");
    return nullptr;
  }

  node->implementation_identifier = rmw_uds::identifier;
  node->data = node_data;
  node->name = rcutils_strdup(name, context->options.allocator);
  node->namespace_ = rcutils_strdup(namespace_, context->options.allocator);
  node->context = context;

  if (!node->name || !node->namespace_) {
    rcutils_allocator_t alloc = context->options.allocator;
    if (node->name) {
      alloc.deallocate(const_cast<char *>(node->name), alloc.state);
    }
    if (node->namespace_) {
      alloc.deallocate(const_cast<char *>(node->namespace_), alloc.state);
    }
    rmw_node_free(node);
    rmw_uds::registry_remove(header, node_data->registry_index);
    auto _r [[maybe_unused]] = rmw_destroy_guard_condition(graph_gc);
    delete node_data;
    RMW_SET_ERROR_MSG("failed to copy node name/namespace");
    return nullptr;
  }

  return node;
}

rmw_ret_t rmw_destroy_node(rmw_node_t * node)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * node_data = static_cast<rmw_uds::UdsNode *>(node->data);
  if (node_data) {
    // Remove from registry
    if (node_data->context && node_data->registry_index >= 0) {
      auto * header = rmw_uds::registry_header(node_data->context->registry_ptr);
      rmw_uds::registry_remove(header, node_data->registry_index);
    }

    if (node_data->graph_guard_condition) {
      auto _r [[maybe_unused]] = rmw_destroy_guard_condition(node_data->graph_guard_condition);
    }

    delete node_data;
  }

  rcutils_allocator_t alloc = node->context->options.allocator;
  if (node->name) {
    alloc.deallocate(const_cast<char *>(node->name), alloc.state);
  }
  if (node->namespace_) {
    alloc.deallocate(const_cast<char *>(node->namespace_), alloc.state);
  }

  rmw_node_free(node);
  return RMW_RET_OK;
}

rmw_ret_t rmw_node_assert_liveliness(const rmw_node_t * node)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  return RMW_RET_OK;
}

const rmw_guard_condition_t * rmw_node_get_graph_guard_condition(
  const rmw_node_t * node)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(node, nullptr);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    node, node->implementation_identifier,
    rmw_uds::identifier, return nullptr);

  auto * node_data = static_cast<rmw_uds::UdsNode *>(node->data);
  return node_data->graph_guard_condition;
}

}  // extern "C"
