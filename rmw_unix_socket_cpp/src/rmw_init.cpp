#include "identifier.hpp"
#include "logging.hpp"
#include "registry.hpp"
#include "transport.hpp"
#include "types.hpp"

#include "rcutils/strdup.h"
#include "rmw/check_type_identifiers_match.h"
#include "rmw/error_handling.h"
#include "rmw/init.h"
#include "rmw/init_options.h"
#include "rmw/rmw.h"

extern "C"
{

rmw_ret_t rmw_init_options_init(
  rmw_init_options_t * init_options,
  rcutils_allocator_t allocator)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(init_options, RMW_RET_INVALID_ARGUMENT);
  if (init_options->implementation_identifier != nullptr) {
    RMW_SET_ERROR_MSG("init_options already initialized");
    return RMW_RET_INVALID_ARGUMENT;
  }
  init_options->instance_id = 0;
  init_options->implementation_identifier = rmw_uds::identifier;
  init_options->allocator = allocator;
  init_options->domain_id = RMW_DEFAULT_DOMAIN_ID;
  init_options->security_options = rmw_get_zero_initialized_security_options();
#ifdef RMW_HAS_LOCALHOST_ONLY
  init_options->localhost_only = RMW_LOCALHOST_ONLY_DEFAULT;
#endif
  init_options->discovery_options = rmw_get_zero_initialized_discovery_options();
  init_options->enclave = nullptr;
  init_options->impl = nullptr;
  return RMW_RET_OK;
}

rmw_ret_t rmw_init_options_copy(
  const rmw_init_options_t * src,
  rmw_init_options_t * dst)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(src, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(dst, RMW_RET_INVALID_ARGUMENT);
  if (dst->implementation_identifier != nullptr) {
    RMW_SET_ERROR_MSG("dst already initialized");
    return RMW_RET_INVALID_ARGUMENT;
  }
  *dst = *src;
  if (src->enclave) {
    dst->enclave = rcutils_strdup(src->enclave, src->allocator);
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_init_options_fini(rmw_init_options_t * init_options)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(init_options, RMW_RET_INVALID_ARGUMENT);
  if (init_options->enclave) {
    init_options->allocator.deallocate(init_options->enclave, init_options->allocator.state);
    init_options->enclave = nullptr;
  }
  rmw_security_options_fini(&init_options->security_options, &init_options->allocator);
  *init_options = rmw_get_zero_initialized_init_options();
  return RMW_RET_OK;
}

rmw_ret_t rmw_init(const rmw_init_options_t * options, rmw_context_t * context)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(options, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(context, RMW_RET_INVALID_ARGUMENT);

  auto * ctx = new (std::nothrow) rmw_uds::UdsContext();
  if (!ctx) {
    RMW_SET_ERROR_MSG("failed to allocate context");
    return RMW_RET_BAD_ALLOC;
  }

  size_t domain_id = options->domain_id;
  if (domain_id == RMW_DEFAULT_DOMAIN_ID) {
    domain_id = 0;
  }
  ctx->domain_id = domain_id;

  // Ensure socket directory exists
  rmw_uds::ensure_socket_dir(domain_id);

  // Open shared memory registry
  ctx->registry_fd = rmw_uds::registry_open(
    domain_id, &ctx->registry_ptr, &ctx->registry_size);
  if (ctx->registry_fd < 0) {
    RMW_UDS_LOG_ERROR(
      "rmw_init: failed to open shared memory registry for domain_id=%zu",
      domain_id);
    delete ctx;
    RMW_SET_ERROR_MSG("failed to open shared memory registry");
    return RMW_RET_ERROR;
  }

  // Reclaim state left behind by processes that terminated ungracefully:
  // stale registry slots (whose PIDs are gone) and orphan socket files in
  // /tmp/ros2_uds/<domain>/. Without this, every uncleaned shutdown
  // permanently eats registry slots.
  {
    auto * init_header = rmw_uds::registry_header(ctx->registry_ptr);
    rmw_uds::registry_cleanup_stale(init_header);
    rmw_uds::cleanup_orphan_socket_files(domain_id);
  }

  rmw_uds::warn_if_sysctl_buffers_undersized();

  // Create send socket
  ctx->send_socket_fd = rmw_uds::create_send_socket();
  if (ctx->send_socket_fd < 0) {
    RMW_UDS_LOG_ERROR(
      "rmw_init: failed to create send socket (domain_id=%zu)", domain_id);
    rmw_uds::registry_close(ctx->registry_fd, ctx->registry_ptr, ctx->registry_size);
    delete ctx;
    RMW_SET_ERROR_MSG("failed to create send socket");
    return RMW_RET_ERROR;
  }

  RMW_UDS_LOG_DEBUG(
    "rmw_init: context up (domain_id=%zu, pid=%d)",
    domain_id, static_cast<int>(getpid()));

  // Read initial generation
  auto * header = rmw_uds::registry_header(ctx->registry_ptr);
  ctx->last_registry_generation = rmw_uds::registry_generation(header);

  context->implementation_identifier = rmw_uds::identifier;
  context->impl = reinterpret_cast<rmw_context_impl_t *>(ctx);
  context->instance_id = options->instance_id;
  context->options = *options;
  if (options->enclave) {
    context->options.enclave = rcutils_strdup(options->enclave, options->allocator);
  }
  context->actual_domain_id = domain_id;

  return RMW_RET_OK;
}

rmw_ret_t rmw_shutdown(rmw_context_t * context)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(context, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    context, context->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = reinterpret_cast<rmw_uds::UdsContext *>(context->impl);
  if (ctx) {
    ctx->is_shutdown.store(true);
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_context_fini(rmw_context_t * context)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(context, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    context, context->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ctx = reinterpret_cast<rmw_uds::UdsContext *>(context->impl);
  if (ctx) {
    if (ctx->send_socket_fd >= 0) {
      close(ctx->send_socket_fd);
    }
    rmw_uds::registry_close(ctx->registry_fd, ctx->registry_ptr, ctx->registry_size);
    delete ctx;
    context->impl = nullptr;
  }

  if (context->options.enclave) {
    context->options.allocator.deallocate(
      context->options.enclave, context->options.allocator.state);
    context->options.enclave = nullptr;
  }

  *context = rmw_get_zero_initialized_context();
  return RMW_RET_OK;
}

}  // extern "C"
