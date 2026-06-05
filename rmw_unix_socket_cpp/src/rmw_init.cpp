#include <sys/stat.h>

#include "identifier.hpp"
#include "logging.hpp"
#include "registry.hpp"
#include "transport.hpp"
#include "types.hpp"

#include "rcutils/strdup.h"
#include "rmw/check_type_identifiers_match.h"
#include "rmw/error_handling.h"
#include "rmw/discovery_options.h"
#include "rmw/init.h"
#include "rmw/init_options.h"
#include "rmw/rmw.h"
#include "rmw/security_options.h"

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
  // security_options.security_root_path is heap-owned; deep-copy it so src and
  // dst don't alias the same pointer and double-free in rmw_init_options_fini.
  dst->security_options = rmw_get_zero_initialized_security_options();
  rmw_ret_t ret = rmw_security_options_copy(
    &src->security_options, &src->allocator, &dst->security_options);
  if (ret != RMW_RET_OK) {
    return ret;
  }
  // discovery_options.static_peers is heap-owned; deep-copy it so src and dst
  // don't alias the same array and double-free in rmw_init_options_fini.
  dst->discovery_options = rmw_get_zero_initialized_discovery_options();
  // rmw_discovery_options_copy takes a non-const allocator (unlike the security
  // variant), so copy the caller's allocator into a local first.
  rcutils_allocator_t disc_alloc = src->allocator;
  ret = rmw_discovery_options_copy(
    &src->discovery_options, &disc_alloc, &dst->discovery_options);
  if (ret != RMW_RET_OK) {
    rmw_security_options_fini(&dst->security_options, &dst->allocator);
    return ret;
  }
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
  rmw_ret_t disc_ret = rmw_discovery_options_fini(&init_options->discovery_options);
  (void)disc_ret;
  *init_options = rmw_get_zero_initialized_init_options();
  return RMW_RET_OK;
}

rmw_ret_t rmw_init(const rmw_init_options_t * options, rmw_context_t * context)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(options, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(context, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    options, options->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  if (context->impl != nullptr) {
    RMW_SET_ERROR_MSG("expected a zero-initialized context");
    return RMW_RET_INVALID_ARGUMENT;
  }

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

  // Ensure socket directory exists. ensure_socket_dir swallows mkdir errors and
  // only returns the path, so validate the directory here (in-scope) rather than
  // changing its signature; downstream registry/socket setup would otherwise fail
  // with a less specific error.
  std::string socket_dir = rmw_uds::ensure_socket_dir(domain_id);
  struct stat dir_st;
  if (stat(socket_dir.c_str(), &dir_st) != 0 || !S_ISDIR(dir_st.st_mode)) {
    RMW_UDS_LOG_ERROR(
      "rmw_init: socket directory '%s' is not usable", socket_dir.c_str());
    delete ctx;
    RMW_SET_ERROR_MSG("failed to create socket directory");
    return RMW_RET_ERROR;
  }

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
  ctx->last_registry_generation.store(
    rmw_uds::registry_generation(header), std::memory_order_relaxed);

  // Deep-copy the caller-owned enclave string before committing any field on
  // context, so a copy failure can tear the context down cleanly.
  char * enclave_copy = nullptr;
  if (options->enclave) {
    enclave_copy = rcutils_strdup(options->enclave, options->allocator);
    if (!enclave_copy) {
      close(ctx->send_socket_fd);
      rmw_uds::registry_close(ctx->registry_fd, ctx->registry_ptr, ctx->registry_size);
      delete ctx;
      RMW_SET_ERROR_MSG("failed to copy enclave string");
      return RMW_RET_BAD_ALLOC;
    }
  }

  context->implementation_identifier = rmw_uds::identifier;
  context->impl = reinterpret_cast<rmw_context_impl_t *>(ctx);
  context->instance_id = options->instance_id;
  context->options = *options;
  context->options.enclave = enclave_copy;
  // discovery_options.static_peers is heap-owned; deep-copy it so context->options
  // owns its own array rather than aliasing the caller's options (mirrors enclave).
  context->options.discovery_options = rmw_get_zero_initialized_discovery_options();
  // rmw_discovery_options_copy needs a non-const allocator; copy into a local.
  rcutils_allocator_t disc_alloc = options->allocator;
  rmw_ret_t disc_ret = rmw_discovery_options_copy(
    &options->discovery_options, &disc_alloc,
    &context->options.discovery_options);
  if (disc_ret != RMW_RET_OK) {
    if (enclave_copy) {
      options->allocator.deallocate(enclave_copy, options->allocator.state);
    }
    close(ctx->send_socket_fd);
    rmw_uds::registry_close(ctx->registry_fd, ctx->registry_ptr, ctx->registry_size);
    delete ctx;
    *context = rmw_get_zero_initialized_context();
    RMW_SET_ERROR_MSG("failed to copy discovery options");
    return disc_ret;
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
  rmw_ret_t disc_ret = rmw_discovery_options_fini(&context->options.discovery_options);
  (void)disc_ret;

  *context = rmw_get_zero_initialized_context();
  return RMW_RET_OK;
}

}  // extern "C"
