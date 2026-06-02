#include "identifier.hpp"
#include "registry.hpp"
#include "transport.hpp"
#include "types.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "rmw/allocators.h"
#include "rmw/check_type_identifiers_match.h"
#include "rmw/error_handling.h"
#include "rmw/rmw.h"

static int64_t now_ns()
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Drain a socket into a message queue (subscription, service, or client)
static void drain_socket(
  int fd,
  std::mutex & queue_mutex,
  std::deque<rmw_uds::ReceivedMessage> & queue,
  size_t max_depth,
  uint8_t expected_msg_type)
{
  rmw_uds::WireHeader hdr;
  std::vector<uint8_t> payload;

  while (rmw_uds::recv_from(fd, hdr, payload)) {
    if (hdr.msg_type != expected_msg_type) {
      payload.clear();
      continue;
    }

    rmw_uds::ReceivedMessage msg;
    msg.header = hdr;
    msg.payload = std::move(payload);
    msg.received_timestamp_ns = now_ns();

    {
      std::lock_guard<std::mutex> lock(queue_mutex);
      queue.push_back(std::move(msg));
      while (queue.size() > max_depth) {
        queue.pop_front();
      }
    }
    payload.clear();
  }
}

extern "C"
{

rmw_wait_set_t * rmw_create_wait_set(rmw_context_t * context, size_t max_conditions)
{
  (void)max_conditions;
  RMW_CHECK_ARGUMENT_FOR_NULL(context, nullptr);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    context, context->implementation_identifier,
    rmw_uds::identifier, return nullptr);

  auto * ws_data = new (std::nothrow) rmw_uds::UdsWaitSet();
  if (!ws_data) {
    RMW_SET_ERROR_MSG("failed to allocate wait set data");
    return nullptr;
  }

  ws_data->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
  if (ws_data->epoll_fd < 0) {
    delete ws_data;
    RMW_SET_ERROR_MSG("failed to create epoll fd");
    return nullptr;
  }

  auto * ws = rmw_wait_set_allocate();
  if (!ws) {
    close(ws_data->epoll_fd);
    delete ws_data;
    RMW_SET_ERROR_MSG("failed to allocate rmw_wait_set_t");
    return nullptr;
  }

  ws->implementation_identifier = rmw_uds::identifier;
  ws->data = ws_data;
  ws->guard_conditions = nullptr;
  return ws;
}

rmw_ret_t rmw_destroy_wait_set(rmw_wait_set_t * wait_set)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(wait_set, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    wait_set, wait_set->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ws_data = static_cast<rmw_uds::UdsWaitSet *>(wait_set->data);
  if (ws_data) {
    if (ws_data->epoll_fd >= 0) {
      close(ws_data->epoll_fd);
    }
    delete ws_data;
  }

  rmw_wait_set_free(wait_set);
  return RMW_RET_OK;
}

rmw_ret_t rmw_wait(
  rmw_subscriptions_t * subscriptions,
  rmw_guard_conditions_t * guard_conditions,
  rmw_services_t * services,
  rmw_clients_t * clients,
  rmw_events_t * events,
  rmw_wait_set_t * wait_set,
  const rmw_time_t * wait_timeout)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(wait_set, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    wait_set, wait_set->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ws_data = static_cast<rmw_uds::UdsWaitSet *>(wait_set->data);

  // 1. Drain all sockets
  if (subscriptions) {
    for (size_t i = 0; i < subscriptions->subscriber_count; ++i) {
      if (!subscriptions->subscribers[i]) {continue;}
      auto * sub = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[i]);
      drain_socket(sub->socket_fd, sub->queue_mutex, sub->message_queue,
        sub->queue_depth, 0);
    }
  }

  if (services) {
    for (size_t i = 0; i < services->service_count; ++i) {
      if (!services->services[i]) {continue;}
      auto * srv = static_cast<rmw_uds::UdsService *>(services->services[i]);
      drain_socket(srv->socket_fd, srv->queue_mutex, srv->request_queue, 100, 1);
    }
  }

  if (clients) {
    for (size_t i = 0; i < clients->client_count; ++i) {
      if (!clients->clients[i]) {continue;}
      auto * cli = static_cast<rmw_uds::UdsClient *>(clients->clients[i]);
      drain_socket(cli->socket_fd, cli->queue_mutex, cli->response_queue, 100, 2);
    }
  }

  // 2. Check graph generation changes — trigger graph guard conditions
  // We need to find the context from any available entity
  rmw_uds::UdsContext * ctx = nullptr;
  if (subscriptions && subscriptions->subscriber_count > 0 && subscriptions->subscribers[0]) {
    ctx = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[0])->context;
  } else if (services && services->service_count > 0 && services->services[0]) {
    ctx = static_cast<rmw_uds::UdsService *>(services->services[0])->context;
  } else if (clients && clients->client_count > 0 && clients->clients[0]) {
    ctx = static_cast<rmw_uds::UdsClient *>(clients->clients[0])->context;
  }

  if (ctx && ctx->registry_ptr) {
    auto * header = rmw_uds::registry_header(ctx->registry_ptr);
    uint64_t gen = rmw_uds::registry_generation(header);
    if (gen != ctx->last_registry_generation) {
      ctx->last_registry_generation = gen;

      // TRANSIENT_LOCAL late-joiner replay. Lock held across the loop —
      // rmw_destroy_publisher takes the same mutex then deletes, so this
      // is what keeps each pub pointer alive while we dereference it.
      std::lock_guard<std::mutex> tl_lock(ctx->transient_local_pubs_mutex);
      for (auto * pub : ctx->transient_local_pubs) {
        std::vector<std::string> sub_paths;
        {
          std::lock_guard<std::mutex> sc_lock(pub->sub_cache_mutex);
          if (rmw_uds::registry_generation(header) != pub->cached_generation) {
            auto subs = rmw_uds::registry_query(
              header, rmw_uds::ENTRY_SUBSCRIPTION,
              pub->topic_name.c_str(), nullptr, nullptr);
            pub->cached_generation = rmw_uds::registry_generation(header);
            pub->cached_subscriber_paths.clear();
            pub->cached_subscriber_paths.reserve(subs.size());
            for (const auto & s : subs) {
              if (!s.socket_path.empty()) {
                pub->cached_subscriber_paths.push_back(s.socket_path);
              }
            }
          }
          sub_paths = pub->cached_subscriber_paths;
        }
        std::lock_guard<std::mutex> c_lock(pub->cache_mutex);
        for (const auto & path : sub_paths) {
          if (pub->known_subscriber_paths.count(path) != 0) {
            continue;
          }
          pub->known_subscriber_paths.insert(path);
          for (const auto & cm : pub->message_cache) {
            rmw_uds::send_to(
              ctx->send_socket_fd,
              path, cm.header, cm.payload.data(), cm.payload.size());
          }
        }
      }

      // Trigger all graph guard conditions in the guard_conditions list
      if (guard_conditions) {
        for (size_t i = 0; i < guard_conditions->guard_condition_count; ++i) {
          if (!guard_conditions->guard_conditions[i]) {continue;}
          // We don't know which are graph GCs, so we just note the change
          // The graph GC is triggered by the node itself
        }
      }
      // Trigger graph guard condition on the context
      if (ctx->graph_guard_condition) {
        auto _r [[maybe_unused]] = rmw_trigger_guard_condition(ctx->graph_guard_condition);
      }
    }
  }

  // Arm every entity fd with epoll on every wait. EPOLL_CTL_ADD is idempotent
  // here: a still-live fd returns EEXIST (already armed), while a fd number
  // reused after its previous owner was closed gets freshly armed. The kernel
  // auto-removes closed fds, so no EPOLL_CTL_DEL is needed.
  {
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    auto register_fd = [&](int fd) {
        if (fd < 0) {return;}
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        // EEXIST means the fd is already armed -> treat as success.
        if (epoll_ctl(ws_data->epoll_fd, EPOLL_CTL_ADD, fd, &ev) != 0 &&
          errno != EEXIST)
        {
          // Other errors are ignored, as before.
        }
      };

    if (subscriptions) {
      for (size_t i = 0; i < subscriptions->subscriber_count; ++i) {
        if (!subscriptions->subscribers[i]) {continue;}
        auto * sub = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[i]);
        register_fd(sub->socket_fd);
      }
    }
    if (services) {
      for (size_t i = 0; i < services->service_count; ++i) {
        if (!services->services[i]) {continue;}
        auto * srv = static_cast<rmw_uds::UdsService *>(services->services[i]);
        register_fd(srv->socket_fd);
      }
    }
    if (clients) {
      for (size_t i = 0; i < clients->client_count; ++i) {
        if (!clients->clients[i]) {continue;}
        auto * cli = static_cast<rmw_uds::UdsClient *>(clients->clients[i]);
        register_fd(cli->socket_fd);
      }
    }
    if (guard_conditions) {
      for (size_t i = 0; i < guard_conditions->guard_condition_count; ++i) {
        if (!guard_conditions->guard_conditions[i]) {continue;}
        auto * gc = static_cast<rmw_uds::UdsGuardCondition *>(
          guard_conditions->guard_conditions[i]);
        register_fd(gc->eventfd_fd);
      }
    }
  }

  // 3. Check if anything is already ready
  bool something_ready = false;

  if (subscriptions) {
    for (size_t i = 0; i < subscriptions->subscriber_count; ++i) {
      if (!subscriptions->subscribers[i]) {continue;}
      auto * sub = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[i]);
      std::lock_guard<std::mutex> lock(sub->queue_mutex);
      if (!sub->message_queue.empty()) {
        something_ready = true;
      }
    }
  }

  if (services) {
    for (size_t i = 0; i < services->service_count; ++i) {
      if (!services->services[i]) {continue;}
      auto * srv = static_cast<rmw_uds::UdsService *>(services->services[i]);
      std::lock_guard<std::mutex> lock(srv->queue_mutex);
      if (!srv->request_queue.empty()) {
        something_ready = true;
      }
    }
  }

  if (clients) {
    for (size_t i = 0; i < clients->client_count; ++i) {
      if (!clients->clients[i]) {continue;}
      auto * cli = static_cast<rmw_uds::UdsClient *>(clients->clients[i]);
      std::lock_guard<std::mutex> lock(cli->queue_mutex);
      if (!cli->response_queue.empty()) {
        something_ready = true;
      }
    }
  }

  if (guard_conditions) {
    for (size_t i = 0; i < guard_conditions->guard_condition_count; ++i) {
      if (!guard_conditions->guard_conditions[i]) {continue;}
      auto * gc = static_cast<rmw_uds::UdsGuardCondition *>(
        guard_conditions->guard_conditions[i]);
      uint64_t val;
      ssize_t r = read(gc->eventfd_fd, &val, sizeof(val));
      if (r == static_cast<ssize_t>(sizeof(val))) {
        something_ready = true;
        // Write it back so it stays triggered for the ready check below
        write(gc->eventfd_fd, &val, sizeof(val));
      }
    }
  }

  // 4. If nothing ready, block with epoll
  if (!something_ready) {
    // Compute timeout. -1 means block forever (epoll_wait sentinel).
    int timeout_ms = -1;
    if (wait_timeout) {
      // Accumulate in int64_t; RMW_DURATION_INFINITE (~9.2e12 ms) overflows int.
      int64_t ms = static_cast<int64_t>(wait_timeout->sec) * 1000 +
        static_cast<int64_t>(wait_timeout->nsec) / 1000000;
      if (ms > std::numeric_limits<int>::max()) {
        timeout_ms = -1;  // Infinite (or beyond epoll's range) -> block forever
      } else {
        timeout_ms = static_cast<int>(ms);
        if (timeout_ms == 0 && wait_timeout->nsec > 0) {
          timeout_ms = 1;  // At least 1ms
        }
      }
    }

    // Block
    struct epoll_event ready_events[64];
    epoll_wait(ws_data->epoll_fd, ready_events, 64, timeout_ms);
    // No EPOLL_CTL_DEL needed — fds stay registered across calls.

    // Drain again after epoll
    if (subscriptions) {
      for (size_t i = 0; i < subscriptions->subscriber_count; ++i) {
        if (!subscriptions->subscribers[i]) {continue;}
        auto * sub = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[i]);
        drain_socket(sub->socket_fd, sub->queue_mutex, sub->message_queue,
          sub->queue_depth, 0);
      }
    }
    if (services) {
      for (size_t i = 0; i < services->service_count; ++i) {
        if (!services->services[i]) {continue;}
        auto * srv = static_cast<rmw_uds::UdsService *>(services->services[i]);
        drain_socket(srv->socket_fd, srv->queue_mutex, srv->request_queue, 100, 1);
      }
    }
    if (clients) {
      for (size_t i = 0; i < clients->client_count; ++i) {
        if (!clients->clients[i]) {continue;}
        auto * cli = static_cast<rmw_uds::UdsClient *>(clients->clients[i]);
        drain_socket(cli->socket_fd, cli->queue_mutex, cli->response_queue, 100, 2);
      }
    }
  }

  // 5. Set output: ready entities stay, non-ready set to NULL
  bool any_ready = false;

  if (subscriptions) {
    for (size_t i = 0; i < subscriptions->subscriber_count; ++i) {
      if (!subscriptions->subscribers[i]) {continue;}
      auto * sub = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[i]);
      std::lock_guard<std::mutex> lock(sub->queue_mutex);
      if (sub->message_queue.empty()) {
        subscriptions->subscribers[i] = nullptr;
      } else {
        any_ready = true;
      }
    }
  }

  if (guard_conditions) {
    for (size_t i = 0; i < guard_conditions->guard_condition_count; ++i) {
      if (!guard_conditions->guard_conditions[i]) {continue;}
      auto * gc = static_cast<rmw_uds::UdsGuardCondition *>(
        guard_conditions->guard_conditions[i]);
      uint64_t val;
      ssize_t r = read(gc->eventfd_fd, &val, sizeof(val));
      if (r == static_cast<ssize_t>(sizeof(val))) {
        any_ready = true;
      } else {
        guard_conditions->guard_conditions[i] = nullptr;
      }
    }
  }

  if (services) {
    for (size_t i = 0; i < services->service_count; ++i) {
      if (!services->services[i]) {continue;}
      auto * srv = static_cast<rmw_uds::UdsService *>(services->services[i]);
      std::lock_guard<std::mutex> lock(srv->queue_mutex);
      if (srv->request_queue.empty()) {
        services->services[i] = nullptr;
      } else {
        any_ready = true;
      }
    }
  }

  if (clients) {
    for (size_t i = 0; i < clients->client_count; ++i) {
      if (!clients->clients[i]) {continue;}
      auto * cli = static_cast<rmw_uds::UdsClient *>(clients->clients[i]);
      std::lock_guard<std::mutex> lock(cli->queue_mutex);
      if (cli->response_queue.empty()) {
        clients->clients[i] = nullptr;
      } else {
        any_ready = true;
      }
    }
  }

  // Events — not supported, set all to null
  if (events) {
    for (size_t i = 0; i < events->event_count; ++i) {
      events->events[i] = nullptr;
    }
  }

  if (!any_ready) {
    return RMW_RET_TIMEOUT;
  }

  return RMW_RET_OK;
}

}  // extern "C"
