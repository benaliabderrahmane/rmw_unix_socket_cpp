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

#include "listener.hpp"

#include "drain.hpp"
#include "logging.hpp"

#include "rmw/error_handling.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <system_error>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace rmw_uds
{

// Poke the listener out of epoll_wait so it re-reads its target map or sees a
// cleared running flag.
static void wake_listener(UdsContext * ctx)
{
  if (ctx->listener_wake_fd < 0) {
    return;
  }
  uint64_t val = 1;
  ssize_t ret = write(ctx->listener_wake_fd, &val, sizeof(val));
  (void)ret;
}

static void drain_eventfd(int fd)
{
  uint64_t val;
  while (read(fd, &val, sizeof(val)) == static_cast<ssize_t>(sizeof(val))) {
  }
}

static void listener_loop(UdsContext * ctx)
{
  struct epoll_event ready[16];
  while (ctx->listener_running.load(std::memory_order_acquire)) {
    const int n = epoll_wait(ctx->listener_epoll_fd, ready, 16, -1);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      // Nothing can be recovered here and spinning on a broken epoll would
      // burn a core, so the thread leaves. Endpoints stay usable through
      // rmw_wait; only callback-driven delivery stops.
      RMW_UDS_LOG_ERROR(
        "listener epoll_wait failed: %s (errno=%d) — callback delivery stopped",
        std::strerror(errno), errno);
      return;
    }
    for (int i = 0; i < n; ++i) {
      const int fd = ready[i].data.fd;
      if (fd == ctx->listener_wake_fd) {
        drain_eventfd(fd);
        continue;  // Watch-list change or stop; the loop condition re-checks.
      }

      // Held across the drain: listener_unwatch() takes this mutex, so a
      // thread destroying an endpoint blocks here until the drain - and the
      // callback it fires - has returned. It also guards delivery_fds, so a
      // wait set cannot be deregistered and closed between the enqueue and
      // the signal below.
      std::lock_guard<std::mutex> lock(ctx->listener_mutex);
      auto it = ctx->listener_targets.find(fd);
      if (it == ctx->listener_targets.end()) {
        continue;  // Unwatched between the wake and this lookup.
      }
      size_t enqueued = 0;
      const ArmedEntry & entry = it->second;
      switch (entry.kind) {
        case ARMED_SUBSCRIPTION:
          enqueued = drain_endpoint(
            drain_target(static_cast<UdsSubscription *>(entry.entity)));
          break;
        case ARMED_SERVICE:
          enqueued = drain_endpoint(
            drain_target(static_cast<UdsService *>(entry.entity)));
          break;
        case ARMED_CLIENT:
          enqueued = drain_endpoint(
            drain_target(static_cast<UdsClient *>(entry.entity)));
          break;
        default:
          break;
      }

      // Signal strictly AFTER the enqueue, and signal EVERY live wait set. An
      // rmw_wait drains its own delivery fd strictly BEFORE checking its
      // queues, so this datagram either lands in that queue check or leaves
      // the level-triggered eventfd readable for its epoll — a wait cannot
      // block on a socket this thread already emptied. Same ordering pair as
      // ring_doorbells. One fd per wait set rather than one shared fd, since
      // a read drains the whole counter and would otherwise let the first wait
      // set to wake consume a credit another one needed.
      if (enqueued > 0) {
        uint64_t val = 1;
        for (const int delivery_fd : ctx->delivery_fds) {
          ssize_t ret = write(delivery_fd, &val, sizeof(val));
          (void)ret;
        }
      }
    }
  }
}

static void listener_release_fds(UdsContext * ctx)
{
  if (ctx->listener_wake_fd >= 0) {
    close(ctx->listener_wake_fd);
    ctx->listener_wake_fd = -1;
  }
  if (ctx->listener_epoll_fd >= 0) {
    close(ctx->listener_epoll_fd);
    ctx->listener_epoll_fd = -1;
  }
}

// Create the epoll instance and the wake eventfd, then start the thread.
// Called with listener_mutex held. The delivery eventfds are not created here:
// they belong to the wait sets and already exist by the time a callback is
// registered.
static rmw_ret_t listener_start(UdsContext * ctx)
{
  ctx->listener_epoll_fd = epoll_create1(EPOLL_CLOEXEC);
  if (ctx->listener_epoll_fd < 0) {
    RMW_SET_ERROR_MSG("failed to create listener epoll");
    return RMW_RET_ERROR;
  }
  ctx->listener_wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (ctx->listener_wake_fd < 0) {
    listener_release_fds(ctx);
    RMW_SET_ERROR_MSG("failed to create listener eventfd");
    return RMW_RET_ERROR;
  }

  struct epoll_event ev;
  std::memset(&ev, 0, sizeof(ev));
  ev.events = EPOLLIN;
  ev.data.fd = ctx->listener_wake_fd;
  if (epoll_ctl(ctx->listener_epoll_fd, EPOLL_CTL_ADD, ctx->listener_wake_fd, &ev) != 0) {
    listener_release_fds(ctx);
    RMW_SET_ERROR_MSG("failed to watch the listener wake eventfd");
    return RMW_RET_ERROR;
  }

  // The flag has to be set before the thread exists, or the loop condition
  // can read false and the thread exits immediately - so it is cleared again
  // if the construction throws. std::thread throws std::system_error when the
  // process is out of threads, and this runs under an extern "C" entry point
  // where an escaping exception is undefined behavior.
  ctx->listener_running.store(true, std::memory_order_release);
  try {
    ctx->listener_thread = std::thread(listener_loop, ctx);
  } catch (const std::system_error & e) {
    ctx->listener_running.store(false, std::memory_order_release);
    listener_release_fds(ctx);
    RMW_UDS_LOG_ERROR("failed to start the listener thread: %s", e.what());
    RMW_SET_ERROR_MSG("failed to start the listener thread");
    return RMW_RET_ERROR;
  }
  return RMW_RET_OK;
}

rmw_ret_t listener_set_callback(
  UdsContext * ctx, const DrainTarget & t, uint8_t kind, void * entity,
  uint64_t uid, rmw_event_callback_t callback, const void * user_data)
{
  {
    std::lock_guard<std::mutex> lock(*t.callback_mutex);
    // Flush the backlog only when a callback is taking over from none. rclcpp
    // sets the callback twice in a row on purpose - a stack temporary, then
    // its permanent storage (subscription_base.hpp) - and paying the same
    // backlog out twice hands the executor two events per queued message.
    const bool taking_over = !*t.callback;
    *t.callback = callback;
    *t.callback_user_data = user_data;

    if (callback && taking_over) {
      size_t backlog = 0;
      {
        std::lock_guard<std::mutex> qlock(*t.queue_mutex);
        backlog = t.queue->size();
      }
      // Fired with queue_mutex released. A callback is user code: holding the
      // queue lock across it means a callback that calls rmw_take on its own
      // endpoint deadlocks against itself. drain_endpoint() notifies the same
      // way.
      if (backlog > 0) {
        callback(user_data, backlog);
      }
    }
  }

  // Hand the socket to the listener, or take it back. Deliberately outside
  // callback_mutex: the listener holds listener_mutex across a drain and takes
  // callback_mutex inside it, so acquiring them in the other order here would
  // deadlock against a drain already in flight.
  //
  // The flush above covered the queue; the watch below covers the socket,
  // which epoll reports level-triggered, so datagrams that arrived between the
  // two are delivered rather than dropped or double-reported.
  if (!callback) {
    listener_unwatch(ctx, t.fd);
    return RMW_RET_OK;
  }
  const rmw_ret_t ret = listener_watch(ctx, t.fd, kind, entity, uid);
  if (ret != RMW_RET_OK) {
    // A failure has to mean the callback is not set. Leaving it installed
    // behind an error is the worst of both: the caller is told the
    // registration failed and still gets fired at.
    std::lock_guard<std::mutex> lock(*t.callback_mutex);
    *t.callback = nullptr;
    *t.callback_user_data = nullptr;
  }
  return ret;
}

void listener_add_delivery_fd(UdsContext * ctx, int fd)
{
  if (!ctx || fd < 0) {
    return;
  }
  std::lock_guard<std::mutex> lock(ctx->listener_mutex);
  ctx->delivery_fds.push_back(fd);
}

void listener_remove_delivery_fd(UdsContext * ctx, int fd)
{
  if (!ctx || fd < 0) {
    return;
  }
  std::lock_guard<std::mutex> lock(ctx->listener_mutex);
  auto & fds = ctx->delivery_fds;
  fds.erase(std::remove(fds.begin(), fds.end(), fd), fds.end());
}

rmw_ret_t listener_watch(
  UdsContext * ctx, int fd, uint8_t kind, void * entity, uint64_t uid)
{
  if (!ctx || fd < 0) {
    return RMW_RET_INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> lock(ctx->listener_mutex);
  // Both read under the mutex. A context past rmw_shutdown has already joined
  // its thread, and restarting it for an endpoint on its way out would leak
  // the thread. Reading is_shutdown before taking the mutex - and without
  // listener_stopping beside it - let a registration slip into the window
  // where listener_stop has released the mutex but not yet joined, and
  // listener_start there either terminates the process or loses the stop's
  // wake-up forever. See UdsContext::listener_stopping.
  if (ctx->is_shutdown.load(std::memory_order_acquire) || ctx->listener_stopping) {
    return RMW_RET_OK;
  }

  if (!ctx->listener_running.load(std::memory_order_relaxed)) {
    const rmw_ret_t ret = listener_start(ctx);
    if (ret != RMW_RET_OK) {
      return ret;
    }
  }

  struct epoll_event ev;
  std::memset(&ev, 0, sizeof(ev));
  ev.events = EPOLLIN;
  ev.data.fd = fd;
  // EEXIST means this fd is already watched, which happens when a callback is
  // re-registered on the same endpoint; the map entry below is still refreshed.
  if (epoll_ctl(ctx->listener_epoll_fd, EPOLL_CTL_ADD, fd, &ev) != 0 &&
    errno != EEXIST)
  {
    RMW_UDS_LOG_ERROR(
      "listener epoll_ctl ADD failed for fd %d: %s (errno=%d) — this endpoint "
      "delivers only while a thread is inside rmw_wait",
      fd, std::strerror(errno), errno);
    RMW_SET_ERROR_MSG("failed to watch the endpoint socket on the listener");
    return RMW_RET_ERROR;
  }
  ctx->listener_targets[fd] = ArmedEntry{kind, entity, uid};

  // The socket may already hold datagrams that arrived before the callback was
  // registered. epoll is level-triggered, so the wake alone makes the listener
  // notice them.
  wake_listener(ctx);
  return RMW_RET_OK;
}

void listener_unwatch(UdsContext * ctx, int fd)
{
  if (!ctx || fd < 0) {
    return;
  }
  std::lock_guard<std::mutex> lock(ctx->listener_mutex);
  if (ctx->listener_targets.erase(fd) == 0) {
    return;
  }
  if (ctx->listener_epoll_fd >= 0) {
    (void)epoll_ctl(ctx->listener_epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
  }
}

void listener_stop(UdsContext * ctx)
{
  if (!ctx) {
    return;
  }
  // Clear the flag and wake the thread before joining: it blocks in
  // epoll_wait with no timeout, so nothing else would end the wait.
  {
    std::lock_guard<std::mutex> lock(ctx->listener_mutex);
    if (!ctx->listener_running.load(std::memory_order_relaxed) &&
      !ctx->listener_thread.joinable())
    {
      return;
    }
    // Closes the window below against a concurrent listener_watch.
    ctx->listener_stopping = true;
    ctx->listener_running.store(false, std::memory_order_release);
    wake_listener(ctx);
  }

  // Joined outside the mutex: the thread takes it around every drain.
  if (ctx->listener_thread.joinable()) {
    ctx->listener_thread.join();
  }

  std::lock_guard<std::mutex> lock(ctx->listener_mutex);
  ctx->listener_targets.clear();
  // delivery_fds are the wait sets' own fds, closed by rmw_destroy_wait_set.
  listener_release_fds(ctx);
  ctx->listener_stopping = false;
}

}  // namespace rmw_uds
