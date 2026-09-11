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

#include "test_base.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

#include <dirent.h>

#include "test_msgs/msg/basic_types.hpp"
#include "test_msgs/srv/basic_types.hpp"

#include "rmw/qos_profiles.h"
#include "rosidl_typesupport_cpp/message_type_support.hpp"
#include "rosidl_typesupport_cpp/service_type_support.hpp"

// Records what a listener callback was handed. rmw_event_callback_t is a plain
// function pointer, so the fixture passes one of these as its user_data.
struct CallbackCounter
{
  std::atomic<size_t> calls{0};
  std::atomic<size_t> events{0};
  std::atomic<bool> saw_zero{false};

  static void fire(const void * user_data, size_t number_of_events)
  {
    auto * self =
      const_cast<CallbackCounter *>(static_cast<const CallbackCounter *>(user_data));
    // rmw/event_callback_type.h: "It should never be 0."
    if (number_of_events == 0) {
      self->saw_zero.store(true);
    }
    self->calls.fetch_add(1);
    self->events.fetch_add(number_of_events);
  }
};

// Threads in this process, from /proc/self/task. Used to pin the listener's
// lazy start: a process whose executor waits instead of listening must not
// gain a thread.
static size_t thread_count()
{
  DIR * dir = opendir("/proc/self/task");
  if (!dir) {
    return 0;
  }
  size_t n = 0;
  while (const dirent * entry = readdir(dir)) {
    if (entry->d_name[0] != '.') {
      ++n;
    }
  }
  closedir(dir);
  return n;
}

// Spin until `counter` reports something or the budget runs out. Deliberately
// calls neither rmw_wait nor rmw_take: the point is that delivery happens
// without either.
static bool await_events(const CallbackCounter & counter, int budget_ms = 2000)
{
  for (int waited = 0; waited < budget_ms; waited += 5) {
    if (counter.events.load() > 0) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return counter.events.load() > 0;
}

class ListenerCallbackTest : public RmwUdsNodeTest
{
protected:
  rmw_wait_set_t * ws = nullptr;
  rmw_qos_profile_t qos;
  CallbackCounter counter;

  void SetUp() override
  {
    RmwUdsNodeTest::SetUp();
    std::memset(&qos, 0, sizeof(qos));
    qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
    qos.depth = 10;
    qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
    qos.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;

    ws = rmw_create_wait_set(&context, 1);
    ASSERT_NE(nullptr, ws);
  }

  void TearDown() override
  {
    if (ws) { auto _r [[maybe_unused]] = rmw_destroy_wait_set(ws); }
    RmwUdsNodeTest::TearDown();
  }

  static rmw_time_t short_timeout()
  {
    rmw_time_t t;
    t.sec = 1;
    t.nsec = 0;
    return t;
  }

  void wait_on_subscription(rmw_subscription_t * sub)
  {
    rmw_subscriptions_t subs;
    void * arr[1] = {sub->data};
    subs.subscribers = arr;
    subs.subscriber_count = 1;
    const rmw_time_t timeout = short_timeout();
    ASSERT_EQ(RMW_RET_OK, rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, ws, &timeout));
  }

  void wait_on_service(rmw_service_t * srv)
  {
    rmw_services_t services;
    void * arr[1] = {srv->data};
    services.services = arr;
    services.service_count = 1;
    const rmw_time_t timeout = short_timeout();
    ASSERT_EQ(
      RMW_RET_OK, rmw_wait(nullptr, nullptr, &services, nullptr, nullptr, ws, &timeout));
  }

  void wait_on_client(rmw_client_t * cli)
  {
    rmw_clients_t clients;
    void * arr[1] = {cli->data};
    clients.clients = arr;
    clients.client_count = 1;
    const rmw_time_t timeout = short_timeout();
    ASSERT_EQ(
      RMW_RET_OK, rmw_wait(nullptr, nullptr, nullptr, &clients, nullptr, ws, &timeout));
  }
};

// Before the drains were unified only the rmw_take path notified, so a
// callback registered by an executor heard nothing about messages that
// arrived while a thread sat in rmw_wait - which is every message, for an
// executor that waits before it takes.
//
// Registering the callback also hands the socket to the listener, so either it
// or the wait may be the one that delivers. The invariant is the count: the
// message is reported exactly once, by whichever path got there first.
TEST_F(ListenerCallbackTest, SubscriptionCallbackFiresExactlyOnceOnDelivery)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_topic", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 7;
  ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  wait_on_subscription(sub);

  EXPECT_EQ(1u, counter.events.load());
  EXPECT_FALSE(counter.saw_zero.load());

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// Batched, not one call per message: rmw/event_callback_type.h documents
// number_of_events as the count since the callback was last called and
// explicitly allows > 1.
//
// Asserts the event total, not the number of calls. The listener thread splits
// a burst into however many batches its epoll reports, so the call count is
// not the middleware's to promise - but the events always sum to the number of
// messages, because each batch reports what it added to the queue.
TEST_F(ListenerCallbackTest, SubscriptionCallbackReportsTheBatchCount)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_batch", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_batch", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

  // All three are queued in the subscriber's socket buffer before the last
  // rmw_publish returns, so one drain reads all of them.
  for (int32_t i = 0; i < 3; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i;
    ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }

  wait_on_subscription(sub);

  EXPECT_EQ(3u, counter.events.load());
  EXPECT_FALSE(counter.saw_zero.load());

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// number_of_events is a take credit: the executor calls take() once per event
// it was told about. So the events reported for a burst must equal the number
// of messages rmw_take can actually return - counting datagrams the socket
// handed over rather than datagrams the queue kept spends the difference on
// takes that find nothing.
//
// Asserts credits against takes rather than against qos.depth. How much of a
// 15-message burst survives the socket buffer and the QoS trim is timing, and
// not what this is pinning; that the two numbers agree is.
TEST_F(ListenerCallbackTest, BatchCountMatchesWhatCanBeTaken)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_overflow", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_overflow", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

  // More than qos.depth (10), so the drain has to trim and report the trim.
  for (int32_t i = 0; i < 15; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i;
    ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }

  wait_on_subscription(sub);
  // Nothing publishes any more, so this lets the listener finish whatever it
  // still had in flight before the two counts are compared.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  const size_t events = counter.events.load();
  size_t takeable = 0;
  bool taken = true;
  while (taken) {
    test_msgs::msg::BasicTypes recv;
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    if (taken) {
      ++takeable;
    }
  }

  EXPECT_EQ(takeable, events);
  EXPECT_GT(takeable, 0u);
  EXPECT_LE(takeable, qos.depth);
  EXPECT_FALSE(counter.saw_zero.load());

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// rclcpp sets the callback twice in a row on purpose - once against a stack
// temporary, then again against its permanent storage - to close the gap where
// the std::function is replaced but the middleware still holds the old pointer
// (subscription_base.hpp). The backlog flush must not pay out twice for it, or
// a TRANSIENT_LOCAL subscription reports every replayed sample two times.
TEST_F(ListenerCallbackTest, ReRegisteringTheCallbackDoesNotReflushTheBacklog)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_reflush", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_reflush", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  // Queue three messages with no callback installed, so the wait drains them
  // into the queue silently and both setter calls below see the same backlog.
  for (int32_t i = 0; i < 3; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i;
    ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }
  wait_on_subscription(sub);
  ASSERT_EQ(0u, counter.calls.load());

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));
  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

  EXPECT_EQ(3u, counter.events.load());
  EXPECT_EQ(1u, counter.calls.load());

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// A drain that enqueues nothing must stay silent: the callback contract has
// no zero, so notifying on an empty drain would report an event that did not
// happen.
TEST_F(ListenerCallbackTest, EmptyDrainDoesNotNotify)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_quiet", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

  // Nothing was published, so the wait times out with an empty drain.
  rmw_subscriptions_t subs;
  void * arr[1] = {sub->data};
  subs.subscribers = arr;
  subs.subscriber_count = 1;
  rmw_time_t timeout;
  timeout.sec = 0;
  timeout.nsec = 20 * 1000 * 1000;
  EXPECT_EQ(
    RMW_RET_TIMEOUT, rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, ws, &timeout));

  EXPECT_EQ(0u, counter.calls.load());

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
}

TEST_F(ListenerCallbackTest, ClearingTheCallbackStopsNotifications)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_cleared", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_cleared", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));
  ASSERT_EQ(
    RMW_RET_OK, rmw_subscription_set_on_new_message_callback(sub, nullptr, nullptr));

  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 1;
  ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  wait_on_subscription(sub);

  EXPECT_EQ(0u, counter.calls.load());

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// on_new_request_cb and on_new_response_cb were stored by their setters and
// flushed once against an already-queued backlog, but no drain ever fired
// them again - services and clients were callback-dead after registration.
TEST_F(ListenerCallbackTest, ServiceCallbackFiresOnRequest)
{
  auto * ts = rosidl_typesupport_cpp::get_service_type_support_handle<
    test_msgs::srv::BasicTypes>();
  auto * srv = rmw_create_service(node, ts, "/listener_srv", &qos);
  auto * cli = rmw_create_client(node, ts, "/listener_srv", &qos);
  ASSERT_NE(nullptr, srv);
  ASSERT_NE(nullptr, cli);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_service_set_on_new_request_callback(srv, CallbackCounter::fire, &counter));

  test_msgs::srv::BasicTypes::Request request;
  request.int32_value = 11;
  int64_t seq_id = 0;
  ASSERT_EQ(RMW_RET_OK, rmw_send_request(cli, &request, &seq_id));

  wait_on_service(srv);

  EXPECT_EQ(1u, counter.events.load());
  EXPECT_FALSE(counter.saw_zero.load());

  auto _c [[maybe_unused]] = rmw_destroy_client(node, cli);
  auto _s [[maybe_unused]] = rmw_destroy_service(node, srv);
}

TEST_F(ListenerCallbackTest, ClientCallbackFiresOnResponse)
{
  auto * ts = rosidl_typesupport_cpp::get_service_type_support_handle<
    test_msgs::srv::BasicTypes>();
  auto * srv = rmw_create_service(node, ts, "/listener_cli", &qos);
  auto * cli = rmw_create_client(node, ts, "/listener_cli", &qos);
  ASSERT_NE(nullptr, srv);
  ASSERT_NE(nullptr, cli);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_client_set_on_new_response_callback(cli, CallbackCounter::fire, &counter));

  test_msgs::srv::BasicTypes::Request request;
  request.int32_value = 22;
  int64_t seq_id = 0;
  ASSERT_EQ(RMW_RET_OK, rmw_send_request(cli, &request, &seq_id));

  test_msgs::srv::BasicTypes::Request recv_request;
  rmw_service_info_t request_header;
  std::memset(&request_header, 0, sizeof(request_header));
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_request(srv, &request_header, &recv_request, &taken));
  ASSERT_TRUE(taken);

  test_msgs::srv::BasicTypes::Response response;
  response.int32_value = 33;
  ASSERT_EQ(RMW_RET_OK, rmw_send_response(srv, &request_header.request_id, &response));

  wait_on_client(cli);

  EXPECT_EQ(1u, counter.events.load());
  EXPECT_FALSE(counter.saw_zero.load());

  auto _c [[maybe_unused]] = rmw_destroy_client(node, cli);
  auto _s [[maybe_unused]] = rmw_destroy_service(node, srv);
}

// The reason the listener thread exists. rclcpp's EventsExecutor never calls
// rmw_wait, and only calls rmw_take once a callback has told it there is
// something to take - so with delivery happening exclusively inside rmw_wait,
// nothing ever moves the datagram off the socket and the executor waits
// forever. This test calls neither.
TEST_F(ListenerCallbackTest, CallbackFiresWithNoThreadInWaitOrTake)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_async", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_async", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 99;
  ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  EXPECT_TRUE(await_events(counter)) << "no callback without rmw_wait/rmw_take";
  EXPECT_FALSE(counter.saw_zero.load());

  // The message really is queued, not merely announced.
  test_msgs::msg::BasicTypes recv;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  EXPECT_TRUE(taken);
  EXPECT_EQ(99, recv.int32_value);

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// The listener starts on the first registration and only then, so the
// zero-background-threads property still holds for every executor that waits.
TEST_F(ListenerCallbackTest, NoListenerThreadUntilACallbackIsRegistered)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto sub_opts = rmw_get_default_subscription_options();

  const size_t before = thread_count();
  ASSERT_GT(before, 0u);

  auto * sub = rmw_create_subscription(node, ts, "/listener_lazy", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);
  EXPECT_EQ(before, thread_count()) << "creating a subscription started a thread";

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));
  EXPECT_EQ(before + 1, thread_count()) << "registration did not start the listener";

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
}

TEST_F(ListenerCallbackTest, ClearingTheCallbackStopsAsyncDelivery)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_unwatch", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_unwatch", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));
  ASSERT_EQ(
    RMW_RET_OK, rmw_subscription_set_on_new_message_callback(sub, nullptr, nullptr));

  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 5;
  ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(0u, counter.calls.load());

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// listener_unwatch must not return while the listener is inside a drain of
// this subscription, or the delete that follows frees it underneath the
// listener. Run under -fsanitize=thread/address to make a regression loud.
TEST_F(ListenerCallbackTest, DestroySubscriptionWhileMessagesArrive)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_churn", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  for (int round = 0; round < 20; ++round) {
    auto sub_opts = rmw_get_default_subscription_options();
    auto * sub = rmw_create_subscription(node, ts, "/listener_churn", &qos, &sub_opts);
    ASSERT_NE(nullptr, sub);
    ASSERT_EQ(
      RMW_RET_OK,
      rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

    for (int i = 0; i < 5; ++i) {
      test_msgs::msg::BasicTypes msg;
      msg.int32_value = i;
      ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
    }
    // Destroy with datagrams very likely still in flight on the listener.
    EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, sub));
  }

  EXPECT_FALSE(counter.saw_zero.load());
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// The listener and a wait set can drain the same socket. If the listener wins
// the race between a wait's queue scan and its epoll_wait, the socket is
// already empty and epoll has nothing left to report - the wait would block
// with a full queue. delivery_fd is what closes that window; this drives the
// race repeatedly rather than trying to interleave it exactly.
TEST_F(ListenerCallbackTest, WaitAndListenerOnTheSameSubscriptionDoNotHang)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_race", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_race", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

  rmw_subscriptions_t subs;
  void * arr[1] = {sub->data};
  subs.subscribers = arr;
  subs.subscriber_count = 1;
  rmw_time_t timeout;
  timeout.sec = 2;
  timeout.nsec = 0;

  for (int round = 0; round < 50; ++round) {
    std::atomic<bool> waiting{false};
    std::atomic<rmw_ret_t> wait_ret{RMW_RET_ERROR};
    std::thread waiter([&]() {
        waiting.store(true);
        wait_ret.store(
          rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, ws, &timeout));
      });
    while (!waiting.load()) {
      std::this_thread::yield();
    }

    test_msgs::msg::BasicTypes msg;
    msg.int32_value = round;
    ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

    waiter.join();
    // A timeout here means the wait slept through a message that was already
    // in the queue.
    EXPECT_EQ(RMW_RET_OK, wait_ret.load()) << "round " << round;

    bool taken = true;
    while (taken) {
      test_msgs::msg::BasicTypes recv;
      ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    }
  }

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// The same coexistence, with a second wait set competing for the wake. A
// delivery eventfd read drains the whole counter, so one shared per-context fd
// made this a single credit: a wait set that gained no work of its own still
// consumed it and threw it away, and the wait set that needed it slept on a
// socket the listener had already emptied. One fd per wait set removes the
// sharing, so the thief below cannot affect the victim at all.
TEST_F(ListenerCallbackTest, ASecondWaitSetDoesNotStealTheDeliveryWakeup)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_steal", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_steal", &qos, &sub_opts);
  // Held only by the thief wait set, and nothing ever publishes to it.
  auto * idle = rmw_create_subscription(node, ts, "/listener_steal_idle", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);
  ASSERT_NE(nullptr, idle);

  rmw_wait_set_t * thief_ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, thief_ws);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

  rmw_subscriptions_t subs;
  void * arr[1] = {sub->data};
  subs.subscribers = arr;
  subs.subscriber_count = 1;
  rmw_time_t timeout;
  timeout.sec = 2;
  timeout.nsec = 0;

  // Spins rmw_wait on its own wait set for the whole test, so every delivery
  // the listener signals has a competitor racing to consume it.
  std::atomic<bool> stop{false};
  std::thread thief([&]() {
      rmw_subscriptions_t idle_subs;
      void * idle_arr[1] = {idle->data};
      idle_subs.subscribers = idle_arr;
      idle_subs.subscriber_count = 1;
      rmw_time_t brief;
      brief.sec = 0;
      brief.nsec = 2 * 1000 * 1000;
      while (!stop.load()) {
        (void)rmw_wait(&idle_subs, nullptr, nullptr, nullptr, nullptr, thief_ws, &brief);
      }
    });

  for (int round = 0; round < 50; ++round) {
    std::atomic<bool> waiting{false};
    std::atomic<rmw_ret_t> wait_ret{RMW_RET_ERROR};
    std::thread waiter([&]() {
        waiting.store(true);
        wait_ret.store(
          rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, ws, &timeout));
      });
    while (!waiting.load()) {
      std::this_thread::yield();
    }

    test_msgs::msg::BasicTypes msg;
    msg.int32_value = round;
    ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

    waiter.join();
    EXPECT_EQ(RMW_RET_OK, wait_ret.load()) << "round " << round;

    bool taken = true;
    while (taken) {
      test_msgs::msg::BasicTypes recv;
      ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    }
  }

  stop.store(true);
  thief.join();

  auto _w [[maybe_unused]] = rmw_destroy_wait_set(thief_ws);
  auto _i [[maybe_unused]] = rmw_destroy_subscription(node, idle);
  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// A wait set's delivery fd has to leave context->delivery_fds before it is
// closed. The other order leaves the listener holding a stale fd number that
// the next open() in the process reuses, and its 8-byte write lands in whatever
// that turns out to be. Churns wait sets against a listener that is delivering
// the whole time; the payoff is under the sanitizer jobs.
TEST_F(ListenerCallbackTest, DestroyingWaitSetsWhileTheListenerDeliversIsSafe)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_wschurn", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_wschurn", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

  std::atomic<bool> stop{false};
  std::thread publisher([&]() {
      int32_t i = 0;
      while (!stop.load()) {
        test_msgs::msg::BasicTypes msg;
        msg.int32_value = i++;
        (void)rmw_publish(pub, &msg, nullptr);
        std::this_thread::sleep_for(std::chrono::microseconds(200));
      }
    });

  for (int round = 0; round < 200; ++round) {
    rmw_wait_set_t * churn = rmw_create_wait_set(&context, 1);
    ASSERT_NE(nullptr, churn) << "round " << round;
    ASSERT_EQ(RMW_RET_OK, rmw_destroy_wait_set(churn)) << "round " << round;
  }

  stop.store(true);
  publisher.join();

  // The listener was the only thing draining, so this also confirms it kept
  // running across the churn.
  EXPECT_GT(counter.events.load(), 0u);
  EXPECT_FALSE(counter.saw_zero.load());

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// listener_watch read is_shutdown BEFORE taking listener_mutex, and
// listener_stop releases that mutex before its join(). A registration landing
// in that window finds listener_running already false and calls
// listener_start, which move-assigns over a std::thread that is still
// joinable - and that calls std::terminate(). rmw_context_fini reaches the
// same stop path without setting is_shutdown at all, so moving the flag read
// under the mutex does not close it on its own.
//
// This one fails by aborting the process rather than by failing an assertion,
// which is what a std::terminate bug looks like from a test.
TEST_F(ListenerCallbackTest, RegisteringACallbackWhileTheContextShutsDownDoesNotAbort)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();

  for (int round = 0; round < 250; ++round) {
    rmw_context_t ctx = rmw_get_zero_initialized_context();
    rmw_init_options_t opts = rmw_get_zero_initialized_init_options();
    ASSERT_EQ(RMW_RET_OK, rmw_init_options_init(&opts, rcutils_get_default_allocator()));
    // A private domain: this context is torn down mid-test and must not
    // disturb the fixture's registry.
    opts.domain_id = 97;
    ASSERT_EQ(RMW_RET_OK, rmw_init(&opts, &ctx));

    auto * n = rmw_create_node(&ctx, "shutdown_race", "/test_ns");
    ASSERT_NE(nullptr, n) << "round " << round;
    auto sub_opts = rmw_get_default_subscription_options();
    auto * sub = rmw_create_subscription(n, ts, "/listener_shutdown_race", &qos, &sub_opts);
    ASSERT_NE(nullptr, sub) << "round " << round;

    // Start the listener, so the shutdown below takes its stop-and-join path.
    ASSERT_EQ(
      RMW_RET_OK,
      rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

    // Several registrars, so that when listener_stop releases listener_mutex
    // at least one is likely to be queued on it having already passed the
    // is_shutdown check - which is the interleaving that crashes.
    std::atomic<bool> stop{false};
    std::vector<std::thread> registrars;
    for (int r = 0; r < 4; ++r) {
      registrars.emplace_back([&]() {
          while (!stop.load()) {
            // Both calls reach the listener. One landing between
            // listener_stop's unlock and its join is the crash.
            (void)rmw_subscription_set_on_new_message_callback(sub, nullptr, nullptr);
            (void)rmw_subscription_set_on_new_message_callback(
              sub, CallbackCounter::fire, &counter);
          }
        });
    }

    EXPECT_EQ(RMW_RET_OK, rmw_shutdown(&ctx)) << "round " << round;
    stop.store(true);
    for (auto & t : registrars) {
      t.join();
    }

    auto _s [[maybe_unused]] = rmw_destroy_subscription(n, sub);
    auto _n [[maybe_unused]] = rmw_destroy_node(n);
    EXPECT_EQ(RMW_RET_OK, rmw_context_fini(&ctx)) << "round " << round;
    EXPECT_EQ(RMW_RET_OK, rmw_init_options_fini(&opts)) << "round " << round;
  }
}

// drain_endpoint() recv()s outside queue_mutex and push_back()s inside it, so
// two concurrent drains of one socket can interleave as recv A / recv B /
// push B / push A and land same-publisher datagrams out of order. The queue
// mutex prevents corruption, not reordering, and ROS 2 guarantees
// per-publisher FIFO. Both drainers are reachable through the public API
// alone: the listener drains because a callback is registered, and rmw_take
// drains on every call.
TEST_F(ListenerCallbackTest, ConcurrentDrainsPreservePublisherOrder)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();

  // Deep enough that the QoS trim never drops a sample, so an out-of-order
  // pair below is reordering rather than overflow.
  rmw_qos_profile_t deep = qos;
  deep.depth = 5000;

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_order", &deep, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_order", &deep, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  // Hands the socket to the listener, which then drains it concurrently with
  // the rmw_take loop below.
  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, CallbackCounter::fire, &counter));

  const int32_t total = 1500;
  std::thread publisher([&]() {
      for (int32_t i = 0; i < total; ++i) {
        test_msgs::msg::BasicTypes msg;
        msg.int32_value = i;
        (void)rmw_publish(pub, &msg, nullptr);
      }
    });

  int32_t prev = -1;
  int32_t inversions = 0;
  int32_t first_bad_prev = -1, first_bad = -1;
  size_t collected = 0;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (collected < static_cast<size_t>(total) &&
    std::chrono::steady_clock::now() < deadline)
  {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    if (!taken) {
      continue;
    }
    if (recv.int32_value <= prev) {
      if (inversions == 0) {
        first_bad_prev = prev;
        first_bad = recv.int32_value;
      }
      ++inversions;
    }
    prev = recv.int32_value;
    ++collected;
  }
  publisher.join();

  EXPECT_EQ(0, inversions)
    << "first inversion: " << first_bad_prev << " then " << first_bad
    << " (collected " << collected << " of " << total << ")";
  // Guards against the assertion above passing vacuously.
  EXPECT_GT(collected, static_cast<size_t>(total) / 2);

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// A listener callback that takes from its own endpoint. drain_endpoint()
// releases drain_mutex and queue_mutex before it notifies, and the backlog
// flush in the setter does the same, so a callback runs holding only
// callback_mutex and this is legal. Hold either data lock across the
// notification and rmw_take deadlocks against it one frame up, so this test
// fails by wedging the binary rather than by failing an assertion.
struct TakingCallback
{
  rmw_subscription_t * sub = nullptr;
  std::atomic<size_t> taken{0};

  static void fire(const void * user_data, size_t)
  {
    auto * self =
      const_cast<TakingCallback *>(static_cast<const TakingCallback *>(user_data));
    test_msgs::msg::BasicTypes msg;
    bool got = false;
    if (rmw_take(self->sub, &msg, &got, nullptr) == RMW_RET_OK && got) {
      self->taken.fetch_add(1);
    }
  }
};

TEST_F(ListenerCallbackTest, ACallbackMayTakeFromItsOwnEndpoint)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/listener_reentrant", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/listener_reentrant", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  TakingCallback tc;
  tc.sub = sub;

  // Queue a backlog with no callback installed, so the registration below
  // fires the setter's flush path with entries already waiting.
  for (int32_t i = 0; i < 3; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i;
    ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }
  wait_on_subscription(sub);

  // Deadlocks here if the flush still holds queue_mutex across the callback.
  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, TakingCallback::fire, &tc));

  // And here if the listener's drain still holds drain_mutex across it.
  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 99;
  ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  wait_on_subscription(sub);

  EXPECT_GT(tc.taken.load(), 0u) << "the callback never managed a take";

  auto _c [[maybe_unused]] =
    rmw_subscription_set_on_new_message_callback(sub, nullptr, nullptr);
  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}
