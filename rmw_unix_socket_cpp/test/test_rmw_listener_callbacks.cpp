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
#include <cstring>

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
TEST_F(ListenerCallbackTest, SubscriptionCallbackFiresFromWait)
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

// One notification per drain carrying the batch size, not one per message.
// rmw/event_callback_type.h documents number_of_events as the count since the
// callback was last called and explicitly allows > 1.
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
  EXPECT_EQ(1u, counter.calls.load());
  EXPECT_FALSE(counter.saw_zero.load());

  auto _s [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _p [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

// number_of_events is a take credit: the executor calls take() once per event
// it is told about. A drain that overflows the queue must therefore report
// what the queue kept, not what the socket handed it, or the executor spends
// the difference on takes that find nothing.
TEST_F(ListenerCallbackTest, BatchCountExcludesDatagramsDroppedByOverflow)
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

  // qos.depth is 10, so a single drain of 15 keeps the last 10 and drops 5.
  for (int32_t i = 0; i < 15; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i;
    ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }

  wait_on_subscription(sub);

  EXPECT_EQ(qos.depth, counter.events.load());
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
