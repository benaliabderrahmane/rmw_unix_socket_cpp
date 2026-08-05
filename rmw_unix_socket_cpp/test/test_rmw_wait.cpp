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

#include "test_msgs/msg/basic_types.hpp"

#include "rmw/qos_profiles.h"
#include "rosidl_typesupport_cpp/message_type_support.hpp"

#include "../src/types.hpp"

TEST_F(RmwUdsTestBase, CreateDestroyWaitSet)
{
  auto * ws = rmw_create_wait_set(&context, 10);
  ASSERT_NE(nullptr, ws);
  EXPECT_EQ(uds_id(), ws->implementation_identifier);
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
}

TEST_F(RmwUdsNodeTest, WaitWithGuardCondition)
{
  auto * gc = rmw_create_guard_condition(&context);
  ASSERT_NE(nullptr, gc);

  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);

  // Trigger before wait
  EXPECT_EQ(RMW_RET_OK, rmw_trigger_guard_condition(gc));

  // Wait should return immediately
  rmw_guard_conditions_t guard_conditions;
  void * gc_array[1] = {gc->data};
  guard_conditions.guard_conditions = gc_array;
  guard_conditions.guard_condition_count = 1;

  rmw_time_t timeout;
  timeout.sec = 0;
  timeout.nsec = 100000000;  // 100ms

  rmw_ret_t ret = rmw_wait(nullptr, &guard_conditions, nullptr, nullptr, nullptr, ws, &timeout);
  EXPECT_EQ(RMW_RET_OK, ret);
  // The triggered guard condition should still be non-null
  EXPECT_NE(nullptr, guard_conditions.guard_conditions[0]);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(gc));
}

TEST_F(RmwUdsNodeTest, WaitTimeoutWhenNoData)
{
  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);

  auto * gc = rmw_create_guard_condition(&context);
  ASSERT_NE(nullptr, gc);

  rmw_guard_conditions_t guard_conditions;
  void * gc_array[1] = {gc->data};
  guard_conditions.guard_conditions = gc_array;
  guard_conditions.guard_condition_count = 1;

  rmw_time_t timeout;
  timeout.sec = 0;
  timeout.nsec = 10000000;  // 10ms

  rmw_ret_t ret = rmw_wait(nullptr, &guard_conditions, nullptr, nullptr, nullptr, ws, &timeout);
  EXPECT_EQ(RMW_RET_TIMEOUT, ret);
  // Guard condition should be nulled out (not triggered)
  EXPECT_EQ(nullptr, guard_conditions.guard_conditions[0]);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(gc));
}

TEST_F(RmwUdsNodeTest, WaitWithSubscription)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();

  rmw_qos_profile_t qos;
  std::memset(&qos, 0, sizeof(qos));
  qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
  qos.depth = 10;
  qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
  qos.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/wait_test", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/wait_test", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);

  // Publish a message
  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 77;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  // Wait on subscription
  rmw_subscriptions_t subscriptions;
  void * sub_array[1] = {sub->data};
  subscriptions.subscribers = sub_array;
  subscriptions.subscriber_count = 1;

  rmw_time_t timeout;
  timeout.sec = 1;
  timeout.nsec = 0;

  rmw_ret_t ret = rmw_wait(&subscriptions, nullptr, nullptr, nullptr, nullptr, ws, &timeout);
  EXPECT_EQ(RMW_RET_OK, ret);
  EXPECT_NE(nullptr, subscriptions.subscribers[0]);

  // Take the message
  test_msgs::msg::BasicTypes recv;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
  EXPECT_TRUE(taken);
  EXPECT_EQ(77, recv.int32_value);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, sub));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, pub));
}

// drain_socket() (rmw_wait.cpp) applies the same ignore_local_publications
// filter as drain_subscription(), so a subscription created with
// ignore_local_publications=true must not wake a wait set for a message
// published from within the same process/context.
TEST_F(RmwUdsNodeTest, WaitDoesNotWakeForIgnoredLocalPublication)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();

  rmw_qos_profile_t qos;
  std::memset(&qos, 0, sizeof(qos));
  qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
  qos.depth = 10;
  qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
  qos.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/wait_ignore_local", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub_opts.ignore_local_publications = true;
  auto * sub = rmw_create_subscription(node, ts, "/wait_ignore_local", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);

  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 77;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));

  rmw_subscriptions_t subscriptions;
  void * sub_array[1] = {sub->data};
  subscriptions.subscribers = sub_array;
  subscriptions.subscriber_count = 1;

  rmw_time_t timeout;
  timeout.sec = 0;
  timeout.nsec = 200000000;  // 200 ms — no wakeup is expected

  rmw_ret_t ret = rmw_wait(&subscriptions, nullptr, nullptr, nullptr, nullptr, ws, &timeout);
  EXPECT_EQ(RMW_RET_TIMEOUT, ret) <<
    "ignore_local_publications=true must not wake the wait set for a "
    "same-context publication";
  EXPECT_EQ(nullptr, subscriptions.subscribers[0]);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, sub));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, pub));
}

TEST_F(RmwUdsNodeTest, NodeGraphGuardConditionTriggersOnGraphChange)
{
  // Scenario: graph-change notification. rclcpp's GraphListener (and
  // wait_for_service, on_graph_change callbacks) blocks on the node's graph
  // guard condition and relies on it firing when the ROS graph changes.
  // Block on that guard condition alone, then create a subscription from
  // another thread: the wait must wake with the guard condition ready, well
  // before the timeout. Without this, wait_for_service can hang forever even
  // though the service is up.
  const rmw_guard_condition_t * graph_gc = rmw_node_get_graph_guard_condition(node);
  ASSERT_NE(nullptr, graph_gc);

  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);

  auto wait_on_graph_gc = [&](rmw_time_t timeout) {
      void * gc_array[1] = {graph_gc->data};
      rmw_guard_conditions_t gcs;
      gcs.guard_conditions = gc_array;
      gcs.guard_condition_count = 1;
      rmw_ret_t ret = rmw_wait(nullptr, &gcs, nullptr, nullptr, nullptr, ws, &timeout);
      // Ready iff rmw_wait returned OK and kept the entry non-null.
      return ret == RMW_RET_OK && gcs.guard_conditions[0] != nullptr;
    };

  // Settle first. The fixture's own node registration left an unconsumed
  // registry generation edge, and a wait that starts on it reports the guard
  // condition ready without ever blocking — which would let this test pass even
  // with the wakeup path removed entirely. Consume pending edges until a wait
  // genuinely blocks and times out.
  bool settled = false;
  for (int i = 0; i < 50 && !settled; ++i) {
    settled = !wait_on_graph_gc(rmw_time_t{0, 20000000});  // 20 ms
  }
  ASSERT_TRUE(settled) <<
    "the graph guard condition never settled, so the wait below would not block";

  // From here the wait can only be satisfied by the graph change made below.
  std::atomic<bool> woke_ready{false};
  std::atomic<int64_t> blocked_ms{-1};
  std::thread waiter(
    [&] {
      auto t0 = std::chrono::steady_clock::now();
      bool ready = wait_on_graph_gc(rmw_time_t{3, 0});
      blocked_ms.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - t0).count());
      woke_ready.store(ready);
    });

  // Let the waiter reach epoll, then change the graph.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  auto * ts_local = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  rmw_qos_profile_t qos = rmw_qos_profile_default;
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts_local, "/graph_gc_probe", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  waiter.join();
  EXPECT_TRUE(woke_ready.load()) <<
    "the node's graph guard condition was not triggered by a graph change";
  // Proves the wake came from the graph change rather than from an edge that
  // was already pending when the wait started.
  EXPECT_GE(blocked_ms.load(), 150) <<
    "the wait did not block; it was already satisfied before the graph changed";
  EXPECT_LT(blocked_ms.load(), 3000) << "the wait ran to its timeout instead of waking";

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_wait_set(ws);
}

TEST_F(RmwUdsNodeTest, WaitBlocksForFullCallerTimeout)
{
  // Scenario: the caller's timeout is a contract. Callers such as
  // rclcpp::wait_for_message and WaitSet::wait treat an early RMW_RET_TIMEOUT
  // as "nothing arrived in my window" — if rmw_wait returns before the
  // caller's deadline, they misreport. With nothing ready, a 600 ms wait must
  // block ~600 ms and only then return RMW_RET_TIMEOUT.
  auto * gc = rmw_create_guard_condition(&context);
  ASSERT_NE(nullptr, gc);
  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);

  void * gc_array[1] = {gc->data};
  rmw_guard_conditions_t gcs;
  gcs.guard_conditions = gc_array;
  gcs.guard_condition_count = 1;

  rmw_time_t timeout{0, 600000000};  // 600 ms, never triggered
  auto t0 = std::chrono::steady_clock::now();
  rmw_ret_t ret = rmw_wait(nullptr, &gcs, nullptr, nullptr, nullptr, ws, &timeout);
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - t0).count();

  EXPECT_EQ(RMW_RET_TIMEOUT, ret);
  EXPECT_GE(elapsed_ms, 550) <<
    "rmw_wait returned TIMEOUT before the caller's 600 ms deadline";
  EXPECT_LE(elapsed_ms, 1500) << "rmw_wait overshot the deadline";

  auto _r1 [[maybe_unused]] = rmw_destroy_wait_set(ws);
  auto _r2 [[maybe_unused]] = rmw_destroy_guard_condition(gc);
}

TEST_F(RmwUdsNodeTest, LatchedTopicSurvivesAnUnresponsiveParticipant)
{
  // Scenario: one participant on the domain initializes but never services
  // its wait loop (a hung or busy process). However much graph churn its
  // unread notifications accumulate, the rest of the system must keep
  // working: a latched (TRANSIENT_LOCAL) message published by an idle node
  // must still reach a subscriber that joins after heavy churn.
  rmw_init_options_t opts2 = rmw_get_zero_initialized_init_options();
  rcutils_allocator_t allocator = rcutils_get_default_allocator();
  ASSERT_EQ(RMW_RET_OK, rmw_init_options_init(&opts2, allocator));
  opts2.domain_id = 99;  // same domain as the fixture
  rmw_context_t ctx2 = rmw_get_zero_initialized_context();
  ASSERT_EQ(RMW_RET_OK, rmw_init(&opts2, &ctx2));  // never waits, never drains

  auto * ts_local = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  rmw_qos_profile_t latched = rmw_qos_profile_default;
  latched.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
  latched.durability = RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL;
  latched.depth = 5;

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(
    node, ts_local, "/unresponsive_latched", &latched, &pub_opts);
  ASSERT_NE(nullptr, pub);
  test_msgs::msg::BasicTypes m;
  m.int32_value = 21;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));

  // The healthy participant's executor: idle, blocked, servicing its waits —
  // exactly what a quiet production node does.
  auto * gc = rmw_create_guard_condition(&context);
  ASSERT_NE(nullptr, gc);
  auto * ws = rmw_create_wait_set(&context, 4);
  ASSERT_NE(nullptr, ws);
  std::atomic<bool> stop{false};
  std::thread executor(
    [&] {
      while (!stop.load()) {
        void * gc_array[1] = {gc->data};
        rmw_guard_conditions_t gcs;
        gcs.guard_conditions = gc_array;
        gcs.guard_condition_count = 1;
        auto _r [[maybe_unused]] = rmw_wait(
          nullptr, &gcs, nullptr, nullptr, nullptr, ws, nullptr);
      }
    });
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  // Heavy graph churn while the second participant stays unresponsive. The
  // healthy executor keeps draining its own notifications throughout, so any
  // per-sender resource pinned by the unresponsive peer stays pinned.
  rmw_qos_profile_t qos = rmw_qos_profile_default;
  for (int i = 0; i < 600; ++i) {
    auto * p = rmw_create_publisher(node, ts_local, "/churn", &qos, &pub_opts);
    ASSERT_NE(nullptr, p);
    ASSERT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, p));
  }

  // A late joiner after the churn must still receive the latched message.
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(
    node, ts_local, "/unresponsive_latched", &latched, &sub_opts);
  ASSERT_NE(nullptr, sub);

  bool got = false;
  for (int i = 0; i < 300 && !got; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    if (rmw_take(sub, &recv, &taken, nullptr) == RMW_RET_OK && taken &&
      recv.int32_value == 21)
    {
      got = true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  stop.store(true);
  auto _t [[maybe_unused]] = rmw_trigger_guard_condition(gc);
  executor.join();

  EXPECT_TRUE(got) <<
    "a participant that never drains its notifications starved a healthy "
    "idle publisher: the latched message never reached the late joiner";

  auto _r1 [[maybe_unused]] = rmw_destroy_subscription(node, sub);
  auto _r2 [[maybe_unused]] = rmw_destroy_wait_set(ws);
  auto _r3 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
  auto _r4 [[maybe_unused]] = rmw_destroy_guard_condition(gc);
  EXPECT_EQ(RMW_RET_OK, rmw_shutdown(&ctx2));
  EXPECT_EQ(RMW_RET_OK, rmw_context_fini(&ctx2));
  EXPECT_EQ(RMW_RET_OK, rmw_init_options_fini(&opts2));
}
