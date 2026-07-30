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
