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

// A same-context publication dropped by ignore_local_publications wakes the
// epoll but leaves nothing to take. rmw_wait may return early, but only the
// caller's own deadline may produce RMW_RET_TIMEOUT.
TEST_F(RmwUdsNodeTest, IgnoredLocalPublicationIsNotReportedAsTimeout)
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
  auto * pub = rmw_create_publisher(node, ts, "/ignore_local_no_timeout", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub_opts.ignore_local_publications = true;
  auto * sub = rmw_create_subscription(node, ts, "/ignore_local_no_timeout", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);

  // Publish only once the wait is already blocked, so the drop happens on the
  // post-epoll drain rather than the pre-epoll one.
  std::thread publisher([&]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(150));
      test_msgs::msg::BasicTypes m;
      m.int32_value = 42;
      rmw_publish(pub, &m, nullptr);
    });

  rmw_subscriptions_t subscriptions;
  void * sub_array[1] = {sub->data};
  subscriptions.subscribers = sub_array;
  subscriptions.subscriber_count = 1;

  rmw_time_t timeout;
  timeout.sec = 2;
  timeout.nsec = 0;

  auto t0 = std::chrono::steady_clock::now();
  rmw_ret_t ret = rmw_wait(&subscriptions, nullptr, nullptr, nullptr, nullptr, ws, &timeout);
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - t0).count();
  publisher.join();

  EXPECT_TRUE(ret != RMW_RET_TIMEOUT || elapsed_ms >= 1900) <<
    "rmw_wait reported RMW_RET_TIMEOUT after " << elapsed_ms <<
    " ms for a 2000 ms deadline";
  EXPECT_EQ(nullptr, subscriptions.subscribers[0]);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, sub));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, pub));
}

// rmw_wait(..., nullptr) blocks indefinitely, so RMW_RET_TIMEOUT has no meaning
// for it: rclcpp's GraphListener treats that code as fatal. Dropping a
// same-context publication must not produce it.
TEST_F(RmwUdsNodeTest, InfiniteWaitNeverTimesOutOnIgnoredLocalPublication)
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
  auto * pub = rmw_create_publisher(node, ts, "/ignore_local_infinite", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub_opts.ignore_local_publications = true;
  auto * sub = rmw_create_subscription(node, ts, "/ignore_local_infinite", &qos, &sub_opts);
  auto * gc = rmw_create_guard_condition(&context);  // unblocks the wait on teardown
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);
  ASSERT_NE(nullptr, gc);

  auto * ws = rmw_create_wait_set(&context, 2);
  ASSERT_NE(nullptr, ws);

  std::atomic<bool> returned{false};
  std::atomic<int> ret{RMW_RET_OK};
  std::thread waiter([&]() {
      rmw_subscriptions_t subs;
      void * sub_array[1] = {sub->data};
      subs.subscribers = sub_array;
      subs.subscriber_count = 1;
      rmw_guard_conditions_t gcs;
      void * gc_array[1] = {gc->data};
      gcs.guard_conditions = gc_array;
      gcs.guard_condition_count = 1;
      ret = rmw_wait(&subs, &gcs, nullptr, nullptr, nullptr, ws, nullptr);
      returned = true;
    });

  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  test_msgs::msg::BasicTypes m;
  m.int32_value = 7;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  // Returning early with nothing ready is allowed; returning TIMEOUT is not.
  if (returned.load()) {
    EXPECT_NE(RMW_RET_TIMEOUT, ret.load()) <<
      "infinite rmw_wait returned RMW_RET_TIMEOUT after a dropped same-context "
      "publication";
  }

  rmw_trigger_guard_condition(gc);
  waiter.join();
  EXPECT_NE(RMW_RET_TIMEOUT, ret.load());

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(gc));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, sub));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, pub));
}

// The armed-fd cache survives across rmw_wait calls. A guard condition armed
// by an earlier call but absent from THIS call's array (its node moved to
// another executor, its callback group is busy) must not have its trigger
// consumed by this wait: the wakeup belongs to whichever wait set holds the
// GC now, and consuming it here loses it forever.
TEST_F(RmwUdsNodeTest, WaitDoesNotStealTriggerOfGuardConditionNotWaitedOn)
{
  auto * g1 = rmw_create_guard_condition(&context);
  auto * g2 = rmw_create_guard_condition(&context);
  auto * ws = rmw_create_wait_set(&context, 2);
  ASSERT_NE(nullptr, g1);
  ASSERT_NE(nullptr, g2);
  ASSERT_NE(nullptr, ws);

  // Arm both fds in ws (neither is triggered, so this times out).
  {
    void * both[2] = {g1->data, g2->data};
    rmw_guard_conditions_t gcs;
    gcs.guard_conditions = both;
    gcs.guard_condition_count = 2;
    rmw_time_t t{0, 20000000};  // 20 ms
    EXPECT_EQ(
      RMW_RET_TIMEOUT,
      rmw_wait(nullptr, &gcs, nullptr, nullptr, nullptr, ws, &t));
  }

  // Wait on g1 alone while g2 fires mid-wait. g2's still-armed fd reports
  // ready in this wait set, but g2 is not in this call's array: the wait must
  // neither consume the trigger nor end early on it.
  std::thread trigger([&] {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      auto _r [[maybe_unused]] = rmw_trigger_guard_condition(g2);
    });
  {
    void * only_g1[1] = {g1->data};
    rmw_guard_conditions_t gcs;
    gcs.guard_conditions = only_g1;
    gcs.guard_condition_count = 1;
    rmw_time_t t{0, 300000000};  // 300 ms
    auto t0 = std::chrono::steady_clock::now();
    rmw_ret_t ret = rmw_wait(nullptr, &gcs, nullptr, nullptr, nullptr, ws, &t);
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - t0).count();
    trigger.join();
    EXPECT_EQ(RMW_RET_TIMEOUT, ret) <<
      "a guard condition outside this call's array ended the wait";
    EXPECT_EQ(nullptr, gcs.guard_conditions[0]);
    EXPECT_GE(elapsed_ms, 250) << "the wait did not re-block after g2 fired";
  }

  // The trigger must still be pending for a wait that DOES hold g2.
  {
    void * only_g2[1] = {g2->data};
    rmw_guard_conditions_t gcs;
    gcs.guard_conditions = only_g2;
    gcs.guard_condition_count = 1;
    rmw_time_t t{0, 100000000};  // 100 ms
    EXPECT_EQ(
      RMW_RET_OK,
      rmw_wait(nullptr, &gcs, nullptr, nullptr, nullptr, ws, &t)) <<
      "g2's trigger was consumed by a wait that was not waiting on it";
    EXPECT_NE(nullptr, gcs.guard_conditions[0]);
  }

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(g1));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(g2));
}

// The armed cache is keyed by fd; the uid comparison is what tells "the same
// fd re-armed for the same entity" from "the fd number recycled to a new
// entity after a close". Recycle an eventfd number into a new GC on the same
// wait set: the new GC must be re-armed and still wake the wait.
TEST_F(RmwUdsNodeTest, RecycledFdNumberIsRearmedForNewEntity)
{
  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);

  auto wait_on = [&](rmw_guard_condition_t * gc, rmw_time_t t) {
      void * arr[1] = {gc->data};
      rmw_guard_conditions_t gcs;
      gcs.guard_conditions = arr;
      gcs.guard_condition_count = 1;
      rmw_ret_t ret = rmw_wait(nullptr, &gcs, nullptr, nullptr, nullptr, ws, &t);
      return ret == RMW_RET_OK && gcs.guard_conditions[0] != nullptr;
    };

  auto * g1 = rmw_create_guard_condition(&context);
  ASSERT_NE(nullptr, g1);
  const int fd1 = static_cast<rmw_uds::UdsGuardCondition *>(g1->data)->eventfd_fd;
  EXPECT_FALSE(wait_on(g1, rmw_time_t{0, 20000000}));  // arms fd1 for g1
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(g1));

  // Provoke fd-number reuse: the kernel hands out the lowest free descriptor,
  // so the next eventfd normally lands on fd1 at once. Keep non-matching
  // candidates alive so retries do not just get the same number back.
  rmw_guard_condition_t * g2 = nullptr;
  std::vector<rmw_guard_condition_t *> decoys;
  for (int i = 0; i < 32 && !g2; ++i) {
    auto * cand = rmw_create_guard_condition(&context);
    ASSERT_NE(nullptr, cand);
    if (static_cast<rmw_uds::UdsGuardCondition *>(cand->data)->eventfd_fd == fd1) {
      g2 = cand;
    } else {
      decoys.push_back(cand);
    }
  }
  for (auto * d : decoys) {
    EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(d));
  }
  if (!g2) {
    EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
    GTEST_SKIP() << "eventfd number was not recycled; nothing to pin";
  }

  std::thread trigger([&] {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      auto _r [[maybe_unused]] = rmw_trigger_guard_condition(g2);
    });
  const bool woke = wait_on(g2, rmw_time_t{2, 0});
  trigger.join();
  EXPECT_TRUE(woke) <<
    "a recycled fd number kept its stale arming; the new entity never wakes";

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(g2));
}

// When an entity already holds queued-but-untaken data, the wait makes one
// non-blocking epoll pass instead of blocking: an entity whose data still
// sits unread in its socket must be reported in this same call, and the
// caller's timeout must not be consumed by a wait that already has work.
TEST_F(RmwUdsNodeTest, QueuedBacklogStillReportsSocketDataWithoutBlocking)
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  rmw_qos_profile_t qos = rmw_qos_profile_default;
  auto pub_opts = rmw_get_default_publisher_options();
  auto sub_opts = rmw_get_default_subscription_options();

  auto * pub_x = rmw_create_publisher(node, ts, "/backlog_x", &qos, &pub_opts);
  auto * sub_x = rmw_create_subscription(node, ts, "/backlog_x", &qos, &sub_opts);
  auto * pub_y = rmw_create_publisher(node, ts, "/backlog_y", &qos, &pub_opts);
  auto * sub_y = rmw_create_subscription(node, ts, "/backlog_y", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub_x);
  ASSERT_NE(nullptr, sub_x);
  ASSERT_NE(nullptr, pub_y);
  ASSERT_NE(nullptr, sub_y);

  auto * ws = rmw_create_wait_set(&context, 2);
  ASSERT_NE(nullptr, ws);

  // Two messages for X; wait on X alone so both land in X's queue, take one.
  test_msgs::msg::BasicTypes m;
  m.int32_value = 1;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub_x, &m, nullptr));
  m.int32_value = 2;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub_x, &m, nullptr));
  {
    rmw_subscriptions_t subs;
    void * arr[1] = {sub_x->data};
    subs.subscribers = arr;
    subs.subscriber_count = 1;
    rmw_time_t t{1, 0};
    ASSERT_EQ(
      RMW_RET_OK, rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, ws, &t));
  }
  test_msgs::msg::BasicTypes recv;
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take(sub_x, &recv, &taken, nullptr));
  ASSERT_TRUE(taken);

  // One message for Y, left sitting in Y's socket (Y was never waited on).
  m.int32_value = 3;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub_y, &m, nullptr));

  // X's leftover queue makes the wait poll-only; Y's socket data must still be
  // reported in this call, well before the 5 s deadline.
  rmw_subscriptions_t subs;
  void * arr[2] = {sub_x->data, sub_y->data};
  subs.subscribers = arr;
  subs.subscriber_count = 2;
  rmw_time_t t{5, 0};
  auto t0 = std::chrono::steady_clock::now();
  rmw_ret_t ret = rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, ws, &t);
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - t0).count();

  EXPECT_EQ(RMW_RET_OK, ret);
  EXPECT_NE(nullptr, subs.subscribers[0]) << "X's queued backlog went unreported";
  EXPECT_NE(nullptr, subs.subscribers[1]) <<
    "Y's socket data was missed by the poll-only pass";
  EXPECT_LT(elapsed_ms, 1000) <<
    "a wait with queued work blocked instead of polling";

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, sub_x));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, sub_y));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, pub_x));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, pub_y));
}

// A doorbell-only wake carries no caller-visible work: under continuous
// registry churn a bounded wait must keep re-blocking and still time out at
// the caller's deadline — not before it, and not spinning past it.
TEST_F(RmwUdsNodeTest, BoundedWaitUnderRegistryChurnTimesOutOnSchedule)
{
  // Register the context's doorbell first: it is lazily registered by the
  // first wait whose set holds one of this context's graph guard conditions.
  const rmw_guard_condition_t * graph_gc = rmw_node_get_graph_guard_condition(node);
  ASSERT_NE(nullptr, graph_gc);
  auto * ws_reg = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws_reg);
  {
    void * arr[1] = {graph_gc->data};
    rmw_guard_conditions_t gcs;
    gcs.guard_conditions = arr;
    gcs.guard_condition_count = 1;
    rmw_time_t t{0, 20000000};  // 20 ms
    auto _r [[maybe_unused]] = rmw_wait(
      nullptr, &gcs, nullptr, nullptr, nullptr, ws_reg, &t);
  }

  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::BasicTypes>();
  rmw_qos_profile_t qos = rmw_qos_profile_default;
  auto sub_opts = rmw_get_default_subscription_options();
  auto * sub = rmw_create_subscription(node, ts, "/churn_quiet", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  std::atomic<bool> stop{false};
  std::thread churn([&] {
      auto pub_opts = rmw_get_default_publisher_options();
      auto * ts_c = rosidl_typesupport_cpp::get_message_type_support_handle<
        test_msgs::msg::BasicTypes>();
      rmw_qos_profile_t qos_c = rmw_qos_profile_default;
      while (!stop.load()) {
        auto * p = rmw_create_publisher(node, ts_c, "/churn_topic", &qos_c, &pub_opts);
        if (p) {
          auto _r [[maybe_unused]] = rmw_destroy_publisher(node, p);
        }
      }
    });

  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);
  rmw_subscriptions_t subs;
  void * sub_arr[1] = {sub->data};
  subs.subscribers = sub_arr;
  subs.subscriber_count = 1;
  rmw_time_t t{0, 600000000};  // 600 ms, no traffic on the subscription
  auto t0 = std::chrono::steady_clock::now();
  rmw_ret_t ret = rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, ws, &t);
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - t0).count();
  stop.store(true);
  churn.join();

  EXPECT_EQ(RMW_RET_TIMEOUT, ret) <<
    "registry churn ended a wait with nothing ready";
  EXPECT_EQ(nullptr, subs.subscribers[0]);
  EXPECT_GE(elapsed_ms, 550) << "churn wakes ate into the caller's deadline";
  EXPECT_LE(elapsed_ms, 1500) << "the wait overshot the deadline";

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws_reg));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, sub));
}
