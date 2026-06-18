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

#include <cstring>

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
