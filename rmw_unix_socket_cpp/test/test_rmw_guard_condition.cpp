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

TEST_F(RmwUdsTestBase, CreateDestroyGuardCondition)
{
  auto * gc = rmw_create_guard_condition(&context);
  ASSERT_NE(nullptr, gc);
  EXPECT_EQ(uds_id(), gc->implementation_identifier);
  EXPECT_NE(nullptr, gc->data);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(gc));
}

TEST_F(RmwUdsTestBase, TriggerGuardCondition)
{
  auto * gc = rmw_create_guard_condition(&context);
  ASSERT_NE(nullptr, gc);

  auto * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);

  EXPECT_EQ(RMW_RET_OK, rmw_trigger_guard_condition(gc));

  // A wait after the trigger must return immediately with the entry ready
  rmw_guard_conditions_t guard_conditions;
  void * gc_array[1] = {gc->data};
  guard_conditions.guard_conditions = gc_array;
  guard_conditions.guard_condition_count = 1;

  rmw_time_t timeout;
  timeout.sec = 0;
  timeout.nsec = 100000000;  // 100ms

  rmw_ret_t ret = rmw_wait(nullptr, &guard_conditions, nullptr, nullptr, nullptr, ws, &timeout);
  EXPECT_EQ(RMW_RET_OK, ret);
  EXPECT_NE(nullptr, guard_conditions.guard_conditions[0]);

  // The trigger is one-shot: a second wait must time out with the entry nulled
  gc_array[0] = gc->data;
  timeout.sec = 0;
  timeout.nsec = 50000000;  // 50ms

  ret = rmw_wait(nullptr, &guard_conditions, nullptr, nullptr, nullptr, ws, &timeout);
  EXPECT_EQ(RMW_RET_TIMEOUT, ret);
  EXPECT_EQ(nullptr, guard_conditions.guard_conditions[0]);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(gc));
}

TEST_F(RmwUdsTestBase, NullGuardConditionArgs)
{
  EXPECT_EQ(nullptr, rmw_create_guard_condition(nullptr));
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_destroy_guard_condition(nullptr));
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_trigger_guard_condition(nullptr));
}
