#include "test_base.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include "../src/types.hpp"

TEST_F(RmwUdsTestBase, CreateDestroyGuardCondition)
{
  auto * gc = rmw_create_guard_condition(&context);
  ASSERT_NE(nullptr, gc);
  EXPECT_EQ(uds_id(), gc->implementation_identifier);
  EXPECT_NE(nullptr, gc->data);

  auto * data = static_cast<rmw_uds::UdsGuardCondition *>(gc->data);
  EXPECT_GE(data->eventfd_fd, 0);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(gc));
}

TEST_F(RmwUdsTestBase, TriggerGuardCondition)
{
  auto * gc = rmw_create_guard_condition(&context);
  ASSERT_NE(nullptr, gc);

  EXPECT_EQ(RMW_RET_OK, rmw_trigger_guard_condition(gc));

  // Verify the eventfd is readable
  auto * data = static_cast<rmw_uds::UdsGuardCondition *>(gc->data);
  uint64_t val = 0;
  ssize_t r = read(data->eventfd_fd, &val, sizeof(val));
  EXPECT_EQ(static_cast<ssize_t>(sizeof(val)), r);
  EXPECT_GT(val, 0u);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_guard_condition(gc));
}

TEST_F(RmwUdsTestBase, NullGuardConditionArgs)
{
  EXPECT_EQ(nullptr, rmw_create_guard_condition(nullptr));
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_destroy_guard_condition(nullptr));
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_trigger_guard_condition(nullptr));
}
