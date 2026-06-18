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

#ifndef RMW_UNIX_SOCKET_CPP__TEST_BASE_HPP_
#define RMW_UNIX_SOCKET_CPP__TEST_BASE_HPP_

#include <gtest/gtest.h>

#include "rmw/rmw.h"
#include "rmw/init.h"
#include "rmw/init_options.h"

// Use the identifier from the shared library, not our own copy.
// Pointer comparison requires the same address.
inline const char * uds_id() { return rmw_get_implementation_identifier(); }

// Base fixture: provides a valid rmw_context_t for tests.
class RmwUdsTestBase : public ::testing::Test
{
protected:
  rmw_context_t context = rmw_get_zero_initialized_context();
  rmw_init_options_t options = rmw_get_zero_initialized_init_options();

  void SetUp() override
  {
    rcutils_allocator_t allocator = rcutils_get_default_allocator();
    ASSERT_EQ(RMW_RET_OK, rmw_init_options_init(&options, allocator));
    // Use a unique domain to avoid collisions with running ROS systems
    options.domain_id = 99;
    ASSERT_EQ(RMW_RET_OK, rmw_init(&options, &context));
  }

  void TearDown() override
  {
    EXPECT_EQ(RMW_RET_OK, rmw_shutdown(&context));
    EXPECT_EQ(RMW_RET_OK, rmw_context_fini(&context));
    EXPECT_EQ(RMW_RET_OK, rmw_init_options_fini(&options));
  }
};

// Fixture that also provides a node.
class RmwUdsNodeTest : public RmwUdsTestBase
{
protected:
  rmw_node_t * node = nullptr;

  void SetUp() override
  {
    RmwUdsTestBase::SetUp();
    node = rmw_create_node(&context, "test_node", "/test_ns");
    ASSERT_NE(nullptr, node);
  }

  void TearDown() override
  {
    if (node) {
      EXPECT_EQ(RMW_RET_OK, rmw_destroy_node(node));
    }
    RmwUdsTestBase::TearDown();
  }
};

#endif  // RMW_UNIX_SOCKET_CPP__TEST_BASE_HPP_
