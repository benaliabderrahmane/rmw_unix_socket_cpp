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

#include <gtest/gtest.h>

#include "rmw/rmw.h"
#include "rmw/init.h"
#include "rmw/init_options.h"

TEST(RmwInitTest, GetIdentifier)
{
  const char * id = rmw_get_implementation_identifier();
  ASSERT_NE(nullptr, id);
  EXPECT_STREQ("rmw_unix_socket_cpp", id);
}

TEST(RmwInitTest, GetSerializationFormat)
{
  const char * fmt = rmw_get_serialization_format();
  ASSERT_NE(nullptr, fmt);
  EXPECT_STREQ("cdr", fmt);
}

TEST(RmwInitTest, InitOptionsLifecycle)
{
  rmw_init_options_t options = rmw_get_zero_initialized_init_options();
  rcutils_allocator_t allocator = rcutils_get_default_allocator();

  EXPECT_EQ(RMW_RET_OK, rmw_init_options_init(&options, allocator));
  EXPECT_EQ(rmw_get_implementation_identifier(), options.implementation_identifier);

  // Copy
  rmw_init_options_t copy = rmw_get_zero_initialized_init_options();
  EXPECT_EQ(RMW_RET_OK, rmw_init_options_copy(&options, &copy));
  EXPECT_EQ(rmw_get_implementation_identifier(), copy.implementation_identifier);

  EXPECT_EQ(RMW_RET_OK, rmw_init_options_fini(&copy));
  EXPECT_EQ(RMW_RET_OK, rmw_init_options_fini(&options));
}

TEST(RmwInitTest, InitShutdownLifecycle)
{
  rmw_init_options_t options = rmw_get_zero_initialized_init_options();
  rcutils_allocator_t allocator = rcutils_get_default_allocator();
  ASSERT_EQ(RMW_RET_OK, rmw_init_options_init(&options, allocator));
  options.domain_id = 96;

  rmw_context_t context = rmw_get_zero_initialized_context();
  ASSERT_EQ(RMW_RET_OK, rmw_init(&options, &context));
  EXPECT_EQ(rmw_get_implementation_identifier(), context.implementation_identifier);
  EXPECT_EQ(96u, context.actual_domain_id);

  EXPECT_EQ(RMW_RET_OK, rmw_shutdown(&context));
  EXPECT_EQ(RMW_RET_OK, rmw_context_fini(&context));
  EXPECT_EQ(RMW_RET_OK, rmw_init_options_fini(&options));
}

TEST(RmwInitTest, NullArgumentsRejected)
{
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_init(nullptr, nullptr));
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_shutdown(nullptr));
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_context_fini(nullptr));
}
