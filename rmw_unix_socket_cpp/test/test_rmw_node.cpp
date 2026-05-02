#include "test_base.hpp"

TEST_F(RmwUdsTestBase, CreateDestroyNode)
{
  rmw_node_t * node = rmw_create_node(&context, "my_node", "/my_ns");
  ASSERT_NE(nullptr, node);
  EXPECT_STREQ("my_node", node->name);
  EXPECT_STREQ("/my_ns", node->namespace_);
  EXPECT_EQ(uds_id(), node->implementation_identifier);
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_node(node));
}

TEST_F(RmwUdsTestBase, MultipleNodes)
{
  constexpr int N = 10;
  std::vector<rmw_node_t *> nodes;

  for (int i = 0; i < N; ++i) {
    std::string name = "node_" + std::to_string(i);
    auto * n = rmw_create_node(&context, name.c_str(), "/");
    ASSERT_NE(nullptr, n);
    nodes.push_back(n);
  }

  for (auto * n : nodes) {
    EXPECT_EQ(RMW_RET_OK, rmw_destroy_node(n));
  }
}

TEST_F(RmwUdsTestBase, GraphGuardCondition)
{
  rmw_node_t * node = rmw_create_node(&context, "gc_node", "/");
  ASSERT_NE(nullptr, node);

  const rmw_guard_condition_t * gc = rmw_node_get_graph_guard_condition(node);
  ASSERT_NE(nullptr, gc);
  EXPECT_EQ(uds_id(), gc->implementation_identifier);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_node(node));
}

TEST_F(RmwUdsTestBase, NullNodeArgs)
{
  EXPECT_EQ(nullptr, rmw_create_node(nullptr, "n", "/"));
  EXPECT_EQ(nullptr, rmw_create_node(&context, nullptr, "/"));
  EXPECT_EQ(nullptr, rmw_create_node(&context, "n", nullptr));
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_destroy_node(nullptr));
}
