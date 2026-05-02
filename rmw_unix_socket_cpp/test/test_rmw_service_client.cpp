#include "test_base.hpp"

#include <cstring>

#include "test_msgs/srv/basic_types.hpp"

#include "rmw/qos_profiles.h"
#include "rosidl_typesupport_cpp/service_type_support.hpp"

class ServiceClientTest : public RmwUdsNodeTest
{
protected:
  rmw_service_t * srv = nullptr;
  rmw_client_t * cli = nullptr;
  const rosidl_service_type_support_t * ts = nullptr;
  rmw_qos_profile_t qos;

  void SetUp() override
  {
    RmwUdsNodeTest::SetUp();
    ts = rosidl_typesupport_cpp::get_service_type_support_handle<
      test_msgs::srv::BasicTypes>();
    std::memset(&qos, 0, sizeof(qos));
    qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
    qos.depth = 10;
    qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
    qos.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;
  }

  void TearDown() override
  {
    if (cli) { auto _r [[maybe_unused]] = rmw_destroy_client(node, cli); }
    if (srv) { auto _r [[maybe_unused]] = rmw_destroy_service(node, srv); }
    RmwUdsNodeTest::TearDown();
  }
};

TEST_F(ServiceClientTest, CreateDestroyService)
{
  srv = rmw_create_service(node, ts, "/test_srv", &qos);
  ASSERT_NE(nullptr, srv);
  EXPECT_STREQ("/test_srv", srv->service_name);
  EXPECT_EQ(uds_id(), srv->implementation_identifier);
}

TEST_F(ServiceClientTest, CreateDestroyClient)
{
  cli = rmw_create_client(node, ts, "/test_srv", &qos);
  ASSERT_NE(nullptr, cli);
  EXPECT_STREQ("/test_srv", cli->service_name);
  EXPECT_EQ(uds_id(), cli->implementation_identifier);
}

TEST_F(ServiceClientTest, ServiceServerIsAvailable)
{
  cli = rmw_create_client(node, ts, "/avail_srv", &qos);
  ASSERT_NE(nullptr, cli);

  bool available = false;
  EXPECT_EQ(RMW_RET_OK, rmw_service_server_is_available(node, cli, &available));
  EXPECT_FALSE(available);

  srv = rmw_create_service(node, ts, "/avail_srv", &qos);
  ASSERT_NE(nullptr, srv);

  EXPECT_EQ(RMW_RET_OK, rmw_service_server_is_available(node, cli, &available));
  EXPECT_TRUE(available);
}

TEST_F(ServiceClientTest, RequestResponseRoundTrip)
{
  srv = rmw_create_service(node, ts, "/roundtrip_srv", &qos);
  cli = rmw_create_client(node, ts, "/roundtrip_srv", &qos);
  ASSERT_NE(nullptr, srv);
  ASSERT_NE(nullptr, cli);

  // Send request
  test_msgs::srv::BasicTypes::Request request;
  request.bool_value = true;
  request.int32_value = 42;
  request.float64_value = 3.14;

  int64_t seq_id = 0;
  EXPECT_EQ(RMW_RET_OK, rmw_send_request(cli, &request, &seq_id));
  EXPECT_GE(seq_id, 1);

  // Take request on service side
  test_msgs::srv::BasicTypes::Request recv_request;
  rmw_service_info_t request_header;
  std::memset(&request_header, 0, sizeof(request_header));
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take_request(srv, &request_header, &recv_request, &taken));
  ASSERT_TRUE(taken);
  EXPECT_EQ(42, recv_request.int32_value);
  EXPECT_TRUE(recv_request.bool_value);
  EXPECT_DOUBLE_EQ(3.14, recv_request.float64_value);

  // Send response
  test_msgs::srv::BasicTypes::Response response;
  response.bool_value = true;
  response.int32_value = 84;
  response.string_value = "success";
  EXPECT_EQ(RMW_RET_OK, rmw_send_response(srv, &request_header.request_id, &response));

  // Take response on client side
  test_msgs::srv::BasicTypes::Response recv_response;
  rmw_service_info_t response_header;
  std::memset(&response_header, 0, sizeof(response_header));
  taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take_response(cli, &response_header, &recv_response, &taken));
  ASSERT_TRUE(taken);
  EXPECT_EQ(84, recv_response.int32_value);
  EXPECT_TRUE(recv_response.bool_value);
  EXPECT_EQ("success", recv_response.string_value);
}
