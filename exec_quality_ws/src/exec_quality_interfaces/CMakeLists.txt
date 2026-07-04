cmake_minimum_required(VERSION 3.8)
project(exec_quality_interfaces)

find_package(ament_cmake REQUIRED)
find_package(rosidl_default_generators REQUIRED)

rosidl_generate_interfaces(${PROJECT_NAME}
  "msg/OrderMsg.msg"
  "msg/FillMsg.msg"
  "msg/MetricMsg.msg"
  "msg/VenueReport.msg"
)

ament_package()
