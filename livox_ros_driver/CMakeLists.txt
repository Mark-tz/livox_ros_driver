cmake_minimum_required(VERSION 3.8)
project(livox_ros_driver)

# Default to C++17
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Find dependencies
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(std_msgs REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(geometry_msgs REQUIRED)
find_package(pcl_ros REQUIRED)
find_package(pcl_conversions REQUIRED)
find_package(rosbag2_cpp REQUIRED)
find_package(rosbag2_storage REQUIRED)
find_package(tf2 REQUIRED)
find_package(tf2_ros REQUIRED)
find_package(rosidl_default_generators REQUIRED)
find_package(Boost REQUIRED COMPONENTS system thread chrono)
find_package(PCL REQUIRED)
find_package(PkgConfig REQUIRED)

# Check for APR
pkg_check_modules(APR apr-1)
if(APR_FOUND)
  message(STATUS "APR found: ${APR_INCLUDE_DIRS}")
  message(STATUS "APR libraries: ${APR_LIBRARIES}")
endif()

# Generate interfaces
rosidl_generate_interfaces(${PROJECT_NAME}
  "msg/CustomPoint.msg"
  "msg/CustomMsg.msg"
  DEPENDENCIES std_msgs
)

# Add this line to ensure proper interface generation
rosidl_get_typesupport_target(cpp_typesupport_target ${PROJECT_NAME} "rosidl_typesupport_cpp")

# Build livox_sdk as subdirectory with relaxed warnings
set(LIVOX_SDK_DIR "${CMAKE_CURRENT_SOURCE_DIR}/livox_sdk")

# Create a separate scope for livox_sdk compilation
function(build_livox_sdk)
  # Save current flags
  set(ORIGINAL_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  
  # Set relaxed compilation flags for livox_sdk
  set(CMAKE_CXX_FLAGS "-std=c++17 -O2")
  
  # Add livox_sdk
  add_subdirectory(livox_sdk livox_sdk_build)
  
  # Restore flags (this won't affect the parent scope)
  set(CMAKE_CXX_FLAGS "${ORIGINAL_CXX_FLAGS}")
endfunction()

build_livox_sdk()

# Include directories
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/common
  ${CMAKE_CURRENT_SOURCE_DIR}/common/rapidjson
  ${CMAKE_CURRENT_SOURCE_DIR}/common/rapidxml
  ${CMAKE_CURRENT_SOURCE_DIR}/common/comm
  ${CMAKE_CURRENT_SOURCE_DIR}/timesync
  ${CMAKE_CURRENT_SOURCE_DIR}/timesync/user_uart
  ${CMAKE_CURRENT_SOURCE_DIR}/livox_sdk/sdk_core/include
  ${PCL_INCLUDE_DIRS}
  ${APR_INCLUDE_DIRS}
)

# Link directories
link_directories(
  ${PCL_LIBRARY_DIRS}
  ${APR_LIBRARY_DIRS}
)

# Add compile definitions
add_definitions(${PCL_DEFINITIONS})

# Create executable
add_executable(${PROJECT_NAME}_node
  src/lvx_file.cpp
  src/ldq.cpp
  src/lds.cpp
  src/lds_lvx.cpp
  src/lds_lidar.cpp
  src/lds_hub.cpp
  src/lddc.cpp
  src/livox_ros_driver.cpp
  timesync/timesync.cpp
  timesync/user_uart/user_uart.cpp
  common/comm/comm_protocol.cpp
  common/comm/sdk_protocol.cpp
  common/comm/gps_protocol.cpp
)

# Target dependencies
ament_target_dependencies(${PROJECT_NAME}_node
  rclcpp
  std_msgs
  sensor_msgs
  geometry_msgs
  pcl_ros
  pcl_conversions
  rosbag2_cpp
  rosbag2_storage
  tf2
  tf2_ros
)

# Link libraries
target_link_libraries(${PROJECT_NAME}_node
  livox_sdk_static
  ${Boost_LIBRARIES}
  ${PCL_LIBRARIES}
  ${APR_LIBRARIES}
)

# Ensure message generation happens before building the executable
rosidl_get_typesupport_target(cpp_typesupport_target ${PROJECT_NAME} "rosidl_typesupport_cpp")
target_link_libraries(${PROJECT_NAME}_node "${cpp_typesupport_target}")

# Install targets
install(TARGETS ${PROJECT_NAME}_node
  DESTINATION lib/${PROJECT_NAME}
)

# Install directories
install(DIRECTORY
  launch
  config
  DESTINATION share/${PROJECT_NAME}/
)

# Install package.xml
install(FILES package.xml
  DESTINATION share/${PROJECT_NAME}
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()
endif()

# Export dependencies and targets for message interfaces
ament_export_dependencies(rosidl_default_runtime)
ament_package()
