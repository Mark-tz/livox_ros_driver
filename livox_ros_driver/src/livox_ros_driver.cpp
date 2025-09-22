//
// The MIT License (MIT)
//
// Copyright (c) 2019 Livox. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

#include "include/livox_ros_driver.h"

#include <chrono>
#include <vector>
#include <csignal>
#include <memory>

#include <rclcpp/rclcpp.hpp>
#include "lddc.h"
#include "lds_hub.h"
#include "lds_lidar.h"
#include "lds_lvx.h"
#include "livox_sdk.h"

using namespace livox_ros;

const int32_t kSdkVersionMajorLimit = 2;

// Global node pointer for signal handler
std::shared_ptr<rclcpp::Node> g_node = nullptr;

inline void SignalHandler(int signum) {
  printf("livox ros driver will exit\r\n");
  if (g_node) {
    rclcpp::shutdown();
  }
  exit(signum);
}

int main(int argc, char **argv) {
  /** ROS2 initialization */
  rclcpp::init(argc, argv);
  
  // Create node
  auto livox_node = rclcpp::Node::make_shared("livox_lidar_publisher");
  g_node = livox_node;

  RCLCPP_INFO(livox_node->get_logger(), "Livox Ros Driver Version: %s", LIVOX_ROS_DRIVER_VERSION_STRING);
  signal(SIGINT, SignalHandler);
  
  /** Check sdk version */
  LivoxSdkVersion _sdkversion;
  GetLivoxSdkVersion(&_sdkversion);
  if (_sdkversion.major < kSdkVersionMajorLimit) {
    RCLCPP_INFO(livox_node->get_logger(), "The SDK version[%d.%d.%d] is too low", _sdkversion.major,
             _sdkversion.minor, _sdkversion.patch);
    return 0;
  }

  /** Init default system parameter */
  int xfer_format = kPointCloud2Msg;
  int multi_topic = 0;
  int data_src = kSourceRawLidar;
  double publish_freq  = 10.0; /* Hz */
  int output_type      = kOutputToRos;
  std::string frame_id = "livox_frame";
  bool lidar_bag = true;
  bool imu_bag   = false;
  bool use_pc_time = false;

  // Declare and get parameters
  livox_node->declare_parameter("xfer_format", xfer_format);
  livox_node->declare_parameter("multi_topic", multi_topic);
  livox_node->declare_parameter("data_src", data_src);
  livox_node->declare_parameter("publish_freq", publish_freq);
  livox_node->declare_parameter("output_data_type", output_type);
  livox_node->declare_parameter("frame_id", frame_id);
  livox_node->declare_parameter("enable_lidar_bag", lidar_bag);
  livox_node->declare_parameter("enable_imu_bag", imu_bag);
  livox_node->declare_parameter("use_pc_time", use_pc_time);
  
  livox_node->get_parameter("xfer_format", xfer_format);
  livox_node->get_parameter("multi_topic", multi_topic);
  livox_node->get_parameter("data_src", data_src);
  livox_node->get_parameter("publish_freq", publish_freq);
  livox_node->get_parameter("output_data_type", output_type);
  livox_node->get_parameter("frame_id", frame_id);
  livox_node->get_parameter("enable_lidar_bag", lidar_bag);
  livox_node->get_parameter("enable_imu_bag", imu_bag);
  livox_node->get_parameter("use_pc_time", use_pc_time);
  
  if (publish_freq > 100.0) {
    publish_freq = 100.0;
  } else if (publish_freq < 0.1) {
    publish_freq = 0.1;
  } else {
    publish_freq = publish_freq;
  }

  /** Lidar data distribute control and lidar data source set */
  Lddc *lddc = new Lddc(xfer_format, multi_topic, data_src, output_type,
                        publish_freq, frame_id, lidar_bag, imu_bag, use_pc_time, livox_node);

  int ret = 0;
  if (data_src == kSourceRawLidar) {
    RCLCPP_INFO(livox_node->get_logger(), "Data Source is raw lidar.");

    std::string user_config_path;
    livox_node->declare_parameter("user_config_path", user_config_path);
    livox_node->get_parameter("user_config_path", user_config_path);
    RCLCPP_INFO(livox_node->get_logger(), "Config file : %s", user_config_path.c_str());

    std::string cmdline_bd_code;
    livox_node->declare_parameter("cmdline_str", cmdline_bd_code);
    livox_node->get_parameter("cmdline_str", cmdline_bd_code);

    std::vector<std::string> bd_code_list;
    ParseCommandlineInputBdCode(cmdline_bd_code.c_str(), bd_code_list);

    LdsLidar *read_lidar = LdsLidar::GetInstance(1000 / publish_freq);
    lddc->RegisterLds(static_cast<Lds *>(read_lidar));
    ret = read_lidar->InitLdsLidar(bd_code_list, user_config_path.c_str());
    if (!ret) {
      RCLCPP_INFO(livox_node->get_logger(), "Init lds lidar success!");
    } else {
      RCLCPP_ERROR(livox_node->get_logger(), "Init lds lidar fail!");
    }
  } else if (data_src == kSourceRawHub) {
    RCLCPP_INFO(livox_node->get_logger(), "Data Source is hub.");

    std::string user_config_path;
    livox_node->declare_parameter("user_config_path", user_config_path);
    livox_node->get_parameter("user_config_path", user_config_path);
    RCLCPP_INFO(livox_node->get_logger(), "Config file : %s", user_config_path.c_str());

    std::string cmdline_bd_code;
    livox_node->declare_parameter("cmdline_str", cmdline_bd_code);
    livox_node->get_parameter("cmdline_str", cmdline_bd_code);

    std::vector<std::string> bd_code_list;
    ParseCommandlineInputBdCode(cmdline_bd_code.c_str(), bd_code_list);

    LdsHub *read_hub = LdsHub::GetInstance(1000 / publish_freq);
    lddc->RegisterLds(static_cast<Lds *>(read_hub));
    ret = read_hub->InitLdsHub(bd_code_list, user_config_path.c_str());
    if (!ret) {
      RCLCPP_INFO(livox_node->get_logger(), "Init lds hub success!");
    } else {
      RCLCPP_ERROR(livox_node->get_logger(), "Init lds hub fail!");
    }
  } else {
    RCLCPP_INFO(livox_node->get_logger(), "Data Source is lvx file.");

    std::string cmdline_file_path;
    livox_node->declare_parameter("cmdline_file_path", cmdline_file_path);
    livox_node->get_parameter("cmdline_file_path", cmdline_file_path);

    do {
      if (!IsFilePathValid(cmdline_file_path.c_str())) {
        RCLCPP_ERROR(livox_node->get_logger(), "File path invalid : %s !", cmdline_file_path.c_str());
        break;
      }

      std::string rosbag_file_path;
      int path_end_pos = cmdline_file_path.find_last_of('.');
      rosbag_file_path = cmdline_file_path.substr(0, path_end_pos);
      rosbag_file_path += ".bag";

      LdsLvx *read_lvx = LdsLvx::GetInstance(1000 / publish_freq);
      lddc->RegisterLds(static_cast<Lds *>(read_lvx));
      lddc->CreateBagFile(rosbag_file_path);
      int ret = read_lvx->InitLdsLvx(cmdline_file_path.c_str());
      if (!ret) {
        RCLCPP_INFO(livox_node->get_logger(), "Init lds lvx file success!");
      } else {
        RCLCPP_ERROR(livox_node->get_logger(), "Init lds lvx file fail!");
      }
    } while (0);
  }

  // Main loop using ROS2 spin
  while (rclcpp::ok()) {
    lddc->DistributeLidarData();
    rclcpp::spin_some(livox_node);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  rclcpp::shutdown();
  return 0;
}
