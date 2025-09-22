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

#include "lddc.h"

#include <chrono>
#include <inttypes.h>
#include <math.h>
#include <stdint.h>

#include <rclcpp/rclcpp.hpp>
#include <rclcpp/serialization.hpp>
#include <rosbag2_cpp/writer.hpp>
#include <rosbag2_storage/storage_options.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <pcl_conversions/pcl_conversions.h>

#include <livox_ros_driver/msg/custom_msg.hpp>
#include <livox_ros_driver/msg/custom_point.hpp>
#include "lds_lidar.h"
#include "lds_lvx.h"

namespace livox_ros {

rclcpp::Time get_pc_timestamp(std::shared_ptr<rclcpp::Node> node){
  return node->get_clock()->now();
}

/** Lidar Data Distribute Control--------------------------------------------*/
Lddc::Lddc(int format, int multi_topic, int data_src, int output_type,
    double frq, std::string &frame_id, bool lidar_bag, bool imu_bag, bool use_pc_time, std::shared_ptr<rclcpp::Node> node)
    : transfer_format_(format),
      use_multi_topic_(multi_topic),
      data_src_(data_src),
      output_type_(output_type),
      publish_frq_(frq),
      frame_id_(frame_id),
      enable_lidar_bag_(lidar_bag),
      enable_imu_bag_(imu_bag),
      use_pc_time_(use_pc_time),
      cur_node_(node) {
  publish_period_ns_ = kNsPerSecond / publish_frq_;
  lds_ = nullptr;
  // 在构造函数中初始化CustomMsg Publisher
  for (uint32_t i = 0; i < kMaxSourceLidar; i++) {
    private_pub_[i] = nullptr;
    private_custom_pub_[i] = nullptr;
    private_imu_pub_[i] = nullptr;
  }
  global_pub_ = nullptr;
  global_custom_pub_ = nullptr;
  global_imu_pub_ = nullptr;
  bag_ = nullptr;
};

Lddc::~Lddc() {
  if (lds_) {
    lds_->PrepareExit();
  }
  // ROS2的智能指针会自动管理内存，不需要手动delete
}

int32_t Lddc::GetPublishStartTime(LidarDevice *lidar, LidarDataQueue *queue,
                                  uint64_t *start_time,
                                  StoragePacket *storage_packet) {
  QueuePrePop(queue, storage_packet);
  uint64_t timestamp =
      GetStoragePacketTimestamp(storage_packet, lidar->data_src);
  uint32_t remaining_time = timestamp % publish_period_ns_;
  uint32_t diff_time = publish_period_ns_ - remaining_time;
  /** Get start time, down to the period boundary */
  if (diff_time > (publish_period_ns_ / 4)) {
    // ROS_INFO("0 : %u", diff_time);
    *start_time = timestamp - remaining_time;
    return 0;
  } else if (diff_time <= lidar->packet_interval_max) {
    *start_time = timestamp;
    return 0;
  } else {
    /** Skip some packets up to the period boundary*/
    // ROS_INFO("2 : %u", diff_time);
    do {
      if (QueueIsEmpty(queue)) {
        break;
      }
      QueuePopUpdate(queue); /* skip packet */
      QueuePrePop(queue, storage_packet);
      uint32_t last_remaning_time = remaining_time;
      timestamp = GetStoragePacketTimestamp(storage_packet, lidar->data_src);
      remaining_time = timestamp % publish_period_ns_;
      /** Flip to another period */
      if (last_remaning_time > remaining_time) {
        // ROS_INFO("Flip to another period, exit");
        break;
      }
      diff_time = publish_period_ns_ - remaining_time;
    } while (diff_time > lidar->packet_interval);

    /* the remaning packets in queue maybe not enough after skip */
    return -1;
  }
}

void Lddc::InitPointcloud2MsgHeader(sensor_msgs::msg::PointCloud2& cloud) {
  cloud.header.frame_id.assign(frame_id_);
  cloud.height = 1;
  cloud.width = 0;
  cloud.fields.resize(4);
  cloud.fields[0].offset = 0;
  cloud.fields[0].name = "x";
  cloud.fields[0].count = 1;
  cloud.fields[0].datatype = sensor_msgs::msg::PointField::FLOAT32;
  cloud.fields[1].offset = 4;
  cloud.fields[1].name = "y";
  cloud.fields[1].count = 1;
  cloud.fields[1].datatype = sensor_msgs::msg::PointField::FLOAT32;
  cloud.fields[2].offset = 8;
  cloud.fields[2].name = "z";
  cloud.fields[2].count = 1;
  cloud.fields[2].datatype = sensor_msgs::msg::PointField::FLOAT32;
  cloud.fields[3].offset = 12;
  cloud.fields[3].name = "intensity";
  cloud.fields[3].count = 1;
  cloud.fields[3].datatype = sensor_msgs::msg::PointField::FLOAT32;
  cloud.point_step = 16;
}

uint32_t Lddc::PublishPointcloud2(LidarDataQueue *queue, uint32_t packet_num,
                                  uint8_t handle) {
  uint64_t timestamp = 0;
  uint64_t last_timestamp = 0;
  uint32_t published_packet = 0;

  StoragePacket storage_packet;
  LidarDevice *lidar = &lds_->lidars_[handle];
  if (GetPublishStartTime(lidar, queue, &last_timestamp, &storage_packet)) {
    /* the remaning packets in queue maybe not enough after skip */
    return 0;
  }

  sensor_msgs::msg::PointCloud2 cloud;
  InitPointcloud2MsgHeader(cloud);
  cloud.data.resize(packet_num * kMaxPointPerEthPacket *
                    sizeof(LivoxPointXyzrtl));
  cloud.point_step = sizeof(LivoxPointXyzrtl);

  uint8_t *point_base = cloud.data.data();
  uint8_t data_source = lidar->data_src;
  uint32_t line_num = GetLaserLineNumber(lidar->info.type);
  uint32_t echo_num = GetEchoNumPerPoint(lidar->raw_data_type);
  uint32_t is_zero_packet = 0;
  while ((published_packet < packet_num) && !QueueIsEmpty(queue)) {
    QueuePrePop(queue, &storage_packet);
    LivoxEthPacket *raw_packet =
        reinterpret_cast<LivoxEthPacket *>(storage_packet.raw_data);
    timestamp = GetStoragePacketTimestamp(&storage_packet, data_source);
    int64_t packet_gap = timestamp - last_timestamp;
    if ((packet_gap > lidar->packet_interval_max) &&
        lidar->data_is_pubulished) {
      // ROS_INFO("Lidar[%d] packet time interval is %ldns", handle,
      //     packet_gap);
      if (kSourceLvxFile != data_source) {
        timestamp = last_timestamp + lidar->packet_interval;
        ZeroPointDataOfStoragePacket(&storage_packet);
        is_zero_packet = 1;
      }
    }
    /** Use the first packet timestamp as pointcloud2 msg timestamp */
    if (!published_packet) {
      cloud.header.stamp = use_pc_time_ ? get_pc_timestamp(cur_node_) : rclcpp::Time(timestamp);
    }
    uint32_t single_point_num = storage_packet.point_num * echo_num;

    if (kSourceLvxFile != data_source) {
      PointConvertHandler pf_point_convert =
          GetConvertHandler(lidar->raw_data_type);
      if (pf_point_convert) {
        point_base = pf_point_convert(point_base, raw_packet,
            lidar->extrinsic_parameter, line_num);
      } else {
        /** Skip the packet */
        RCLCPP_INFO(cur_node_->get_logger(), "Lidar[%d] unkown packet type[%d]", handle,
                 raw_packet->data_type);
        break;
      }
    } else {
      point_base = LivoxPointToPxyzrtl(point_base, raw_packet,
          lidar->extrinsic_parameter, line_num);
    }

    if (!is_zero_packet) {
      QueuePopUpdate(queue);
    } else {
      is_zero_packet = 0;
    }
    cloud.width += single_point_num;
    ++published_packet;
    last_timestamp = timestamp;
  }
  cloud.row_step     = cloud.width * cloud.point_step;
  cloud.is_bigendian = false;
  cloud.is_dense     = true;
  cloud.data.resize(cloud.row_step); /** Adjust to the real size */
  auto p_publisher = Lddc::GetCurrentPublisher(handle);
  if (kOutputToRos == output_type_) {
    p_publisher->publish(cloud);
  } else {
    if (bag_ && enable_lidar_bag_) {
      rclcpp::Serialization<sensor_msgs::msg::PointCloud2> serialization;
      rclcpp::SerializedMessage serialized_msg;
      serialization.serialize_message(&cloud, &serialized_msg);
      
      rosbag2_storage::TopicMetadata topic_metadata;
      topic_metadata.name = "livox_lidar";
      topic_metadata.type = "sensor_msgs/msg/PointCloud2";
      topic_metadata.serialization_format = "cdr";
      
      bag_->write(std::make_shared<rclcpp::SerializedMessage>(serialized_msg), topic_metadata.name, topic_metadata.type, rclcpp::Time(timestamp));
    }
  }
  if (!lidar->data_is_pubulished) {
    lidar->data_is_pubulished = true;
  }
  return published_packet;
}

void Lddc::FillPointsToPclMsg(PointCloud::Ptr& pcl_msg, \
    LivoxPointXyzrtl* src_point, uint32_t num) {
  LivoxPointXyzrtl* point_xyzrtl = (LivoxPointXyzrtl*)src_point;
  for (uint32_t i = 0; i < num; i++) {
    pcl::PointXYZI point;
    point.x = point_xyzrtl->x;
    point.y = point_xyzrtl->y;
    point.z = point_xyzrtl->z;
    point.intensity = point_xyzrtl->reflectivity;
    ++point_xyzrtl;
    pcl_msg->points.push_back(point);
  }
}

/* for pcl::pxyzi */
uint32_t Lddc::PublishPointcloudData(LidarDataQueue *queue, uint32_t packet_num,
                                     uint8_t handle) {
  uint64_t timestamp = 0;
  uint64_t last_timestamp = 0;
  uint32_t published_packet = 0;

  StoragePacket storage_packet;
  LidarDevice *lidar = &lds_->lidars_[handle];
  if (GetPublishStartTime(lidar, queue, &last_timestamp, &storage_packet)) {
    /* the remaning packets in queue maybe not enough after skip */
    return 0;
  }

  PointCloud::Ptr cloud(new PointCloud);
  cloud->header.frame_id.assign(frame_id_);
  cloud->height = 1;
  cloud->width = 0;

  uint8_t point_buf[2048];
  uint32_t is_zero_packet = 0;
  uint8_t data_source = lidar->data_src;
  uint32_t line_num = GetLaserLineNumber(lidar->info.type);
  uint32_t echo_num = GetEchoNumPerPoint(lidar->raw_data_type);
  while ((published_packet < packet_num) && !QueueIsEmpty(queue)) {
    QueuePrePop(queue, &storage_packet);
    LivoxEthPacket *raw_packet =
        reinterpret_cast<LivoxEthPacket *>(storage_packet.raw_data);
    timestamp = GetStoragePacketTimestamp(&storage_packet, data_source);
    int64_t packet_gap = timestamp - last_timestamp;
    if ((packet_gap > lidar->packet_interval_max) &&
        lidar->data_is_pubulished) {
      //ROS_INFO("Lidar[%d] packet time interval is %ldns", handle, packet_gap);
      if (kSourceLvxFile != data_source) {
        timestamp = last_timestamp + lidar->packet_interval;
        ZeroPointDataOfStoragePacket(&storage_packet);
        is_zero_packet = 1;
      }
    }
    if (!published_packet) {
      cloud->header.stamp = use_pc_time_ ? get_pc_timestamp(cur_node_).nanoseconds()/1000.0 : timestamp / 1000.0;  // to pcl ros time stamp
    }
    uint32_t single_point_num = storage_packet.point_num * echo_num;

    if (kSourceLvxFile != data_source) {
      PointConvertHandler pf_point_convert =
          GetConvertHandler(lidar->raw_data_type);
      if (pf_point_convert) {
        pf_point_convert(point_buf, raw_packet, lidar->extrinsic_parameter, \
            line_num);
      } else {
        RCLCPP_INFO(cur_node_->get_logger(), "Lidar[%d] unkown packet type[%d]", handle,
                 raw_packet->data_type);
        break;
      }
    } else {
      LivoxPointToPxyzrtl(point_buf, raw_packet, lidar->extrinsic_parameter, \
          line_num);
    }
    LivoxPointXyzrtl *dst_point = (LivoxPointXyzrtl *)point_buf;
    FillPointsToPclMsg(cloud, dst_point, single_point_num);
    if (!is_zero_packet) {
      QueuePopUpdate(queue);
    } else {
      is_zero_packet = 0;
    }
    cloud->width += single_point_num;
    ++published_packet;
    last_timestamp = timestamp;
  }

  auto p_publisher = Lddc::GetCurrentPublisher(handle);
  if (kOutputToRos == output_type_) {
    sensor_msgs::msg::PointCloud2 cloud_msg;
    pcl::toROSMsg(*cloud, cloud_msg);
    p_publisher->publish(cloud_msg);
  } else {
    if (bag_ && enable_lidar_bag_) {
      sensor_msgs::msg::PointCloud2 cloud_msg;
      pcl::toROSMsg(*cloud, cloud_msg);
      rclcpp::Serialization<sensor_msgs::msg::PointCloud2> serialization;
      rclcpp::SerializedMessage serialized_msg;
      serialization.serialize_message(&cloud_msg, &serialized_msg);
      bag_->write(std::make_shared<rclcpp::SerializedMessage>(serialized_msg), "livox_lidar", "sensor_msgs/msg/PointCloud2",
                  rclcpp::Time(timestamp));
    }
  }
  if (!lidar->data_is_pubulished) {
    lidar->data_is_pubulished = true;
  }
  return published_packet;
}

void Lddc::FillPointsToCustomMsg(livox_ros_driver::msg::CustomMsg& livox_msg, \
    LivoxPointXyzrtl* src_point, uint32_t num, uint32_t offset_time, \
    uint32_t point_interval, uint32_t echo_num) {
  LivoxPointXyzrtl* point_xyzrtl = (LivoxPointXyzrtl*)src_point;
  for (uint32_t i = 0; i < num; i++) {
    livox_ros_driver::msg::CustomPoint point;
    if (echo_num > 1) { /** dual return mode */
      point.offset_time = offset_time + (i / echo_num) * point_interval;
    } else {
      point.offset_time = offset_time + i * point_interval;
    }
    point.x = point_xyzrtl->x;
    point.y = point_xyzrtl->y;
    point.z = point_xyzrtl->z;
    point.reflectivity = point_xyzrtl->reflectivity;
    point.tag = point_xyzrtl->tag;
    point.line = point_xyzrtl->line;
    ++point_xyzrtl;
    livox_msg.points.push_back(point);
  }
}

uint32_t Lddc::PublishCustomPointcloud(LidarDataQueue *queue,
                                       uint32_t packet_num, uint8_t handle) {
  // static uint32_t msg_seq = 0;
  uint64_t timestamp = 0;
  uint64_t last_timestamp = 0;

  StoragePacket storage_packet;
  LidarDevice *lidar = &lds_->lidars_[handle];
  if (GetPublishStartTime(lidar, queue, &last_timestamp, &storage_packet)) {
    /* the remaning packets in queue maybe not enough after skip */
    return 0;
  }

  livox_ros_driver::msg::CustomMsg livox_msg;
  livox_msg.header.frame_id.assign(frame_id_);
  livox_msg.header.stamp = use_pc_time_ ? get_pc_timestamp(cur_node_) : rclcpp::Time(last_timestamp);
  livox_msg.timebase = 0;
  livox_msg.point_num = 0;
  livox_msg.lidar_id = handle;

  uint8_t point_buf[2048];
  uint8_t data_source = lds_->lidars_[handle].data_src;
  uint32_t line_num = GetLaserLineNumber(lidar->info.type);
  uint32_t echo_num = GetEchoNumPerPoint(lidar->raw_data_type);
  uint32_t point_interval = GetPointInterval(lidar->info.type);
  uint32_t published_packet = 0;
  uint32_t packet_offset_time = 0;  /** uint:ns */
  uint32_t is_zero_packet = 0;
  while (published_packet < packet_num) {
    QueuePrePop(queue, &storage_packet);
    LivoxEthPacket *raw_packet =
        reinterpret_cast<LivoxEthPacket *>(storage_packet.raw_data);
    timestamp = GetStoragePacketTimestamp(&storage_packet, data_source);
    int64_t packet_gap = timestamp - last_timestamp;
    if ((packet_gap > lidar->packet_interval_max) &&
        lidar->data_is_pubulished) {
      // RCLCPP_INFO(cur_node_->get_logger(), "Lidar[%d] packet time interval is %ldns", handle, packet_gap);
      if (kSourceLvxFile != data_source) {
        timestamp = last_timestamp + lidar->packet_interval;
        ZeroPointDataOfStoragePacket(&storage_packet);
        is_zero_packet = 1;
      }
    }
    /** first packet */
    if (!published_packet) {
      livox_msg.timebase = timestamp;
      packet_offset_time = 0;
      /** convert to ros time stamp */
      livox_msg.header.stamp = use_pc_time_ ? get_pc_timestamp(cur_node_) : rclcpp::Time(timestamp);
    } else {
      packet_offset_time = (uint32_t)(timestamp - livox_msg.timebase);
    }
    uint32_t single_point_num = storage_packet.point_num * echo_num;

    if (kSourceLvxFile != data_source) {
      PointConvertHandler pf_point_convert =
          GetConvertHandler(lidar->raw_data_type);
      if (pf_point_convert) {
        pf_point_convert(point_buf, raw_packet, lidar->extrinsic_parameter, \
            line_num);
      } else {
        /* Skip the packet */
        RCLCPP_INFO(cur_node_->get_logger(), "Lidar[%d] unkown packet type[%d]", handle,
                    raw_packet->data_type);
        break;
      }
    } else {
      LivoxPointToPxyzrtl(point_buf, raw_packet, lidar->extrinsic_parameter, \
          line_num);
    }
    LivoxPointXyzrtl *dst_point = (LivoxPointXyzrtl *)point_buf;
    FillPointsToCustomMsg(livox_msg, dst_point, single_point_num, \
        packet_offset_time, point_interval, echo_num);

    if (!is_zero_packet) {
      QueuePopUpdate(queue);
    } else {
      is_zero_packet = 0;
    }

    livox_msg.point_num += single_point_num;
    last_timestamp = timestamp;
    ++published_packet;
  }

  auto p_publisher = Lddc::GetCurrentCustomPublisher(handle);
  if (kOutputToRos == output_type_) {
    p_publisher->publish(livox_msg);
  } else {
    if (bag_ && enable_lidar_bag_) {
      rclcpp::Serialization<livox_ros_driver::msg::CustomMsg> serialization;
      rclcpp::SerializedMessage serialized_msg;
      serialization.serialize_message(&livox_msg, &serialized_msg);
      
      rosbag2_storage::TopicMetadata topic_metadata;
      topic_metadata.name = p_publisher->get_topic_name();
      topic_metadata.type = "livox_ros_driver/msg/CustomMsg";
      topic_metadata.serialization_format = "cdr";
      
      bag_->write(std::make_shared<rclcpp::SerializedMessage>(serialized_msg),
                  topic_metadata.name, topic_metadata.type, rclcpp::Time(timestamp));
    }
  }

  if (!lidar->data_is_pubulished) {
    lidar->data_is_pubulished = true;
  }
  return published_packet;
}

// 修复PublishImuData函数
uint32_t Lddc::PublishImuData(LidarDataQueue *queue, uint32_t packet_num,
                              uint8_t handle) {
  uint64_t timestamp = 0;
  uint32_t published_packet = 0;

  sensor_msgs::msg::Imu imu_data;
  imu_data.header.frame_id = "livox_frame";

  uint8_t data_source = lds_->lidars_[handle].data_src;
  StoragePacket storage_packet;
  QueuePrePop(queue, &storage_packet);
  LivoxEthPacket *raw_packet =
      reinterpret_cast<LivoxEthPacket *>(storage_packet.raw_data);
  timestamp = GetStoragePacketTimestamp(&storage_packet, data_source);
  if (timestamp > 0) {  // 修复无符号比较警告
    imu_data.header.stamp = use_pc_time_ ? get_pc_timestamp(cur_node_) : rclcpp::Time(timestamp);
  }

  uint8_t point_buf[2048];
  LivoxImuDataProcess(point_buf, raw_packet);

  LivoxImuPoint *imu = (LivoxImuPoint *)point_buf;
  imu_data.angular_velocity.x = imu->gyro_x;
  imu_data.angular_velocity.y = imu->gyro_y;
  imu_data.angular_velocity.z = imu->gyro_z;
  imu_data.linear_acceleration.x = imu->acc_x;
  imu_data.linear_acceleration.y = imu->acc_y;
  imu_data.linear_acceleration.z = imu->acc_z;

  QueuePopUpdate(queue);
  ++published_packet;

  auto p_publisher = Lddc::GetCurrentImuPublisher(handle);
  if (kOutputToRos == output_type_) {
    p_publisher->publish(imu_data);
  } else {
    if (bag_ && enable_imu_bag_) {
      rclcpp::Serialization<sensor_msgs::msg::Imu> serialization;
      rclcpp::SerializedMessage serialized_msg;
      serialization.serialize_message(&imu_data, &serialized_msg);
      
      rosbag2_storage::TopicMetadata topic_metadata;
      topic_metadata.name = p_publisher->get_topic_name();
      topic_metadata.type = "sensor_msgs/msg/Imu";
      topic_metadata.serialization_format = "cdr";
      
      bag_->write(std::make_shared<rclcpp::SerializedMessage>(serialized_msg),
                  topic_metadata.name, topic_metadata.type, rclcpp::Time(timestamp));
    }
  }
  return published_packet;
}

int Lddc::RegisterLds(Lds *lds) {
  if (lds_ == nullptr) {
    lds_ = lds;
    return 0;
  } else {
    return -1;
  }
}

void Lddc::PollingLidarPointCloudData(uint8_t handle, LidarDevice *lidar) {
  LidarDataQueue *p_queue = &lidar->data;
  if (p_queue->storage_packet == nullptr) {
    return;
  }

  while (!QueueIsEmpty(p_queue)) {
    uint32_t used_size = QueueUsedSize(p_queue);
    uint32_t onetime_publish_packets = lidar->onetime_publish_packets;
    if (used_size < onetime_publish_packets) {
      break;
    }

    if (kPointCloud2Msg == transfer_format_) {
      PublishPointcloud2(p_queue, onetime_publish_packets, handle);
    } else if (kLivoxCustomMsg == transfer_format_) {
      PublishCustomPointcloud(p_queue, onetime_publish_packets, handle);
    } else if (kPclPxyziMsg == transfer_format_) {
      PublishPointcloudData(p_queue, onetime_publish_packets, handle);
    }
  }
}

void Lddc::PollingLidarImuData(uint8_t handle, LidarDevice *lidar) {
  LidarDataQueue *p_queue = &lidar->imu_data;
  if (p_queue->storage_packet == nullptr) {
    return;
  }

  while (!QueueIsEmpty(p_queue)) {
    PublishImuData(p_queue, 1, handle);
  }
}

void Lddc::DistributeLidarData(void) {
  if (lds_ == nullptr) {
    return;
  }
  lds_->semaphore_.Wait();
  for (uint32_t i = 0; i < lds_->lidar_count_; i++) {
    uint32_t lidar_id = i;
    LidarDevice *lidar = &lds_->lidars_[lidar_id];
    LidarDataQueue *p_queue = &lidar->data;
    if ((kConnectStateSampling != lidar->connect_state) ||
        (p_queue == nullptr)) {
      continue;
    }
    PollingLidarPointCloudData(lidar_id, lidar);
    PollingLidarImuData(lidar_id, lidar);
  }

  if (lds_->IsRequestExit()) {
    PrepareExit();
  }
}

rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr Lddc::GetCurrentPublisher(uint8_t handle) {
  rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr *pub = nullptr;
  uint32_t queue_size = kMinEthPacketQueueSize;

  if (use_multi_topic_) {
    pub = &private_pub_[handle];
    queue_size = queue_size * 2;
  } else {
    pub = &global_pub_;
    queue_size = queue_size * 8;
  }

  if (*pub == nullptr) {
    char name_str[48];
    memset(name_str, 0, sizeof(name_str));
    if (use_multi_topic_) {
      snprintf(name_str, sizeof(name_str), "livox/lidar_%s",
               lds_->lidars_[handle].info.broadcast_code);
      RCLCPP_INFO(cur_node_->get_logger(), "Support multi topics.");
    } else {
      RCLCPP_INFO(cur_node_->get_logger(), "Support only one topic.");
      snprintf(name_str, sizeof(name_str), "livox/lidar");
    }

    rclcpp::QoS qos(queue_size);
    if (kPointCloud2Msg == transfer_format_) {
      *pub = cur_node_->create_publisher<sensor_msgs::msg::PointCloud2>(name_str, qos);
      RCLCPP_INFO(cur_node_->get_logger(),
          "%s publish use PointCloud2 format, set ROS publisher queue size %d",
          name_str, queue_size);
    } else if (kPclPxyziMsg == transfer_format_) {
      *pub = cur_node_->create_publisher<sensor_msgs::msg::PointCloud2>(name_str, qos);
      RCLCPP_INFO(cur_node_->get_logger(),
          "%s publish use pcl PointXYZI format, set ROS publisher queue size %d",
          name_str, queue_size);
    }
  }

  return *pub;
}

rclcpp::Publisher<sensor_msgs::msg::Imu>::SharedPtr Lddc::GetCurrentImuPublisher(uint8_t handle) {
  rclcpp::Publisher<sensor_msgs::msg::Imu>::SharedPtr *pub = nullptr;
  uint32_t queue_size = kMinEthPacketQueueSize;

  if (use_multi_topic_) {
    pub = &private_imu_pub_[handle];
    queue_size = queue_size * 2;
  } else {
    pub = &global_imu_pub_;
    queue_size = queue_size * 8;
  }

  if (*pub == nullptr) {
    char name_str[48];
    memset(name_str, 0, sizeof(name_str));
    if (use_multi_topic_) {
      RCLCPP_INFO(cur_node_->get_logger(), "Support multi topics.");
      snprintf(name_str, sizeof(name_str), "livox/imu_%s",
               lds_->lidars_[handle].info.broadcast_code);
    } else {
      RCLCPP_INFO(cur_node_->get_logger(), "Support only one topic.");
      snprintf(name_str, sizeof(name_str), "livox/imu");
    }

    rclcpp::QoS qos(queue_size);
    *pub = cur_node_->create_publisher<sensor_msgs::msg::Imu>(name_str, qos);
    RCLCPP_INFO(cur_node_->get_logger(), "%s publish imu data, set ROS publisher queue size %d", name_str, queue_size);
  }

  return *pub;
}

rclcpp::Publisher<livox_ros_driver::msg::CustomMsg>::SharedPtr Lddc::GetCurrentCustomPublisher(uint8_t handle) {
  rclcpp::Publisher<livox_ros_driver::msg::CustomMsg>::SharedPtr *pub = nullptr;
  uint32_t queue_size = kMinEthPacketQueueSize;

  if (use_multi_topic_) {
    pub = &private_custom_pub_[handle];
    queue_size = queue_size * 2;
  } else {
    pub = &global_custom_pub_;
    queue_size = queue_size * 8;
  }

  if (*pub == nullptr) {
    char name_str[48];
    memset(name_str, 0, sizeof(name_str));
    if (use_multi_topic_) {
      snprintf(name_str, sizeof(name_str), "livox/lidar_%s",
               lds_->lidars_[handle].info.broadcast_code);
      RCLCPP_INFO(cur_node_->get_logger(), "Support multi topics.");
    } else {
      RCLCPP_INFO(cur_node_->get_logger(), "Support only one topic.");
      snprintf(name_str, sizeof(name_str), "livox/lidar");
    }

    rclcpp::QoS qos(queue_size);
    *pub = cur_node_->create_publisher<livox_ros_driver::msg::CustomMsg>(name_str, qos);
    RCLCPP_INFO(cur_node_->get_logger(),
        "%s publish use CustomMsg format, set ROS publisher queue size %d",
        name_str, queue_size);
  }

  return *pub;
}

void Lddc::CreateBagFile(const std::string &file_name) {
  if (!bag_) {
    bag_ = std::make_unique<rosbag2_cpp::Writer>();
    rosbag2_storage::StorageOptions storage_options;
    storage_options.uri = file_name;
    storage_options.storage_id = "sqlite3";
    bag_->open(storage_options);
    RCLCPP_INFO(cur_node_->get_logger(), "Create bag file :%s!", file_name.c_str());
  }
}

void Lddc::PrepareExit(void) {
  if (bag_) {
    RCLCPP_INFO(cur_node_->get_logger(), "Waiting to save the bag file!");
    bag_->close();
    RCLCPP_INFO(cur_node_->get_logger(), "Save the bag file successfully!");
    bag_ = nullptr;
  }
  if (lds_) {
    lds_->PrepareExit();
    lds_ = nullptr;
  }
}

}  // namespace livox_ros
