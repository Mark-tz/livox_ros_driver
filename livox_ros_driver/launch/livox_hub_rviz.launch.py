from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, GroupAction
from launch.conditions import IfCondition
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare
from ament_index_python.packages import get_package_share_directory
import os

def generate_launch_description():
    # Declare launch arguments
    lvx_file_path_arg = DeclareLaunchArgument(
        'lvx_file_path',
        default_value='livox_test.lvx',
        description='Path to LVX file'
    )
    
    bd_list_arg = DeclareLaunchArgument(
        'bd_list',
        default_value='100000000000000',
        description='Broadcast code list'
    )
    
    xfer_format_arg = DeclareLaunchArgument(
        'xfer_format',
        default_value='0',
        description='Transfer format'
    )
    
    multi_topic_arg = DeclareLaunchArgument(
        'multi_topic',
        default_value='0',
        description='Multi topic mode'
    )
    
    data_src_arg = DeclareLaunchArgument(
        'data_src',
        default_value='1',
        description='Data source'
    )
    
    publish_freq_arg = DeclareLaunchArgument(
        'publish_freq',
        default_value='10.0',
        description='Publish frequency'
    )
    
    output_type_arg = DeclareLaunchArgument(
        'output_type',
        default_value='0',
        description='Output data type'
    )
    
    rviz_enable_arg = DeclareLaunchArgument(
        'rviz_enable',
        default_value='true',
        description='Enable RViz'
    )
    
    rosbag_enable_arg = DeclareLaunchArgument(
        'rosbag_enable',
        default_value='false',
        description='Enable rosbag recording'
    )
    
    msg_frame_id_arg = DeclareLaunchArgument(
        'msg_frame_id',
        default_value='livox_frame',
        description='Message frame ID'
    )
    
    lidar_bag_arg = DeclareLaunchArgument(
        'lidar_bag',
        default_value='true',
        description='Enable lidar bag'
    )
    
    imu_bag_arg = DeclareLaunchArgument(
        'imu_bag',
        default_value='true',
        description='Enable IMU bag'
    )
    
    # Get package share directory
    pkg_share = FindPackageShare('livox_ros_driver')
    
    # Livox lidar publisher node
    livox_node = Node(
        package='livox_ros_driver',
        executable='livox_ros_driver_node',
        name='livox_lidar_publisher',
        output='screen',
        parameters=[{
            'xfer_format': LaunchConfiguration('xfer_format'),
            'multi_topic': LaunchConfiguration('multi_topic'),
            'data_src': LaunchConfiguration('data_src'),
            'publish_freq': LaunchConfiguration('publish_freq'),
            'output_data_type': LaunchConfiguration('output_type'),
            'cmdline_str': str(LaunchConfiguration('bd_list')),
            'cmdline_file_path': LaunchConfiguration('lvx_file_path'),
            'user_config_path': PathJoinSubstitution([pkg_share, 'config', 'livox_hub_config.json']),
            'frame_id': LaunchConfiguration('msg_frame_id'),
            'enable_lidar_bag': LaunchConfiguration('lidar_bag'),
            'enable_imu_bag': LaunchConfiguration('imu_bag'),
        }],
        arguments=[LaunchConfiguration('bd_list')]
    )
    
    # RViz node (conditional)
    rviz_node = Node(
        package='rviz2',
        executable='rviz2',
        name='rviz',
        arguments=['-d', PathJoinSubstitution([pkg_share, 'config', 'display_hub_points_ros2.rviz'])],
        condition=IfCondition(LaunchConfiguration('rviz_enable'))
    )
    
    # Rosbag record node (conditional)
    rosbag_node = Node(
        package='ros2bag',
        executable='record',
        name='record',
        arguments=['-a'],
        output='screen',
        condition=IfCondition(LaunchConfiguration('rosbag_enable'))
    )
    
    return LaunchDescription([
        lvx_file_path_arg,
        bd_list_arg,
        xfer_format_arg,
        multi_topic_arg,
        data_src_arg,
        publish_freq_arg,
        output_type_arg,
        rviz_enable_arg,
        rosbag_enable_arg,
        msg_frame_id_arg,
        lidar_bag_arg,
        imu_bag_arg,
        livox_node,
        rviz_node,
        rosbag_node,
    ])