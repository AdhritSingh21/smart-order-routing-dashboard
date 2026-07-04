"""Launch the full Execution Quality Analyzer pipeline.

    ros2 launch exec_quality_analyzer analyzer.launch.py
    ros2 launch exec_quality_analyzer analyzer.launch.py \
        backend_ingest_url:=http://127.0.0.1:8000/ingest

Brings up:
  - order_submitter           (1)
  - execution_listener        (3 — one per venue, configured via venues.yaml)
  - metrics_engine            (1)
  - aggregator                (1)
  - reporter                  (1)
  - dashboard_bridge          (1 — forwards reports/metrics to the dashboard)
"""
import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    config = os.path.join(
        get_package_share_directory('exec_quality_analyzer'),
        'config',
        'venues.yaml',
    )

    return LaunchDescription([
        DeclareLaunchArgument(
            'backend_ingest_url',
            default_value='http://127.0.0.1:8000/ingest',
            description='FastAPI dashboard endpoint the bridge POSTs to',
        ),
        Node(
            package='exec_quality_analyzer',
            executable='order_submitter',
            name='order_submitter',
            parameters=[config],
            output='screen',
        ),
        Node(
            package='exec_quality_analyzer',
            executable='execution_listener',
            name='alpaca_listener',
            parameters=[config],
            output='screen',
        ),
        Node(
            package='exec_quality_analyzer',
            executable='execution_listener',
            name='binance_listener',
            parameters=[config],
            output='screen',
        ),
        Node(
            package='exec_quality_analyzer',
            executable='execution_listener',
            name='coinbase_listener',
            parameters=[config],
            output='screen',
        ),
        Node(
            package='exec_quality_analyzer',
            executable='metrics_engine',
            name='metrics_engine',
            parameters=[config],
            output='screen',
        ),
        Node(
            package='exec_quality_analyzer',
            executable='aggregator',
            name='aggregator',
            output='screen',
        ),
        Node(
            package='exec_quality_analyzer',
            executable='reporter',
            name='reporter',
            output='screen',
        ),
        Node(
            package='exec_quality_analyzer',
            executable='dashboard_bridge',
            name='dashboard_bridge',
            parameters=[{
                'backend_ingest_url': LaunchConfiguration('backend_ingest_url'),
            }],
            output='screen',
        ),
    ])
