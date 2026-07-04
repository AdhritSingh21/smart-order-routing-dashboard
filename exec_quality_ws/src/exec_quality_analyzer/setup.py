import os
from glob import glob
from setuptools import find_packages, setup

package_name = 'exec_quality_analyzer'

setup(
    name=package_name,
    version='0.1.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        (os.path.join('share', package_name, 'launch'),
            glob(os.path.join('launch', '*.launch.py'))),
        (os.path.join('share', package_name, 'config'),
            glob(os.path.join('config', '*.yaml'))),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Adhrit Singh',
    maintainer_email='singh.1614@osu.edu',
    description='Measures slippage, fill rate, and latency per order across trading venues',
    license='MIT',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'order_submitter = exec_quality_analyzer.order_submitter:main',
            'execution_listener = exec_quality_analyzer.execution_listener:main',
            'metrics_engine = exec_quality_analyzer.metrics_engine:main',
            'aggregator = exec_quality_analyzer.aggregator:main',
            'reporter = exec_quality_analyzer.reporter:main',
            'dashboard_bridge = exec_quality_analyzer.dashboard_bridge:main',
        ],
    },
)
