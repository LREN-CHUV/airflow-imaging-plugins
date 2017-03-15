
"""
.. module:: operators.free_space_sensor

    :synopsis: A SensorOperator that waits until there is enough free space on the disk

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""

import os
import logging

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils import apply_defaults


class FreeSpaceSensor(BaseSensorOperator):

    """
    Waits until there is enough free space on the disk.

    :param path: path of the disk area to check for free space
    :type path: string
    :param free_disk_threshold: minimum percentage of free disk
    :type free_disk_threshold: float
    """

    template_fields = tuple()

    @apply_defaults
    def __init__(self, path, free_disk_threshold, *args, **kwargs):
        super(FreeSpaceSensor, self).__init__(*args, **kwargs)
        self.path = path
        self.free_disk_threshold = free_disk_threshold

    def poke(self, context):
        disk = os.statvfs(self.path)
        free = disk.f_bavail / disk.f_blocks
        logging.info('Checking if there is enough free space on {0}, expected at least {1:.2%} free'
                     .format(self.path, self.free_disk_threshold))
        return free >= self.free_disk_threshold
