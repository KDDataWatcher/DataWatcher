#!/usr/bin/env python3
# coding: utf-8
__author__ = 'wanglei_sxcpx@kedacom.com'

import datetime
from common.setting import *
import json


def save_json(data_dict, sys_logger):
    """
    将订阅获取到的数据，写入syslog
    :param data_dict: 要转换为json格式的数据字典
    """
    try:
        sys_logger.info(json.dumps(data_dict))
    except:
        pass


def format_timestamp(time):
    d = datetime.datetime.fromtimestamp(time)
    utc_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    timestamp = d.strftime(utc_format)

    return timestamp
