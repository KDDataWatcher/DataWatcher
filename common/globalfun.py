#!/usr/bin/env python3
# coding: utf-8
__author__ = 'wanglei_sxcpx@kedacom.com'

import json
import os
from common.setting import *


def save_json(data_dict, data_path):
    '''
    将订阅获取到的数据，转为json后写入文件
    :param data_dict: 要转换为json格式的数据字典
    :param data_path: json串写入的文件路径 如：/opt/data/region/platformData/redisWatcher/redis.data
    :return: 写入的结果，exception内容
    '''
    index = '0'
    index_path = '%s.index' % data_path
    try:
        data_size = config.getint('dataWatcher', 'DataSize')
        data_num = config.getint('dataWatcher', 'DataNum')
    except Exception as e:
        return False, e
    if not os.path.isdir(os.path.dirname(os.path.abspath(data_path))):
        os.makedirs(os.path.dirname(os.path.abspath(data_path)))
        with open(index_path, 'w', encoding='utf-8')as f:
            f.write('1')
            index = '1'
    else:
        if not os.path.isfile(index_path):
            with open(index_path, 'w', encoding='utf-8')as f:
                f.write('1')
                index = '1'
        else:
            with open(index_path, 'r', encoding='utf-8')as f:
                index = f.read()
    if os.path.isfile(index_path):
        with open(index_path, 'r', encoding='utf-8')as f:
            index = f.read()

    write_data_path = os.path.abspath('%s.%s' % (data_path, index))
    if os.path.exists(os.path.abspath(write_data_path)):
        if os.path.getsize(os.path.abspath(write_data_path)) > (data_size * 1024 * 1024):
            with open(index_path, 'w', encoding='utf-8')as f:
                index = '%s' % (int(index) + 1)
                if int(index) <= data_num:
                    f.write(index)
                else:
                    f.write('1')
                    index = '1'
            write_data_path = os.path.abspath('%s.%s' % (data_path, index))
            with open(write_data_path, 'w', encoding='utf-8')as f:
                f.write('')

    with open(write_data_path, 'a+', encoding='utf-8')as f:
        try:
            json.dump(data_dict, f)
            f.write('\n')
            return True, ''
        except Exception as e:
            return False, e
