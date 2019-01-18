#!/usr/bin/env python3
# coding: utf-8
__author__ = 'wanglei_sxcpx@kedacom.com'

import json
import os
import time


def save_json(data_dict, data_path):
    '''
    将订阅获取到的数据，转为json后写入文件
    :param data_dict: 要转换为json格式的数据字典
    :param data_path: json串写入的文件路径 如：/opt/data/DataWatcher/RedisData.data
    :return: 写入的结果，exception内容
    '''
    if not os.path.isdir(os.path.dirname(os.path.abspath(data_path))):
        os.makedirs(os.path.dirname(os.path.abspath(data_path)))
    start_time = time.time()
    if os.path.exists(os.path.abspath(data_path)):
        print(os.path.getsize(os.path.abspath(data_path)))
        if os.path.getsize(os.path.dirname(os.path.abspath(data_path))) > 1000000:
    
            print(time.time() - start_time)


    with open(data_path, 'a+', encoding='utf-8')as f:
        try:
            json.dump(data_dict, f)
            f.write('\n')
            return True, ''
        except Exception as e:
            return False, e

