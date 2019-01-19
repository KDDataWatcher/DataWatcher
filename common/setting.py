#!/usr/bin/env python3
# coding: utf-8
__author__ = 'wanglei_sxcpx@kedacom.com'

import os
import configparser
import logging.config

# 路径定义
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONF_PATH = os.path.join(BASE_PATH, 'conf')

# 配置文件
config = configparser.ConfigParser()
config.read(os.path.join(CONF_PATH, 'DataWatcher.ini'))

def load_my_logging_cfg(App, log_path):
    '''
    定义日志格式
    :param App: Watcher类
    :param LOG_PATH: 日志文件路径
    :return: logging
    '''
    # 日志格式
    standard_format = '[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d][%(threadName)s:%(thread)d][%(message)s]'

    LOGGING_DIC = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': standard_format
            },
        },
        'filters': {},
        'handlers': {
            # 打印到文件的日志
            'default': {
                'level': 'DEBUG',
                'class': 'logging.handlers.RotatingFileHandler',  # 保存到文件
                'formatter': 'standard',
                'filename': os.path.join(log_path, 'DataWatcher.log'),  # 日志文件
                'encoding': 'utf-8',  # 日志文件的编码
            },
        },
        'loggers': {
            # logging.getLogger(__name__)拿到的logger配置
            '': {
                'handlers': ['default'],  # 这里把上面定义的handler加上，即log数据既写入文件
                'level': 'DEBUG',
                'propagate': True,  # 向上（更高level的logger）传递
            },
        },
    }

    if not os.path.isdir(log_path):
        os.makedirs(log_path)
    # 将日志文件替换为模块本身的日志路径
    LOGGING_DIC['handlers']['default'].update({'filename': os.path.join(os.path.abspath(log_path),'%s.log' % App.__name__)})
    logging.config.dictConfig(LOGGING_DIC)
    return logging

