#!/usr/bin/env python3
# coding: utf-8
__author__ = 'wanglei_sxcpx@kedacom.com'

import redis
import time
import datetime
import json
from redis.exceptions import TimeoutError, ConnectionError, ResponseError
from json.decoder import JSONDecodeError
from common.setting import *
from common.globalfun import save_json


class RedisWatcher(object):
    def __init__(self, host='localhost', port=6379, password=None, channel=None, data_path=None, log_path=None):
        self._host = host
        self._port = port
        self._password = password
        self._channel = channel
        self._data_path = data_path
        self._log_path = log_path
        self.__name__ = 'RedisWatcher'
        self._logger = None

    @property
    def logger(self):
        '''
        获取logger对象
        :return: logger
        '''
        logging = load_my_logging_cfg(self.__class__, self._log_path)
        self._logger = logging.getLogger(self.__name__)
        return self._logger

    def redis_pool(self):
        '''
        创建并返回一个redis连接池
        :return: redis连接池
        '''
        pool = redis.ConnectionPool(host=self._host, port=self._port, password=self._password,
                                    socket_connect_timeout=20)
        return pool

    def redis_subscribe(self, pool):
        '''
        建立一条redis连接，并订阅一个channel
        :param pool: redis连接池
        :return: 订阅channel后的pubsub对象
        '''
        conn = redis.Redis(connection_pool=pool)
        pub = conn.pubsub()
        # pub.subscribe(self._channel)
        pub.subscribe(*self._channel.replace(' ','').split(','))
        return pub

    def get_message(self, pub):
        '''
        等待订阅的channel推送消息，并将消息写入文件
        :param pub: 订阅channel后的pubsub对象
        :return:
        '''
        if hasattr(pub, 'listen'):
            for message in pub.listen():
                if isinstance(message['data'], (bytes, str,)):
                    data = message['data'].decode('utf-8')
                    data = eval("'%s'" % data)
                    try:
                        data_dict = json.loads(data)
                        time_now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                        data_dict.update({'@timestamp': time_now})
                        flag, msg = save_json(data_dict, self._data_path)
                        if not flag:
                            self._logger.error('write file error: %s' % msg)
                    except JSONDecodeError as e:
                        self._logger.error('json data error:%s, data: %s' % (e, data))

        else:
            self._logger.error('pub error...')


def main():
    pool = None

    host = config.get('redisInfo', 'IpAddr')
    port = config.get('redisInfo', 'Port')
    password = config.get('redisInfo', 'UserPwd')
    channel = config.get('redisInfo', 'Channel')
    data_path = config.get('redisInfo', 'BaseFilePath')
    log_path = config.get('dataWatcher', 'LogPath')
    if port:
        watcher = RedisWatcher(host=host, port=port, password=password, channel=channel,
                               data_path=data_path, log_path=log_path)
    else:
        watcher = RedisWatcher(host=host, password=password, channel=channel,
                               data_path=data_path, log_path=log_path)
    logger = watcher.logger
    if watcher:

        try:
            pool = watcher.redis_pool()
        except Exception as e:
            logger.error(e)
            exit(1)
    while True:
        try:
            logger.info('connecting to %s...' % host)
            if pool:
                pub = watcher.redis_subscribe(pool)
                logger.info('connected to %s' % host)
                watcher.get_message(pub)


        except TimeoutError as e:
            logger.error(e)
        except ConnectionError as e:
            logger.error(e)
        except ResponseError as e:
            if 'invalid password' in e:
                logger.error('password error...')
                exit(2)
            else:
                logger.error(e)
        # except Exception as e:
        #     logger.error(e)

        finally:
            time.sleep(5)
            logger.error('Try to reconnect...')


if __name__ == '__main__':
    main()

