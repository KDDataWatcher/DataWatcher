#!/usr/bin/env python3
# coding: utf-8
__author__ = 'wanglei_sxcpx@kedacom.com'

import redis
import time
import datetime
import json
from redis.exceptions import TimeoutError, ConnectionError, ResponseError
from collections import deque
import threading
from json.decoder import JSONDecodeError
from common.setting import *
from common.globalfun import save_json


class RedisWatcher(object):
    def __init__(self, host='localhost', port=6379, password=None, channel=None, data_path=None, log_path=None):
        '''

        :param host: 服务器地址
        :param port: 服务器端口
        :param password: 服务器密码
        :param channel: 订阅消息通道名称
        :param data_path: 存储数据文件路径
        :param log_path: 存储程序日志路径
        '''
        self._host = host
        self._port = port
        self._password = password
        self._channel = channel
        self._data_path = data_path
        self._log_path = log_path
        self.__name__ = 'RedisWatcher'
        self._logger = None
        # 存储数据的队列
        self._data_queue = deque()

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
        pub.subscribe(*self._channel.replace(' ', '').split(','))
        return pub

    def get_message(self, pub):
        '''
        等待订阅的channel推送消息，并将消息写入文件
        :param pub: 订阅channel后的pubsub对象
        :return: None
        '''
        if hasattr(pub, 'listen'):
            for message in pub.listen():
                if len(threading.enumerate()) == 1:
                    # 当IO操作线程退出后，重新创建并开启IO操作线程
                    self._save_thread = threading.Thread(target=self.save_message, name='SaveMessageThread')
                    self._save_thread.start()

                if isinstance(message['data'], (bytes, str,)):
                    data = message['data'].decode('utf-8')
                    data = eval("'%s'" % data)
                    try:
                        data_dict = json.loads(data)
                        time_now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                        data_dict.update({'@timestamp': time_now})
                        # 将订阅到的消息内容存入数据队列，待IO操作线程取出后写入文件
                        self._data_queue.append(data_dict)

                    except JSONDecodeError as e:
                        self._logger.error('json data error:%s, data: %s' % (e, data))
                    except Exception as e:
                        self._logger.error(e)

        else:
            self._logger.error('pub error...')

    def save_message(self):
        '''
        保存队列中的数据至文件
        :return: None
        '''
        while True:
            try:
                # self._logger.debug('队列长度：%s, 线程数量：%s' % (len(self._data_queue), threading.enumerate()))
                data_dict = self._data_queue.popleft()
                if isinstance(data_dict, dict):
                    flag, msg = save_json(data_dict, self._data_path)
                    if not flag:
                        self._logger.error('write file error: %s' % msg)
                    elif msg:
                        self._logger.info('%s' % msg)
            except IndexError as e:
                # 队列取空之后，结束当前线程
                # self._logger.error(e)
                return
            except Exception as e:
                self._logger.error(e)
                return


def main():
    '''
    主函数，实例化Watcher，创建redis连接池，订阅channel，获取发布消息，写入文件
    :return: None
    '''
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
            logger.error('connect timeout: %s' % e)
        except ConnectionError as e:
            logger.error('connect error: %s' % e)
        except ResponseError as e:
            if 'invalid password' in e:
                logger.error('password error,please check your config file...')
                exit(2)
            else:
                logger.error('response error: %s' % e)
        except Exception as e:
            logger.error('exception error: %s' % e)

        finally:
            logger.error('Connection closed by foreign host,waiting for reconnect...')
            time.sleep(1)
            logger.error('Please try to reconnect...')


if __name__ == '__main__':
    main()
