#!/usr/bin/env python3
# coding: utf-8
__author__ = 'wanglei_sxcpx@kedacom.com'

import redis, json, time, datetime, re, os, threading
from redis.exceptions import TimeoutError, ConnectionError, ResponseError
from common.setting import *

config.read(os.path.join(CONF_PATH, 'RedisWatcher.ini'))
host = config.get('redisInfo', 'IpAddr')
port = config.get('redisInfo', 'Port')
password = config.get('redisInfo', 'UserPwd')
channel = config.get('redisInfo', 'Channel')

logging = load_my_logging_cfg()
logger = logging.getLogger(__name__)


class RedisWatcher(object):
    def __init__(self, host='localhost', port=6379, password=None, channel=None):
        self._host = host
        self._port = port
        self._password = password
        self._channel = channel

    def redis_subscribe(self):
        conn = redis.Redis(host=self._host, port=self._port, password=self._password, socket_connect_timeout=5)
        pub = conn.pubsub()
        pub.subscribe(self._channel)
        return pub

    def get_message(self, pub):
        if hasattr(pub, 'listen'):
            for message in pub.listen():
                print(message)


if __name__ == '__main__':

    def get_time():
        i = 0
        while True:
            print(i, 's')
            time.sleep(1)
            i += 1


    t = threading.Thread(target=get_time, args=(), daemon=True)
    t.start()

    while True:
        try:
            if port:
                watcher = RedisWatcher(host=host, port=port, password=password, channel=channel)
            else:
                watcher = RedisWatcher(host=host, password=password, channel=channel)
            print('connect to %s' % host)

            pub = watcher.redis_subscribe()

            watcher.get_message(pub)
        except TimeoutError as e:
            logger.error(e)
        except ConnectionError as e:
            logger.error(e)
        except ResponseError as e:
            if 'invalid password' in e:
                logger.error('password error...')
            else:
                logger.error(e)
            exit(2)
        except Exception as e:
            logger.error(e)

        finally:
            time.sleep(1)
