#!/usr/bin/env python3
# coding: utf-8
__author__ = 'wanglei_sxcpx@kedacom.com'

import pika
import sys
import os
import time
import setproctitle
import re
import base64
import json
import threading
from queue import Queue
from common.setting import load_my_logging_cfg, config, CONF_PATH
from common.globalfun import save_json, format_timestamp


class MQWatcher(object):
    def __init__(self, hosts=('127.0.0.1',), port=5672, username='dev', password='dev', vhost='/',
                 platform_moid='', process_name='mqwatcher', log_path=None,
                 json_queues=(), json_exchanges=(), pb_queues=(), pb_exchanges=()):
        self.hosts = hosts
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.platform_moid = platform_moid
        self.logger = load_my_logging_cfg(self.__class__, log_path).getLogger('watcher')
        self.sys_logger = load_my_logging_cfg(self.__class__, log_path).getLogger('syslog')
        self.json_queues = json_queues
        self.json_exchanges = json_exchanges
        self.pb_queues = pb_queues
        self.pb_exchanges = pb_exchanges
        self.queue = Queue()
        self.process_name = process_name

        # 设置进程名称
        setproctitle.setproctitle(self.process_name)

    @property
    def connection(self):
        user_pwd = pika.PlainCredentials(self.username, self.password)
        parameters = [pika.ConnectionParameters(host=host, port=self.port, credentials=user_pwd) for host in self.hosts]
        connection = pika.BlockingConnection(parameters)
        return connection

    @property
    def channel(self):
        try:
            if self.connection.is_open:
                channel = self.connection.channel()
                return channel
            else:
                return None
        except Exception as e:
            self.logger.error(str(e))
            return None

    def consumer(self, exchange_name='amq.rabbitmq.trace', queue_name='ops.trace.q', routing_key='deliver.#'):
        # channel = self.get_channel()
        channel = self.channel
        if channel:
            try:
                channel.queue_declare(queue=queue_name, exclusive=False, auto_delete=True)
            except Exception as e:
                self.logger.error(str(e))
            try:
                channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)
            except Exception as e:
                self.logger.error(str(e))
                # 如果无对应exchange，10s后退出线程
                time.sleep(10)
                exit(1)
            channel.basic_qos(prefetch_count=0)
            try:
                for method_frame, properties, body in channel.consume(queue_name):
                    timestamp = format_timestamp(time.time())
                    channel.basic_ack(method_frame.delivery_tag)
                    self.queue.put([method_frame, properties, body, timestamp])
            except Exception as e:
                self.logger.error(str(e))

    def save_trace(self):
        content = {
            '@timestamp': '',
            'beat': {
                'platform_moid': self.platform_moid,
                'name': self.process_name,
                'exchange': '',
                'headers': {}
            },
            'source': ''
        }

        while True:
            if not self.queue.empty():
                for i in range(self.queue.qsize()):
                    # 队列中拿到的为长度为4的列表，分别赋值给4个变量
                    method_frame, properties, body, timestamp = self.queue.get()
                    # 正则匹配exchange和queue名称
                    pattern = 'deliver\.(.*?\.q):?.*'
                    q_name = re.findall(pattern=pattern, string=method_frame.routing_key)
                    content['@timestamp'] = timestamp
                    content['beat']['exchange'] = method_frame.exchange
                    content['beat']['headers'] = properties.headers
                    # exchange：exchange名字和headers中的exchange_name匹配的
                    # queue：headers中的exchange_name为空，且routing_keys和queue名字匹配的
                    if (properties.headers.get('exchange_name') in self.json_exchanges) or \
                            (not properties.headers.get('exchange_name') and q_name and q_name[
                                0] in self.json_queues) or (method_frame.exchange == 'alternate.exchange'):
                        # json格式数据
                        try:
                            content['source'] = json.loads(body.decode(encoding='utf-8'))
                            save_json(content, sys_logger=self.sys_logger)
                        except Exception as e:
                            self.logger.error(str(e))
                    elif (properties.headers.get('exchange_name') in self.pb_exchanges) or \
                            (not properties.headers.get('exchange_name') and q_name and q_name[0] in self.pb_queues):
                        self.logger.info('pb msg: %s' % body)
                        # pb格式数据
                        try:
                            content['source'] = str(base64.b64encode(body), encoding='utf-8')
                            save_json(content, sys_logger=self.sys_logger)
                        except Exception as e:
                            self.logger.error(str(e))
                    else:
                        # 丢弃的数据
                        self.logger.debug('drop msg: %s' % body)
            else:
                # self.logger.info('queue is empty...wait')
                time.sleep(0.001)


def main():
    config.read(os.path.join(CONF_PATH, 'MQWatcher.ini'))

    if sys.platform == 'linux':
        log_path = config.get('Watcher', 'LogPath')
    else:
        log_path = './log/'

    platform_moid = config.get('NodeInfo', 'PlatformDomainMoid')
    hosts = config.get('MqInfo', 'MqIpAddr').split(',')
    port = config.get('MqInfo', 'MqPort')
    username = config.get('MqInfo', 'UserName')
    password = config.get('MqInfo', 'UserPwd')
    vhost = config.get('MqInfo', 'Vhost')
    process_name = 'mqwatcher'
    json_queues = [queue.strip() for queue in config.get('Json', 'Queue').split(',')]
    json_exchanges = [exchange.strip() for exchange in config.get('Json', 'Exchange').split(',')]
    pb_queues = [queue.strip() for queue in config.get('Pb', 'Queue').split(',')]
    pb_exchanges = [exchange.strip() for exchange in config.get('Pb', 'Exchange').split(',')]

    watcher = MQWatcher(hosts=hosts, port=port, username=username, password=password, vhost=vhost,
                        platform_moid=platform_moid, process_name=process_name, log_path=log_path,
                        json_queues=json_queues, json_exchanges=json_exchanges,
                        pb_queues=pb_queues, pb_exchanges=pb_exchanges)
    pid = os.getpid()
    logger = watcher.logger
    logger.info('MQWatcher started with pid:%s' % pid)

    try:
        # 消费者线程，向queue中投递消息
        t_consumer = threading.Thread(target=watcher.consumer, name='t_consumer')
        t_consumer.start()
        # 无法路由消息消费者线程，向queue中投递消息
        t_alternate_consumer = threading.Thread(target=watcher.consumer, name='t_alternate_consumer',
                                                args=('alternate.exchange', 'ops.alternate.q', '#'))
        t_alternate_consumer.start()
        # 存储线程，向syslog写日志
        t_save_trace = threading.Thread(target=watcher.save_trace, name='t_save_trace')
        t_save_trace.start()
        while True:
            if not t_consumer.is_alive():
                t_consumer = threading.Thread(target=watcher.consumer, name='t_consumer')
                t_consumer.start()
                logger.info('restart t_consumer thread')

            if not t_alternate_consumer.is_alive():
                t_alternate_consumer = threading.Thread(target=watcher.consumer, name='t_alternate_consumer',
                                                        args=('alternate.exchange', 'ops.alternate.q', '#'))
                t_alternate_consumer.start()
                logger.info('restart t_alternate_consumer thread')

            if not t_save_trace.is_alive():
                t_save_trace = threading.Thread(target=watcher.save_trace, name='t_save_trace')
                t_save_trace.start()
                logger.info('restart save_trace thread')

            time.sleep(1)
            logger.info('check threads alive...')


    except Exception as e:
        logger.error(str(e))


if __name__ == '__main__':
    main()
