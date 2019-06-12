#!/usr/bin/env python3
# coding: utf-8
__author__ = 'wanglei_sxcpx@kedacom.com'

import sys
import pika
import requests
import json
import setproctitle
import threading
from queue import Queue
from common.globalfun import load_my_logging_cfg, config

'''
GET http://172.16.186.208:6674/api/trace-files   
GET http://172.16.186.208:6674/api/traces
DELETE http://172.16.186.208:6674/api/trace-files/mau.conf.ex.log
DELETE http://172.16.186.208:6674/api/traces/%2F/meeting.mccntf.ex

x = {
        '@timestamp':'',
        'beat':{
            'platform_moid':'',
            'name':'mqwatcher',
            'exchange':'',
            'node':'',
            'queue':'',
            'headers':{
                'exchange_name':'',
                'routing_keys':[],
            },
        },
        'properties':{
            'expiration': '',
            'content-type': 'application/json',
        },
        'source':{
            'type':'',
            'confE164':'',
            'meetingID':'',
            'moid':'',
            'confname':'',
        },
    }

'''
process_name = 'mqwatcher'


class MQWatcher(object):
    def __init__(self, host='127.0.0.1', server_port=5672, manager_port=5673,
                 username='dev', password='dev', vhost='%2f', trace_format='json', log_path=None):
        self.host = host
        self.server_port = server_port
        self.manager_port = manager_port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.trace_format = trace_format
        self.__name__ = 'MQWatcher'
        self.logger = load_my_logging_cfg(self.__class__, log_path).getLogger(self.__name__)
        # self.opened_trace = []
        self.base_url = 'http://%s:%s/' % (self.host, self.manager_port,)
        self.headers = {
            'content-type': 'application/json'
        }
        self.auth = (self.username, self.password,)
        self.session = requests.session()
        self.channel = None
        self.queue = Queue()

    @property
    def is_running(self):
        url = self.base_url
        try:
            ret = self.session.head(url, timeout=3)
        except:
            ret = None
        return ret

    def get_channel(self):
        user_pwd = pika.PlainCredentials(self.username, self.password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.server_port, credentials=user_pwd)
        )
        channel = None
        try:
            if connection.is_open:
                channel = connection.channel()
                self.channel = channel
        except Exception as e:
            self.logger.error(str(e))

        return channel

    def consumer(self, queue_name='ops.trace.q'):
        channel = self.get_channel()
        if channel:
            print('xx1')
            try:
                channel.queue_declare(queue='ops.trace.q', exclusive=False, auto_delete=True)
            except Exception as e:
                print(e)
            # queue_name = result.method.queue
            channel.queue_bind(exchange='amq.rabbitmq.trace', queue='ops.trace.q', routing_key='deliver.#')
            print('xx2')
            channel.basic_qos(prefetch_count=0)
            print('xx3')
            for method_frame, properties, body in channel.consume('ops.trace.q'):
                #                print(body)
                #                print(method_frame)
                #                print(properties)
                channel.basic_ack(method_frame.delivery_tag)
                self.queue.put(body)
                self.logger.info(body)
                # exit(0)
            # channel.basic_consume('ops.trace.q', self.callback, auto_ack=True)

    #            print(queue_name)
    # channel.start_consuming()

    @staticmethod
    def callback(ch, method, properties, body):
        # print("%s" % (body))
        # print(properties.timestamp)
        # print(properties.decode)

        content = {
            '@timestamp': '',
            'beat': {
                'platform_moid': '',
                'name': process_name,
                'exchange_name': '',
                'node': '',
                'queue': '',
                'headers': {}
            }
        }

        print(json.dumps(properties.headers, indent=4))

        # print(dir(method))
        # print(method.routing_key)
        # print(method.exchange)
        # exit(0)
        '''
        'cluster_id',
        'content_encoding', 
        'content_type', 
        'correlation_id', 
        'decode', 
        'delivery_mode', 
        'encode', 
        'expiration', 
        'headers', 
        'message_id', 
        'priority', 
        'reply_to', 
        'timestamp', 
        'type', 
        'user_id'
        '''
        # print("%s" % (json.loads(body),))

    def close(self):
        print('close')

    def save(self):
        while True:
            print(self.queue.qsize())
            if not self.queue.empty():
                x = self.queue.get()
                self.logger.info(x)
                print(x)


def main():
    if sys.platform == 'linux':
        log_path = config.get('dataWatcher', 'LogPath')
    else:
        log_path = './log/'

    mq_l = ['172.16.186.208', '172.16.186.209']
    index = 0
    while index < len(mq_l):
        watcher = MQWatcher('172.16.186.208', manager_port=6674, log_path=log_path, )
        logger = watcher.logger
        try:
            if not watcher.is_running:
                logger.error('rabbitmq is not running... exit 1')
                watcher.session.close()
                exit(1)

                t = threading.Thread(target=watcher.consumer, args=('#.*',))
                t.start()
        except Exception as e:
            logger(str(e))
            watcher.close()
            if index >= len(mq_l):
                index = 0
            else:
                index += 1


if sys.platform == 'linux':
    log_path = config.get('dataWatcher', 'LogPath')
else:
    log_path = './log/'

mq_l = ['172.16.186.208', '172.16.186.209']
index = 0
import time, os, threading

setproctitle.setproctitle('mqwatcher')
print(os.getpid())
while index < len(mq_l):
    print(mq_l[index])
    watcher = MQWatcher(mq_l[index], manager_port=6674, log_path=log_path, )

    try:
        t1 = threading.Thread(target=watcher.consumer())
        t2 = threading.Thread(target=watcher.save())
        t1.start()
        t2.start()
        # watcher.consumer()
    except Exception as e:
        print(e)
        watcher.close()
        if index >= len(mq_l) - 1:
            index = 0
        else:
            index += 1
        time.sleep(1)
