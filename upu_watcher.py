# -*- coding: utf-8 -*-
'''
Created on 2018年12月21日
 
@author: jay
'''


from ctypes import *
from enum import Enum, unique
import threading
import json
import time
import datetime
import re
import queue
import os
from common.setting import *
from common.globalfun import *


@unique
class MacroDefinition(Enum):
    """
    @unique 装饰器检测保证没有重复值
    """
    # Mt Info 结构体宏定义
    MOID_MAX_LEN = 41
    E164_MAX_LEN = 14
    COMMON_MAX_LEN = 17
    DOMAIN_MAX_LEN = 65
    TRANTYPE_MAX_LEN = 33
    DATA_MAX_LEN = 1024


class MtInfoStruct(Structure):
    """
    Construct Mt Struct
    """
    _pack_ = 4
    _fields_ = [('moid', c_char * MacroDefinition.MOID_MAX_LEN.value),
                ('e164', c_char * MacroDefinition.E164_MAX_LEN.value),
                ('prototype', c_char * MacroDefinition.COMMON_MAX_LEN.value),
                ('mttype', c_char * MacroDefinition.COMMON_MAX_LEN.value),
                ('mtstate', c_char * MacroDefinition.COMMON_MAX_LEN.value),
                ('mtip', c_char * MacroDefinition.COMMON_MAX_LEN.value),
                ('nuip', c_char * MacroDefinition.COMMON_MAX_LEN.value),
                ('userdomain', c_char * MacroDefinition.DOMAIN_MAX_LEN.value),
                ('deviceid', c_char * MacroDefinition.MOID_MAX_LEN.value),
                ('nuplatformid', c_char * MacroDefinition.MOID_MAX_LEN.value),
                ('transfertype', c_char * MacroDefinition.TRANTYPE_MAX_LEN.value),
                ('callstate', c_char * MacroDefinition.COMMON_MAX_LEN.value),
                ('isprivatenetmt', c_int),
                ('roommoid', c_char * MacroDefinition.MOID_MAX_LEN.value),
                ('data', c_char * MacroDefinition.DATA_MAX_LEN.value)]


class UpucLoopThread(threading.Thread):
    """
    Upu Loop
    """
    def __init__(self, upuclientwrap):
        super(UpucLoopThread, self).__init__()
        self.upuclientwrap = upuclientwrap
        
    def run(self):
        while True:
            self.upuclientwrap.upuclientlib.upu_client_connect(self.upuclientwrap.upuclient,
                                                               self.upuclientwrap.ip.encode('utf-8'),
                                                               int(self.upuclientwrap.port), 3)
            self.upuclientwrap.upuclientlib.upu_client_loop(self.upuclientwrap.upuclient)
            time.sleep(3)


class RecorderThread(threading.Thread):
    """
    Record data to file
    """

    def __init__(self,upuqueue,datafile):
        super(RecorderThread, self).__init__()
        self.upuqueue = upuqueue
        self.datafile=datafile

    def run(self):
        while True:
            if not self.upuqueue.empty():
                logger.info("RecorderThread start.........")
                self.handeler_pubcallback(self.upuqueue.get())
            else:
                time.sleep(1)

    def handeler_pubcallback(self, pubdata):
        logger.info("data recorder.........." + str(pubdata['topic']))
        # with open("upu_data.json", "a+", encoding="utf-8") as upufile:
        for i in range(0, int(pubdata['count'])):
            pubdatadict={}
            pubdatadict["@timestamp"] = pubdata['timestamp']
            pubdatadict["mtstate_change"]=pubdata['topic']
            pubdatadict["e164"] = pubdata["result"][i].e164.decode()
            pubdatadict["prototype"] = pubdata["result"][i].prototype.decode()
            pubdatadict["mttype"] = pubdata["result"][i].mttype.decode()
            pubdatadict["mtstate"] = pubdata["result"][i].mtstate.decode()
            pubdatadict["mtip"] = pubdata["result"][i].mtip.decode()
            pubdatadict["nuip"] = pubdata["result"][i].nuip.decode()
            pubdatadict["userdomain"] = pubdata["result"][i].userdomain.decode()
            pubdatadict["deviceid"] = pubdata["result"][i].deviceid.decode()
            pubdatadict["nuplatformid"] = pubdata["result"][i].nuplatformid.decode()
            pubdatadict["transfertype"] = pubdata["result"][i].transfertype.decode()
            pubdatadict["callstate"] = pubdata["result"][i].callstate.decode()
            pubdatadict["isprivatenetmt"] = pubdata["result"][i].isprivatenetmt
            pubdatadict["roommoid"] = pubdata["result"][i].roommoid.decode()
            pubdatadict["data"] = pubdata["result"][i].data.decode()
            logger.info(pubdatadict)
            save_json(pubdatadict,self.datafile)
            # upufile.write(json.dumps(pubdatadict) + "\n")

class UpuclientWrap():
    """
    Connect Upu
    """
    _default_timeout = 60

    def __init__(self, ip, port, appmoid, upuqueue,datafile):
        self.ip = ip
        self.port = port
        self.connectflag=0
        self.upuqueue=upuqueue

        # Load Upuclient Library
        try:
            self.upuclientlib = cdll.LoadLibrary('libupu_r.so')

            # Connect
            clientid = appmoid + str(os.getpid())
            self.upuclient = self.upuclientlib.upu_client_init(clientid.encode('utf-8'), 10)
            # 设置连接回调函数
            CMPFUNC = CFUNCTYPE(None, c_int, c_int, c_int, c_int, c_int, c_int, c_int, POINTER(MtInfoStruct), c_void_p)
            self._callback = CMPFUNC(self.upuclient_callback)
            self.upuclientlib.upu_client_set_callback(self.upuclient, self._callback, None)
            # 设置订阅回调函数
            CMPPUBFUNC = CFUNCTYPE(None, c_int, c_int, c_int, c_int, c_int, c_int, POINTER(MtInfoStruct), c_void_p)
            self._pubcallback = CMPPUBFUNC(self.upuclient_pubcallback)    
            self.upuclientlib.upu_client_set_pubcallback(self.upuclient,self._pubcallback, None)  
            # 保活线程
            self.upuloop = UpucLoopThread(self)
            self.upuloop.start()
            # 数据记录线程
            RecorderThread(self.upuqueue,datafile).start()

        except Exception as e:
            logger.error(e)

    """
    flag: 请求标识
    event: 通知事件()
    type: 结果类型(增删查才有结果类型)
    errcode: request是否成功
    count: 符合条件的个数(查询有效)
    total: 所有总段数量(仅在 find_mt_multi_by_usrdomain and find_mt_count_by_usrdomain被使用)
    result: 具体查询的信息(只读)
    arg: 用户设定的参数信息
    """
    def upuclient_callback(self, flag, event, opercode, type, errcode, count, total, result, arg):
        logger.info({'flag': flag, 'event': event, 'opercode': opercode, 'type': type, 'errcode': errcode, 'count': count, 'total': total})
        if event == 1:
            self.connectflag=1
            logger.info('Connect to UpuServer Success!')
        elif event == 2:
            logger.error('Disconnect with UpuServer, Try Connect again....')
            self.upuclientlib.upu_client_connect(self.upuclient, self.ip.encode('utf-8'), int(self.port), 3)
        elif event == 3:
            types = (13, 14, 15, 16)
            if opercode in types:
                    pass
            else:
                pass

    def upuclient_pubcallback(self, flag, errcode, opercode, topic, type,  count, result, arg):
        logger.info({'flag': flag, 'errcode': errcode, 'opercode': opercode, 'topic': topic, 'type': type, 'count': count,})
        if opercode == 200:
            logger.info('PUB UpuServer Success!')
        elif opercode == 201:
            logger.error('DisPUB with UpuServer, Try Connect again....')
#             self.upuclientlib.upu_client_connect(self.upuclient, self.ip.encode('utf-8'), int(self.port), 3)
        elif opercode == 202:
            if type == 2:
                timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                data_callback={
                        'timestamp': timestamp,
                        'topic': topic,
                        'count': count,
                        'result':result,}
                try:
                    self.upuqueue.put_nowait(data_callback)
                except queue.Full:
                    logger.error('UpuQueue is Full ....')

        else:
            logger.error('Unknown Error ....')

    def find_mt_multi_by_usrdomain(self, mtinfos, start, count, nflag,  time_out=_default_timeout):
        logger.info("EXC find_mt_multi_by_usrdomain")
        self.upuclientlib.find_mt_multi_by_usrdomain(self.upuclient, mtinfos, start, count, nflag)
#         timer = threading.Timer(time_out, self.on_rpc_timeout, (nflag,))
#         timer.start()
    def subscribe_topic(self,_topic,nflag):
        logger.info("EXC subscribe_topic")
        self.upuclientlib.subscribe_topic(self.upuclient, _topic, nflag)

class upuwatcher_log():
    def __init__(self):
        self.__name__="upu"

if __name__ == '__main__':

    # upu配置获取

    ip = config.get('upu', 'IpAddr')
    port = config.get('upu', 'Port')
    appmoid = config.get('upu', 'appmoid')
    queuesize = int(config.get('upu', 'queuesize'))
    upudatafile = config.get('common', 'FilePath')
    upulogfile = config.get('common', 'LogPath')

    # upu日志模板设置
    logger = load_my_logging_cfg(upuwatcher_log(),upulogfile)
    # logger.setLevel(level=logging.INFO)
    # fh = logging.FileHandler(upulogfile)
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    #                               datefmt="%Y/%m/%d %X")
    # fh.setFormatter(formatter)
    # logger.addHandler(fh)

    # 设置队列大小，默认无限制
    q=queue.Queue(queuesize)

    #初始化upuclient
    upuwatcher=UpuclientWrap(ip,port,appmoid,q,upudatafile)

    # 判断upu是否连接上
    while True:
        # 连上upu后调用订阅函数
        if upuwatcher.connectflag:
            upuwatcher.subscribe_topic(1,10000001)
            upuwatcher.subscribe_topic(2,10000002)
            break
        else:
            time.sleep(1)
                                   
                                  
 

