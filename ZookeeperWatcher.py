# !/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
@shanchenglong
@time: 2019-1-18
"""

import os
import re
import sys
import json
import time
import logging
# import pyinotify
from common.setting import config, load_my_logging_cfg
from common.globalfun import save_json

sys.path.append("/opt/luban/luban_c_python3/lib/python3.5/site-packages")
from kazoo.client import KazooClient
from kazoo.client import DataWatch

# list of apps that needn't to cheek
exclude_business = ["java", "radar-server", "dms", "glusterfsd", "glusterfs", "zookeeper", "boardmanager"]
cmp_pattern = re.compile("status = (\w+)")


# logger class
class ZkWatcherLogger:
    __slots__ = ['__name__']

    def __init__(self):
        self.__name__ = "ZooKeeperWatcher"

    @staticmethod
    def get_logger(path):
        return load_my_logging_cfg(ZkWatcherLogger(), path).getLogger()


# creat logger recording the running process
try:
    log_path = config.get("dataWatcher", "LogPath")
except Exception:
    # 获取不到日志配置就不记录日志
    logging.basicConfig(level=logging.FATAL)
    logger = logging.getLogger("ZooKeeperWatcher")
else:
    logger = ZkWatcherLogger.get_logger(log_path)
    logger.setLevel(level=logging.INFO)


def get_config():
    """
    get options from configuration file, default /conf/DataWatcher.ini
    :return: zookeeper_ip_port/data_path/config.json_path
    """
    try:
        zk_ip = config.get("zookeeperInfo", "IpAddr")
        zk_port = config.get("zookeeperInfo", "Port")
        data_path = config.get("zookeeperInfo", "BaseFilePath")
        json_path = config.get("zookeeperInfo", "json_path")

        assert zk_ip, 'zookeeper service ip addr is unset'
        assert data_path, 'log_path is unset'
        assert json_path, 'json_path is unset'
    except AssertionError as err:
        logger.error(err)
        return None
    except Exception as err:
        logger.error(err)
        return None

    zk_ip_port = zk_ip + ':2181' if not zk_port else zk_ip + ':' + zk_port
    return zk_ip_port, data_path, json_path


class AssembleZKInfo:
    # 待重做，json格式最终由吴凯提供
    """
    read all installed businesses from deploy.ini and read all business information from deploy.json,
    then assemble zookeeper path for all businesses
    """
    def __init__(self, cfg_json_path):
        self._json_path = cfg_json_path
        self.apps_zk_path = {}

    def _get_json_info(self):
        """
        read all apps information from deploy.json
        :return:list of app information
        """
        try:
            with open(self._json_path) as f:
                content = json.load(f)
        except FileNotFoundError as err:
            logger.error(err)
            return -1
        except Exception as err:
            logger.error(err)
            return -1
        else:
            return content['DeployApp'], content['DeployRoom']

    def _make_zk_path(self):
        """
        assemble zookeeper path
        """
        apps, rooms = self.apps_information

        for app_info in apps:
            domain_moid = app_info.get("Domain_moid")
            machineRoom_moid = app_info.get("MachineRoom_moid")
            group_name = app_info.get("Group_name")
            group_moid = app_info.get("Group_moid")
            key = app_info.get("Key")
            moid = app_info.get("MOID")

            if key in exclude_business:
                # 特殊业务不监控
                continue

            if all([domain_moid, machineRoom_moid, group_name, group_moid, key, moid]):
                key_moid = key + "_" + moid
                group_name_moid = group_name + '_' + group_moid
                zk_path = "/".join(["/service", domain_moid, machineRoom_moid, group_name_moid, key_moid, "status"])
                # self.apps_zk_path.setdefault(key, zk_path)
                self.apps_zk_path.setdefault(zk_path, key)
            else:
                logger.warning("make zk_path error: {}:{}:{}:{}:{}:{}".format(domain_moid, machineRoom_moid, group_name,
                                                                              group_moid, key, moid))

        for room_info in rooms:
            domain_moid = room_info.get("Domain_moid")
            machineRoom_moid = room_info.get("MachineRoom_moid")
            name = room_info.get("name")

            if all([domain_moid, machineRoom_moid, name]):
                zk_path = "/".join(["/service", domain_moid, machineRoom_moid, "status"])
                # self.apps_zk_path.setdefault(name, zk_path)
                self.apps_zk_path.setdefault(zk_path, name)
            else:
                logger.warning("make zk_path error: {}:{}:{}".format(name, domain_moid, machineRoom_moid))

    def _get_zk_info(self):
        """
        make zookeeper path
        :return: a dict of zk_path for all installed apps, None if ERR
        """
        try:
            assert isinstance(self._json_path, str), "parameters must be string"
        except AssertionError as err:
            logger.error(err)
            return None

        try:
            self.apps_information = self._get_json_info()
        except Exception as err:
            logger.error(err)
            return None

        if self.apps_information == -1:
            return None
        self._make_zk_path()
        if len(self.apps_zk_path) == 0:
            logger.warning("make zk_path abnormal, please check config_json file!")
            return None
        return self.apps_zk_path

    def get_zk_info(self):
        return self._get_zk_info()


# class FileWatcherHandler(pyinotify.ProcessEvent):
#     """
#     handing config_json changes
#     """
#     def my_init(self):
#         self.stat = None
#
#     def process_IN_MODIFY(self, event):
#         # 处理修改一次文件触发多次报警
#         if os.stat(event.pathname)[8] != self.stat:
#             logger.info("json_file changed, Action: modify, file: %s " % event.pathname)
#             self.stat = os.stat(event.pathname)[8]
#             main()
#
#     def process_IN_MOVED_TO(self, event):
#         logger.info("json_file changed, Action: move_to, file: %s " % event.pathname)
#         self.stat = os.stat(event.pathname)[8]
#         main()
#
#     def process_IN_CREATE(self, event):
#         pass
#
#     def process_IN_DELETE(self, event):
#         pass


class ZooKeeperWatcher:
    """
    set ZNode watcher ang handing data changes
    """
    def __init__(self, host_port, data_path, timeout=10):
        self._host_port = host_port
        self._timeout = timeout
        self._zk = KazooClient(hosts=self._host_port, timeout=self._timeout,
                               connection_retry={'max_tries': -1, 'delay': 1, 'backoff': 1})
        self._zk.add_listener(self.connection_listener)
        self.run_code = 0
        self._data_path = data_path
        self._zk_paths = {}

    def start(self, zk_paths):
        self._zk_paths = zk_paths
        try:
            assert isinstance(zk_paths, dict), "parameters must be dict"
        except AssertionError as err:
            raise TypeError(err)

        while True:
            try:
                self._zk.start()
            except Exception as err:
                logger.error("connect zookeeper server error: %s" % err)
                time.sleep(2)
            else:
                self.run_code = 1
                break
        self._watcher(self._zk_paths)

    def _watcher(self, zk_paths):
        for zk_path in zk_paths.keys():
            @self._zk.DataWatch(zk_path)
            def my_dw(data, stat, event):
                if event is None:
                    if data is None:
                        # 首次监听,节点未创建
                        logging.warning("Adding watcher, but ZNode isn't exits: %s" % zk_path)
                    else:
                        # 首次监听,节点已创建
                        self._handler(data, stat.mtime, zk_path)
                else:
                    if data is None:
                        # 删除节点
                        self._handler(b"", int(time.time()*1000), event.path)
                    else:
                        # 正常情况
                        self._handler(data, stat.mtime, event.path)
            # DataWatch(client=self._zk, path=value, func=self.handler)

    def stop(self):
        self._zk.stop()
        self._zk.close()
        self.run_code = 0

    def _handler(self, data, ctime, path):
        """
        handing ZNode data changes
        :param data: ZNode data
        :param ctime: timestamp of the ZNode changing
        :type ctime: int
        :param path: path of ZNode
        :return:
        """
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ctime / 1000))
        path_spt = path.split("/")
        if len(path_spt) == 7:
            typ = "app"
            domain_moid = path_spt[2]
            name = path_spt[5].rpartition('_')[0]
            moid = path_spt[5].rpartition('_')[2]
        elif len(path_spt) == 5:
            typ = "machine_room"
            # name = "默认机房"
            domain_moid = path_spt[2]
            name = self._zk_paths.get(path)
            moid = path_spt[3]
        else:
            # 路径格式错误
            logger.warning("path format error: %s" % path)
            return None

        status = re.findall(cmp_pattern, data.decode('utf-8'))
        if status[0] == 'started':
            status = "online"
        else:
            status = "offline"
        data_dict = dict(zip(['@timestamp', 'domain_moid', 'name', 'moid', 'type', 'status'],
                             [timestamp, domain_moid, name, moid, typ, status]))
        self.write_data(data_dict, self._data_path)

    @staticmethod
    def write_data(data, file):
        """
        write data to file throw the function save_json from source file common.globalfun
        """
        res, inf = save_json(data, file)
        if res is not True:
            logger.error("write data error: %s" % inf)

    @staticmethod
    def connection_listener(state):
        """
        监控zk连接状态,记录zk状态变化
        LOST: Register somewhere that the session was lost
        SUSPENDED: Handle being disconnected from Zookeeper
        CONNECTED: Handle being connected/reconnected to Zookeeper
        """
        logger.info("zk connection changes: %s" % state)


# def file_monitor(path):
#     wm = pyinotify.WatchManager()
#     notifier = pyinotify.Notifier(wm)
#     wm.watch_transient_file(path, pyinotify.ALL_EVENTS, FileWatcherHandler)
#     notifier.loop()


def main():
    # global zk_watcher, wt_json_path
    #
    # try:
    #     if isinstance(zk_watcher, ZooKeeperWatcher) and zk_watcher.run_code == 1:
    #         zk_watcher.stop()
    # except NameError:
    #     pass
    # except Exception as err:
    #     logger.error("zk_watcher stop error: %s" % err)

    configuration = get_config()
    if configuration is None:
        logger.error('get configuration from config_file failed')
        raise Exception("get configuration from config_file failed")
    zk_inf, data_path, wt_json_path = configuration

    zk_inf_obj = AssembleZKInfo(wt_json_path)
    biz_inf = zk_inf_obj.get_zk_info()

    if biz_inf is None:
        logger.error('get zookeeper information from json_file failed')
        raise Exception("get zookeeper information from json_file failed")

    zk_watcher = ZooKeeperWatcher(zk_inf, data_path)
    zk_watcher.start(biz_inf)


if __name__ == '__main__':
    main()
    while True:
        time.sleep(999999)
    # file_monitor(wt_json_path)
