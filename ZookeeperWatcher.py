# !/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@shanchenglong
@time: 2019-1-18
"""

import sys
import json
import re
import datetime
import logging
import pyinotify
from common.setting import *
from common.globalfun import save_json

sys.path.append("/opt/luban/luban_c_python3/lib/python3.5/site-packages")
from kazoo.client import KazooClient
from kazoo.client import DataWatch

deploy_service_cfg = "/opt/config/luban_config/deploy.ini"
deploy_path_cfg = "/opt/config/luban_config/deploy.json"

log_path = "/opt/log/guard/zk_watcher.log"
datefmt = '%Y/%m/%d:%H:%M:%S'

# list of apps that needn't to cheek
exclude_business = ["java"]
# special apps list
special_business = ["syslog-ng", "radar-server", "dms", "glusterfsd", "glusterfs", "zookeeper"]

# 创建logger记录脚本自身运行过程
logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(message)s',
                    datefmt=datefmt,
                    filename=log_path,
                    filemode='a+')
logger = logging.getLogger()


def get_config():
    """
    get options from configuration file, default /conf/DataWatcher.ini
    :return: zookeeper_ip_port/data_path/config.json_path
    """
    zk_ip = config.get("zookeeperInfo", "IpAddr")
    zk_port = config.get("zookeeperInfo", "Port")
    des_path = config.get("zookeeperInfo", "BaseFilePath")
    json_path = config.get("zookeeperInfo", "json_path")

    try:
        assert zk_ip, 'zookeeper service ip addr is unset'
        assert des_path, 'log_path is unset'
        assert json_path, 'json_path is unset'
    except AssertionError as err:
        logger.error(err)
        return None

    zk_ip_port = zk_ip + ':2181' if not zk_port else zk_ip + ':' + zk_port
    return zk_ip_port, des_path, json_path


class AssembleZKInfo:
    """
    read all installed businesses from deploy.ini and read all business information from deploy.json,
    then assemble zookeeper path for all businesses
    """
    def __init__(self, cfg_json_path, cfg_ini_path):
        self._json_path = cfg_json_path
        self._ini_path = cfg_ini_path
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
            return content['DeployInfo']

    def _get_ini_info(self):
        """
        read all installed apps from deploy.ini
        :return:list of apps
        """
        try:
            with open(self._ini_path) as f:
                content = f.read()
        except FileNotFoundError as err:
            logger.error(err)
            return -1
        except Exception as err:
            logger.error(err)
            return -1
        else:
            apps_tmp = re.findall("server_list = (\S+)", content)
            apps_tmp = ",".join(apps_tmp)
            apps = apps_tmp.split(",")
            return apps

    def _make_zk_path(self):
        """
        assemble zookeeper path
        """
        for app_info in self.apps_information:
            domain_moid = app_info.get("Domain_moid")
            machineRoom_moid = app_info.get("MachineRoom_moid")
            group_moid = app_info.get("Group_moid")
            key = app_info.get("Key")
            moid = app_info.get("MOID")
            if key not in self.installed_apps:
                continue

            if all([domain_moid, machineRoom_moid, group_moid, key, moid]):
                key_moid = key + "_" + moid
                zk_path = "/".join(["service", domain_moid, machineRoom_moid, group_moid, key_moid])
                self.apps_zk_path.setdefault(key, default=zk_path)
            else:
                logger.error("{} information value error: {}:{}:{}:{}:{}".format(key, domain_moid, machineRoom_moid,
                                                                                 group_moid, key, moid))

    def _get_zk_info(self):
        """
        make zookeeper path
        :return: a dict of zk_path for all installed apps, None if ERR
        """
        try:
            assert isinstance(self._json_path, str), "parameters must be string"
            assert isinstance(self._ini_path, str), "parameters must be string"
        except AssertionError as err:
            logger.error(err)
            return None

        self.installed_apps = self._get_ini_info()
        self.apps_information = self._get_json_info()
        if self.installed_apps == -1 or self.apps_information == -1:
            return None
        self._make_zk_path()
        if len(self.apps_zk_path) == 0:
            return None
        return self.apps_zk_path

    def get_zk_info(self):
        return self._get_zk_info()


class FileWatcherHandler(pyinotify.ProcessEvent):
    def my_init(self):
        self.stat = None

    def process_IN_MODIFY(self, event):
        print(event, self.stat)
        # 处理修改一次文件触发多次报警
        if os.stat(event.pathname)[8] != self.stat:
            self.stat = os.stat(event.pathname)[8]
            print('Action', "modify file: %s " % event.pathname)
            main()

    def process_IN_MOVED_TO(self, event):
        print(event, self.stat)
        print('Action', "move_to file: %s " % event.pathname)
        main()

    def process_IN_CREATE(self, event):
        pass

    def process_IN_DELETE(self, event):
        pass


class ZooKeeperWatcher:
    def __init__(self, host_port, timeout=10):
        self._host_port = host_port
        self._timeout = timeout
        self._zk = KazooClient(hosts=self._host_port, timeout=self._timeout)
        self._zk.add_listener(self.connection_listener)
        self.run_code = 0

    def start(self, zk_path):
        try:
            assert isinstance(zk_path, dict), "parameters must be dict"
            self._zk.start()
        except AssertionError as err:
            print(err)
        except Exception as err:
            print(err)
        self._watcher(zk_path)
        self.run_code = 1

    def _watcher(self, zk_path):
        for value in zk_path.values():
            DataWatch(client=self._zk, path=value, func=self.handler)

    def stop(self):
        self._zk.stop()
        self._zk.close()
        self.run_code = 0

    @staticmethod
    def connection_listener(state):
        """
        监控zk状态
        :param state:
        :return:
        """
        if state == "LOST":
            # Register somewhere that the session was lost
            print("lost")
        elif state == "SUSPENDED":
            # Handle being disconnected from Zookeeper
            print("disconnect")
        elif state == "CONNECTED":
            # Handle being connected/reconnected to Zookeeper
            print("connect")
        else:
            print("other")

    @staticmethod
    def handler(data, stat, event):
        """
        处理节点的数据变化
        :param event:
        :param data:
        :param stat:
        :return:
        """
        print("数据发生变化")
        if event is None and data is None:
            print("首次监听，节点未创建")
            print("数据为:", data)
            print("状态为:", stat)
            print("事件为:", event)
        else:
            print("数据为:", data)
            print("状态为:", stat)
            print("事件为:", event)


def file_monitor(path):
    wm = pyinotify.WatchManager()
    notifier = pyinotify.Notifier(wm)
    wm.watch_transient_file(path, pyinotify.ALL_EVENTS, FileWatcherHandler)
    notifier.loop()


def main():
    global zk_watcher, wt_json_path

    if isinstance(zk_watcher, ZooKeeperWatcher) and zk_watcher.run_code == 1:
        zk_watcher.stop()

    configuration = get_config()
    if configuration is None:
        print('获取配置失败')
        # 待处理
        pass
    zk_inf, data_path, wt_json_path = configuration

    zk_inf_obj = AssembleZKInfo(wt_json_path, None)  # 组装业务路径未定，待修改
    biz_inf = zk_inf_obj.get_zk_info()

    if biz_inf is None:
        print('获取json配置失败')
        # 待处理
        pass

    zk_watcher = ZooKeeperWatcher(zk_inf)
    zk_watcher.start(biz_inf)


if __name__ == '__main__':
    main()
    file_monitor(wt_json_path)
