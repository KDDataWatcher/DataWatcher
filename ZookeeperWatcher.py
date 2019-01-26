# !/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
@shanchenglong
@time: 2019-1-18
"""

import sys
import json
import re
import time
import pyinotify
from common.setting import *
from common.globalfun import save_json

sys.path.append("/opt/luban/luban_c_python3/lib/python3.5/site-packages")
from kazoo.client import KazooClient
from kazoo.client import DataWatch


exclude_business = ["java"]  # list of apps that needn't to cheek
special_business = ["syslog-ng", "radar-server", "dms", "glusterfsd", "glusterfs", "zookeeper"]  # special apps list
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
log_path = config.get("dataWatcher", "LogPath")
logger = ZkWatcherLogger.get_logger(log_path)


def get_config():
    """
    get options from configuration file, default /conf/DataWatcher.ini
    :return: zookeeper_ip_port/data_path/config.json_path
    """
    zk_ip = config.get("zookeeperInfo", "IpAddr")
    zk_port = config.get("zookeeperInfo", "Port")
    data_path = config.get("zookeeperInfo", "BaseFilePath")
    json_path = config.get("zookeeperInfo", "json_path")

    try:
        assert zk_ip, 'zookeeper service ip addr is unset'
        assert data_path, 'log_path is unset'
        assert json_path, 'json_path is unset'
    except AssertionError as err:
        logger.error(err)
        return None

    zk_ip_port = zk_ip + ':2181' if not zk_port else zk_ip + ':' + zk_port
    return zk_ip_port, data_path, json_path


def write_data(data, ctime, path):
    """
    write data to file throw the function save_json from source file common.globalfun
    :param data: ZNode data
    :param ctime: timestamp of the ZNode changing
    :type ctime: int
    :param path: path of ZNode
    :return:
    """
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ctime/1000))
    path_spt = path.split("/")
    if len(path_spt) == 7:
        typ = "app"
        name = path_spt[5].split('_')[0]
        moid = path_spt[5].split('_')[1]
    elif len(path_spt) == 5:
        typ = "machine_room"
        name = "默认机房"
        moid = path_spt[3]
    else:
        # 路径错误
        return None

    status = re.findall(cmp_pattern, data.decode('utf-8'))
    if status[0] == 'started':
        status = "online"
    else:
        status = "offline"

    return dict(zip(['@timestamp', 'name', 'moid', 'type', 'status'], [timestamp, name, moid, typ, status]))
    # save_json()   # 调接口写文件


class AssembleZKInfo:
    # 待重做，json格式最终由吴凯提供
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
    """
    handing config_json changes
    """
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
    """
    set ZNode watcher ang handing data changes
    """
    def __init__(self, host_port, timeout=10):
        self._host_port = host_port
        self._timeout = timeout
        self._zk = KazooClient(hosts=self._host_port, timeout=self._timeout,
                               connection_retry={'max_tries': -1, 'delay': 1, 'backoff': 1})
        self._zk.add_listener(self.connection_listener)
        self.run_code = 0

    def start(self, zk_path):
        try:
            assert isinstance(zk_path, dict), "parameters must be dict"
        except AssertionError as err:
            print(err)
            raise TypeError(err)

        while True:
            try:
                self._zk.start()
            except Exception as err:
                print(err)
                time.sleep(2)
            else:
                break

        self._watcher(zk_path)
        self.run_code = 1

    def _watcher(self, zk_path):
        for value in zk_path.values():
            @self._zk.DataWatch(value)
            def my_dw(data, stat, event):
                if event is None:
                    if data is None:
                        print("首次监听，节点未创建")
                        # 特殊处理
                        # self.handler(data, stat.mtime, value)
                    else:
                        print("首次监听，节点已创建")
                        self.handler(data, stat.mtime, value)
                else:
                    self.handler(data, stat.mtime, event.path)
            # DataWatch(client=self._zk, path=value, func=self.handler)

    def stop(self):
        self._zk.stop()
        self._zk.close()
        self.run_code = 0

    @staticmethod
    def connection_listener(state):
        """
        监控zk连接状态
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
    def handler(data, modify_time, zk_path):
        """
        处理节点的数据变化
        :param modify_time:
        :param zk_path:
        :param data:
        :return:
        """
        print("数据发生变化")
        print("数据变更:", data)
        print("变更时间:", modify_time)
        print("数据路径:", zk_path)
        # write_data(data, modify_time, zk_path)  # 暂时不打开


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
