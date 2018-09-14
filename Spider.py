import json
import threading
import time
import os
import datetime


import logging
import requests
from requests.exceptions import ConnectionError
from requests.exceptions import ReadTimeout

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

ObtainDomainListInterval = 60

TS_master = '服务器套接字'

# 日志模块
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(level=logging.WARNING,
                    filename=os.path.join(BASE_DIR,'Slave.log'),
                    filemode='a',
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

# 自定义异常
class SlaveHTTPException(BaseException):
    def __init__(self,arg):
        self.args = arg


class Processor(threading.Thread):
    """
    检测线程类,负责检测域名和提交检测结果
    """
    post_domain_url = TS_master + '/spiderkingdom/api/slave_post/'
    def __init__(self,domain,node):
        threading.Thread.__init__(self)
        self.domain = domain
        self.node = node
        self.status_code = None
        self.total_time = None
        self.datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        self.detection_interval = domain['detection_interval']

    def run(self):
        start_time = time.time()
        try:
            result = requests.get("https://{}".format(self.domain['domain']),timeout=120,verify=False)
            stop_time = time.time()
            self.total_time = int((stop_time - start_time) * 1000)
            self.status_code = result.status_code
        except Exception as e:
            logging.info(e.args)
        data = {
            'http_code':self.status_code,
            'domain':self.domain['id'],
            'node':self.node,
            'datetime':self.datetime,
            'total_time':self.total_time,
            'detection_interval':self.detection_interval
        }
        print(data)
        try:
            response = requests.post(self.post_domain_url,data=data)
            if response.status_code != 201:
                # raise SlaveHTTPException("提交 {} 数据失败".format(self.domain))
                print('提交失败')
        except Exception as e:
            logging.error(e.args)
            return False


class DispatchCheckDomai(object):
    """
    调度检测线程类
    """
    get_domain_url = TS_master + '/spiderkingdom/api/slave_get/'
    def __init__(self):
        self.node = None
        self.Timer = {}             # 定时器，记录每个域名的最后检测时间
        self.last_update = None     # 记录最后一次从服务器get所有域名的时间，
        self.domains = None         # 存放从服务器上get到的所有域名
        self.processor = Processor  # 检测每个域名并提交数据到服务器的类

    def get_domain(self):
        """
        获取所有属于本节点的域名数据
        :return:
        """
        try:
            response = requests.get(self.get_domain_url)
            if response.status_code != 200:
                print(response.text)
                logging.error(response.text)
                time.sleep(30)
                return self.get_domain()
            result = json.loads(response.text)
        # 如果上面请求出错就记录日志,30秒后重试
        except Exception as e:
            logging.error(e.args)
            time.sleep(30)
            return self.get_domain()
        # 如果请求正常，将结果保存
        self.node = result.get('node')
        self.domains = result.get('data')
        return True

    def start(self):
        """
        获取域名列表,根据每个域名的检测间隔时间来调度Processor来检测
        :return:
        """
        self.last_update = 0
        while True:
            # 如果上次get所有域名的时间大于ObtainDomainListInterval，就从新在get一下，保证是最新数据
            if time.time() - self.last_update > ObtainDomainListInterval:
                self.get_domain()
                self.last_update = time.time()
            # 循环所有域名，并实例化processor类，开启一个线程处理
            for domain_info in self.domains:
                domain = domain_info.get('domain')
                if domain  not in  self.Timer:
                    self.Timer[domain] = 0
                # 如果"当前时间"减去"上次检测时间"大于此域名的"检测间隔时间",就调用检测器检测
                if time.time() - self.Timer[domain] > domain_info['detection_interval']:
                    self.Timer[domain] = time.time()
                    processor_thread = self.processor(domain_info,self.node)
                    processor_thread.start()
            time.sleep(1)




if __name__ == '__main__':

    C = DispatchCheckDomai()
    C.start()

