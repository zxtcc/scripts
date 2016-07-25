#!/usr/bin/env python
# encoding: utf-8
import argparse
import json
import sys
import subprocess
import time
import os
import datetime
import urllib2
import urllib
import socket
import random
import multiprocessing.dummy as dummy
import threading
from Queue import Queue
import logging
import logging.config

msg_to_list = ["123", "123"]
fail_material = './fail_material'

class Shuffler(object):
    def __init__(self, file_list, log_dir, conf, thread_num, batch_size, map_size, url, sc_type):
        self.file_list = []
        self.log_dir = log_dir
        self.thread_num = thread_num
        self.batch_size = batch_size
        self.map_size = map_size
        self.requrl = url
        self.sc_type = sc_type

        for file in file_list:
            self.file_list.append(file)

        self.pool = dummy.Pool(self.thread_num)
        if not os.path.exists(self.log_dir.rstrip('/')+'/fail_log'):
            os.makedirs(self.log_dir.rstrip('/')+'/fail_log')

        logging.config.fileConfig(conf)
        self.sc_fail_logger = logging.getLogger('sc_fail')
        self.sc_res_logger = logging.getLogger('sc_res')
        self.http_fail_logger = logging.getLogger('http_fail')

    def valid(self):
        if len(self.file_list) < 1:
            return "no input file..."
        if self.thread_num < 1:
            return "thread_num should >= 1..."
        if self.batch_size < 1:
            return "batch_size should >= 1..."
        if self.map_size < 1:
            return "map_size should >= 1..."
        return None

    def run(self):
        t0 = datetime.datetime.now()
        total = 0
        for f in self.file_list:
            file = open(f)
            batch_obj_list = []
            map_obj_list = []
            prefix=os.path.split(f)[1].split('_')[0]+'materials: '
            material_count = 0
            
            for line in file:
                material_count += 1
                total += 1
                print prefix,material_count
                try:
                    obj = json.loads(line)
                    if self.sc_type == 'old' and 'fields' in obj:
                        """
                        old sc dont need seg_ingo
                        """
                        obj['fields'].pop('seg_info')
                    batch_obj_list.append(obj)
                except Exception as e:
                    print 'material load err: ',e
                    continue
                if len(batch_obj_list) >= self.batch_size:
                    """
                    a req to sc contain batch_size materials
                    """
                    map_obj_list.append(json.dumps(batch_obj_list))
                    batch_obj_list = []

                if len(map_obj_list) >= self.map_size:
                    self._work(map_obj_list)
                    map_obj_list = []

            if len(batch_obj_list) > 0:
                map_obj_list.append(json.dumps(batch_obj_list))
            if len(map_obj_list) > 0:
                self._work(map_obj_list)
            file.close()
        t1 = datetime.datetime.now()
        tt = t1 - t0
        used_s = (tt.seconds * 1000 + tt.microseconds / 1000)/1000
        print "used: ",used_s,'s'
        if used_s != 0:
            print "qps: ",(total*1.0)/(used_s*1.0)
        pass

    def _work(self, map_obj_list):
        #self.pool.map(self._debug, map_obj_list)
        self.pool.map(self._send, map_obj_list)
        pass

    def _debug(self, material):
        """
        debug
        """
        time.sleep(0.05)
        if random.randint(0,9) == 3:
            self.sc_fail_logger.debug(material+'\t'+'sc_fail')
            pass
        if random.randint(0,9) == 7:
            self.http_fail_logger.debug(material+'\t'+'http_fail')
            pass
        if random.randint(0,9) == 1:
            print 'sleep..'
            time.sleep(5)
            print 'sleep done..'
        pass

    def _send(self, material):
        """
        send materials to sc
        """
        header = {"Content-type":"application/json"}
        try:
            req_ = urllib2.Request(url=self.requrl,data=material,headers=header)
            res = urllib2.urlopen(req_, timeout=10)
            res = res.read()
            if self.check_res(res) is not None: #sc返回是否异常
                self.sc_fail_logger.debug(material+'\t'+res)
            self.sc_res_logger.debug(res)
        except urllib2.URLError as e: #处理超时等http错误
            self.http_fail_logger.debug(material+'\t'+str(e))
        except Exception as e: #处理超时等http错误
            self.http_fail_logger.debug(material+'\t'+str(e))

    def check_res(self, res):
        if self.sc_type == 'new':
            return self.check_sc_res(res)
        else:
            return self.check_old_sc_res(res)

    def check_sc_res(self, res):
        """
        res的返回是：{"totalCount":10,"successCount":0,"failureCount":0,"reasons":{}}
        """
        try:
            res = json.loads(res)
        except Exception as e:
            return "load json failed"
        if "failureCount" not in res or res["failureCount"] != 0:
            return "push to sc failed"
        
    def check_old_sc_res(self, res):
        """
        res的返回是：{"status": "ok"}
        """
        try:
            res = json.loads(res)
        except Exception as e:
            return "load json failed"
        if "status" not in res or res["status"] != "ok":
            return "push to sc failed"
        
class DataCollector(object):
    """
    collect files into one file
    """
    def __init__(self, dirs, output_file):
        self.dirs = dirs
        self.output = open(output_file, "w+")
    def check_dirs(self):
        """
        check if there have fail materials
        """
        for dir in self.dirs:
            file_names = os.listdir(dir)
            for file in file_names:
                if os.path.getsize(dir.rstrip('/')+'/'+file) != 0:
                    return "need shuffle fail materials..."
        return None
    def collect(self):
        for dir in self.dirs:
            file_names = os.listdir(dir)
            for file in file_names:
                self.__write(dir.rstrip('/')+'/'+file)
    def __write(self, file):
        """
        write file to output
        """
        input = open(file)
        for line in input:
            materials = json.loads(line.split('\t')[0])
            for material in materials:
                self.output.write(json.dumps(material)+'\n')
        input.close()
    def close(self):
        self.output.close()
    
def shuffle_once(file_path_list, log_dir, conf, batch_size, map_size, thread_num, url, sc_type):
    shuffler = Shuffler(file_path_list, log_dir, conf, thread_num, batch_size, map_size, url, sc_type)
    if shuffler.valid() is not None:
        print 'shuffler is invalid'
    else:
        shuffler.run()

def warn(msg):
    if msg == '':
        return
    IPPORT = ['emp01.baidu.com:15003','emp02.baidu.com:15003']
    cmd_pre = 'gsmsend %s'%(' '.join(['-s %s'%ip for ip in IPPORT]))
    for phone in msg_to_list:
        phone = phone.strip()
        cmd = u'%s %s@"%s"'%(cmd_pre, phone, msg)
        os.system(cmd.encode("gbk"))

def start(file_path_list, log_dir, conf, batch_size, map_size, thread_num, url, sc_type):
    """
    start to shuffle data to sc
    """
    global fail_material
    #第一次灌库
    shuffle_once(file_path_list, log_dir, conf, batch_size, map_size, thread_num, url, sc_type)
    
    fail_log_dir = log_dir.rstrip('/') + '/fail_log'
    print fail_log_dir
    #将错误数据重新灌库
    print "shuffle fail materials..."
    for i in range(0,3):
        print "re shuffle: ",i
        collector = DataCollector([fail_log_dir], fail_material)
        if collector.check_dirs() is not None:
            collector.collect()
            collector.close()
            shuffle_once([fail_material], log_dir, conf, batch_size, map_size, thread_num, url, sc_type)
        else:
            print "no fail materials..."
            collector.close()
            break

    warn_msg = ''
    for dir in [fail_log_dir]:
        file_names = os.listdir(dir)
        for file in file_names:
            if os.path.getsize(dir.rstrip('/')+'/'+file) != 0:
                warn_msg += (dir+" still has fail materials...\n")
    print warn_msg
    #warn(warn_msg)

def upload_files(file_path_list):
    file_list = []
    files = open(file_path_list)
    for item in files:
        item = item.replace('\n', '')
        if (os.path.isfile(item)):
            file_list.append(item)
        else:
            print "err file: ",item
    print "file list: ",file_list
    files.close()
    return file_list

def mutli_shuffle(args):
    """
    interface
    """
    file_list = upload_files(args.file_path_list)
    url = 'http://'+args.endpoint
    if args.sc_type == 'new':
        url = url + '/v2/'+args.engineName+'/document/direct?businessId='+args.businessId+'&apiKey='+args.apiKey
    else:
        url = url + '/v1/'+args.engineName+'/document/batch?userId='+args.userId
    start(file_list, args.log_dir, args.log_conf, args.batch_size, args.map_size, args.thread_num, url, args.sc_type)

def main():
    parser = argparse.ArgumentParser();
    parser.add_argument('file_path_list', type=str);
    parser.add_argument('--thread_num',type=int, default=10)
    parser.add_argument('--batch_size',type=int, default=1)
    parser.add_argument('--map_size',type=int, default=10000)
    parser.add_argument('--log_dir',type=str, help='log_dir default=./cq02/log/', default='./cq02/log/')
    parser.add_argument('--log_conf',type=str, help='log_conf default=./conf/cq02_logging.conf/', default='./conf/cq02_logging.conf')
    parser.add_argument('--endpoint',type=str, help='endpoint', default='10.26.26.120:8081')
    parser.add_argument('--engineName',type=str, help='engineName', default='K12_cq02')
    parser.add_argument('--sc_type',type=str, help='sc_type', default='new')
    parser.add_argument('--businessId',type=str, help='businessId', default='e6d3bc8bc2a54f5f8e653d97495a42e1')
    parser.add_argument('--apiKey',type=str, help='apiKey', default='ZWUwNDg4ZjAxMzE2')
    parser.add_argument('--userId',type=str, help='userId', default='qsearch')
    args = parser.parse_args();
    print args
    mutli_shuffle(args)

main()
