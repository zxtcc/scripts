#!/usr/bin/env python
# coding: utf-8
import argparse
import json
import sys
import subprocess
import time
import os
import datetime
import urllib2
import urllib
import random,threading
from Queue import Queue

endpoint="0.0.0.0:0000"
engineName="K12_gzhxy"
businessId="xxxx"
apiKey="xxxx"
url = 'http://'+endpoint+'/xx/'+engineName+'/document/direct?businessId='+businessId+'&apiKey='+apiKey
log_dir="./log/"
fail_material = './fail_material'

class Producer(threading.Thread):
    def __init__(self, t_name, queue, file_list, batch):
        threading.Thread.__init__(self,name=t_name)
        self.data=queue
        self.file_list=file_list
        self.batch=batch
        self.log=open(log_dir+self.name+".log", "w+") #发生错误后，数据会被log，以便之后再补灌
    def changeFileList(self, file_list):
        self.file_list = file_list
    def run(self):
        t0 = datetime.datetime.now()
        total = 0
        for file in self.file_list:
            f = open(file)
            now_file_material = os.path.split(file)[1].split('_')[0]+".material: "
            material_count = 0
            lineno = 0
            data_list = []
            for line in f:
                material_count += 1
                total += 1
                print now_file_material, material_count
                #print line
                lineno += 1
                try:
                    json_obj = json.loads(line)
                    data_list.append(json_obj)
                except Exception as e:
                    print "producer run error: ",e
                    self.log_err_data(line, data_list)
                    data_list = []
                    lineno = 0
                    continue
                if lineno >= self.batch:
                    req = json.dumps(data_list)
                    self.push_data(req)
                    data_list = []
                    lineno = 0
            if len(data_list) > 0:
                req = json.dumps(data_list)
                self.push_data(req)
            f.close()
        t1 = datetime.datetime.now()
        tt = t1 - t0
        used_s = (tt.seconds * 1000 + tt.microseconds / 1000)/1000
        print "used: ",used_s,'s'
        print "qps: ",(total*1.0)/(used_s*1.0)
        self.close()
    def close(self):
        self.log.close()
    def push_data(self, data):
        self.data.put(data) #将数据存入队列
    def log_err_data(self, line, data_list):
        self.log.write(line.rstrip('\n') + '\n')
        for item in data_list:
            try:
                self.log.write(json.dumps(item) + '\n')
            except Exception as e:
                continue

class Consumer(threading.Thread):
    def __init__(self,t_name,queue,url):
        threading.Thread.__init__(self,name=t_name)
        self.data=queue
        self.url=url
        if not os.path.exists(log_dir+'./http_fail_log'):
            os.mkdir(log_dir+'./http_fail_log')
        if not os.path.exists(log_dir+'./sc_fail_log'):
            os.mkdir(log_dir+'./sc_fail_log')
        self.http_fail_log=open(log_dir+"./http_fail_log/"+self.name+".http.fail.log", "w+")
        self.sc_fail_log=open(log_dir+"./sc_fail_log/"+self.name+".sc.fail.log", "w+")
        self.log=open(log_dir+self.name+".log", "w+")
    def run(self):
        while 1:
            try:
                val = self.data.get(1,8) #get(self, block=True, timeout=None) ,1就是阻塞等待,8是超时8秒
                output = val
                #print output
                #self.send_req(val, self.url)
                #self.log.write(output+'\n')
                self.debug(output)
                #time.sleep(0.1)
            except Exception as e:   #等待输入，超过8秒 就报异常
                print self.name+'--quit',e
                time.sleep(1)
                break
            self.data.task_done()
        self.close()
    def close(self):
        self.http_fail_log.close()
        self.sc_fail_log.close()
        self.log.close()
    def debug(self, req):
        """
        debug type ,dont send req to remote
        """
        if self.name == 'consumer.5':
            self.http_fail_log.write(req+'\t'+'http fail'+'\n')
        if self.name == 'consumer.7':
            self.sc_fail_log.write(req+'\t'+'failurecount=1'+'\n')

    def send_req(self, req, requrl):
        header = {"Content-type":"application/json"}
        try:
            req_ = urllib2.Request(url=requrl,data=req,headers=header)
            res = urllib2.urlopen(req_, timeout=3)
            res = res.read()
            self.log.write(res+'\n')
        except urllib2.URLError, e: #处理超时等http错误
            self.http_fail_log.write(req+'\t'+e.read()+'\n')
            return -1
        if self.check_res(res) is not None: #sc返回是否异常
            self.sc_fail_log.write(req+'\t'+res+'\n')
    def check_res(self,res):
        """
        res的返回是：{"totalCount":10,"successCount":0,"failureCount":0,"reasons":{}}
        """
        try:
            res = json.loads(res)
        except Exception as e:
            return "load json failed"
        if "failureCount" not in res or res["failureCount"] != 0:
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
    
def shuffle_once(file_path_list, batch, queue_size, thread_num):
    global url
    global log_dir
    queue = Queue(maxsize=queue_size)
    producer = Producer('producer', queue, file_path_list, batch)
    consumers = []
    for i in range(0, thread_num):
        consumer = Consumer('consumer.'+str(i), queue, url)
        consumers.append(consumer)
    producer.start()
    for cns in consumers:
        cns.start()
    producer.join()
    for cns in consumers:
        cns.join()

def start(file_path_list, batch, queue_size, thread_num):
    """
    start to shuffle data to sc
    """
    global fail_material
    #第一次灌库
    shuffle_once(file_path_list, batch, queue_size, thread_num)
    
    #将错误数据重新灌库
    print "shuffle fail materials..."
    for i in range(0,1):
        print "re shuffle: ",i
        collector = DataCollector([log_dir+'http_fail_log/', log_dir+'sc_fail_log/'], fail_material)
        if collector.check_dirs() is not None:
            collector.collect()
            collector.close()
            shuffle_once([fail_material], batch, queue_size, thread_num)
        else:
            print "no fail materials..."
            collector.close()
            break

    for dir in [log_dir+'http_fail_log/', log_dir+'sc_fail_log/']:
        file_names = os.listdir(dir)
        for file in file_names:
            if os.path.getsize(dir.rstrip('/')+'/'+file) != 0:
                print dir,"still has fail materials..."

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

def mutli_shuffle(file_path_file, batch, queue_size, thread_num):
    """
    interface
    """
    file_list = upload_files(file_path_file)
    start(file_list, batch, queue_size, thread_num)

def setEnv():
    global log_dir
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

def main():
    global thread_num
    parser = argparse.ArgumentParser();
    parser.add_argument('file_path_list', type=str);
    parser.add_argument('--thread_num',type=int, default=10)
    parser.add_argument('--batch',type=int, default=1)
    parser.add_argument('--queue_size',type=int, default=10)
    args = parser.parse_args();
    thread_num = args.thread_num

    setEnv()
    mutli_shuffle(args.file_path_list, args.batch, args.queue_size, args.thread_num)

main()
