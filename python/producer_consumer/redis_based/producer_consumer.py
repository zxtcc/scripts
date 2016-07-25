#!/usr/bin/python
# encoding: utf-8

import os
import sys
import json
import redis
import threading
import argparse
import logging
import datetime
import time

class Producer(threading.Thread):
    def __init__(self, file_list, r, redis_list_name, batch_size, quit_symbol, thread_num, pipe_size=10):
        threading.Thread.__init__(self, name='producer')
        self.file_list = file_list
        self.redis = r
        self.pipe = r.pipeline()
        self.redis_list = redis_list_name
        self.redis.delete(self.redis_list)
        self.batch_size = batch_size
        self.quit_symbol = quit_symbol
        self.thread_num = thread_num

        self.pipe_cur_size = 0
        self.pipe_size = pipe_size
        self.total = 0
        pass
        
    def run(self):
        for file in self.file_list:
            if not os.path.exists(file):
                continue
            material_count = 0
            material_list = []
            for line in open(file):
                material_count += 1
                self.total += 1
                print 'file.material: ',material_count
                try:
                    material_list.append(json.loads(line))
                except Exception as e:
                    print 'not json',e
                    continue
                if len(material_list) >= self.batch_size:
                    self._send(json.dumps(material_list))
                    material_list = []
            if len(material_list) > 0:
                self._send(json.dumps(material_list))

            self._execute()
        self._exit()
        pass

    def _send(self, materials):
        self.pipe.lpush(self.redis_list, materials)
        self.pipe_cur_size += 1
        if self.pipe_cur_size >= self.pipe_size:
            self._execute()
        pass

    def _execute(self):
        self.pipe.execute()
        pass

    def _exit(self):
        for i in xrange(self.thread_num):
            self.pipe.lpush(self.redis_list, self.quit_symbol)
        self._execute()
        pass

class Consumer(threading.Thread):
    def __init__(self, t_name, r, redis_list_name, quit_symbol, logger):
        threading.Thread.__init__(self, name=t_name)
        self.redis = r
        self.redis_list = redis_list_name
        self.logger = logger
        self.quit_symbol = quit_symbol
        pass

    def run(self):
        while(1):
            list_name, val = self.redis.brpop(self.redis_list)
            if val == self.quit_symbol:
                break
            self._debug(val)
        print self.name+' quit...'
        pass

    def _debug(self, val):
        if self.name.split('.')[1] == '1':
            time.sleep(5)
        else:
            time.sleep(0.05)
        self.logger.debug(val)
        pass

def init_logger():
    logging.basicConfig(level=logging.DEBUG,
            format='%(message)s',
            filename='consumer.log',
            filemode='w+')
    pass

def start(args):
    start_time = datetime.datetime.now()
    init_logger()
    redis_pool = redis.ConnectionPool(host=args.redis_host, port=args.redis_port, db=0)
    r = redis.StrictRedis(connection_pool=redis_pool)
    file_list = []
    for file in open(args.file_list):
        file_list.append(file.rstrip('\n'))
    producer = Producer(file_list, r, args.redis_list_name, args.batch_size, args.quit_symbol, args.thread_num)
    producer.start()
    consumers = []
    for i in xrange(args.thread_num):
        consumer = Consumer('consumer'+'.'+str(i), redis.StrictRedis(connection_pool=redis_pool), args.redis_list_name, args.quit_symbol, logging.getLogger())
        consumer.start()
        consumers.append(consumer)
    producer.join()
    for consumer in consumers:
        consumer.join()
    end_time = datetime.datetime.now()
    start_to_end = end_time - start_time
    used_s = start_to_end.seconds + start_to_end.microseconds/1000000
    print 'used :',used_s,'s'
    print 'total: ',producer.total
    if used_s != 0:
        print 'qps: ',(producer.total*1.0)/(used_s*1.0)
    print 'done'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-redis_host', type=str, help='redis host,default=localhost', default='localhost')
    parser.add_argument('-redis_port', type=str, help='redis port,default=6379',default=6379)
    parser.add_argument('-file_list', type=str, help='all input file name in this file')
    parser.add_argument('-batch_size', type=int, help='batch the materials, default=20', default=20)
    parser.add_argument('-thread_num', type=int, help='thread num, default=10', default=10)
    parser.add_argument('-quit_symbol', type=str, help='quit_symbol, default=quit', default='quit')
    parser.add_argument('-redis_list_name', type=str, help='redis_list_name, default=producer_consumer', default='producer_consumer')

    args = parser.parse_args()
    start(args)
    pass
            
if __name__ == '__main__':
    main()
        
