#!/usr/bin/python
# encoding: utf-8
import os

PYTHON = '/home/zhengxiaotian/.jumbo/bin/python'
args_all = {
    'cq02' : {
            'thread_num' : 20,
            'batch_size' : 20,
            'map_size' : 10000,
            'log_dir' : './cq02/log/',
            'log_conf' : './conf/cq02_logging.conf',
            'sc_type' : 'new',
            'endpoint' : '0.0.0.0:0000',
            'engineName' : 'K12_cq02',
            'businessId' : 'xxxxx',
            'apiKey' : 'xxxx',
            'userId' : 'none'
            },
    'gzhxy' : {
            'thread_num' : 20,
            'batch_size' : 20,
            'map_size' : 10000,
            'log_dir' : './gzhxy/log/',
            'log_conf' : './conf/gzhxy_logging.conf',
            'sc_type' : 'new',
            'endpoint' : '0.0.0.0:0000',
            'engineName' : 'K12_gzhxy',
            'businessId' : 'xxxxx',
            'apiKey' : 'xxxx',
            'userId' : 'none'
            },
    'bj_old' : {
            'thread_num' : 20,
            'batch_size' : 1,
            'map_size' : 10000,
            'log_dir' : './bj_old/log/',
            'log_conf' : './conf/bj_old_logging.conf',
            'sc_type' : 'old',
            'endpoint' : '0.0.0.0:0000',
            'engineName' : 'K12',
            'businessId' : 'xxxxx',
            'apiKey' : 'xxxx',
            'userId' : 'none'
            },
    'nj_old' : {
            'thread_num' : 20,
            'batch_size' : 1,
            'map_size' : 10000,
            'log_dir' : './nj_old/log/',
            'log_conf' : './conf/nj_old_logging.conf',
            'sc_type' : 'old',
            'endpoint' : '0.0.0.0:0000',
            'engineName' : 'K12',
            'businessId' : 'xxxxx',
            'apiKey' : 'xxxx',
            'userId' : 'none'
            }
}

def start_shuffle():
    for k,v in args_all.items():
        #if k != 'nj_old':
        #    continue
        cmd = PYTHON + ' shuffle_multiprocess.py file_list.txt' +\
            ' --thread_num='+str(v['thread_num'])+\
            ' --batch_size='+str(v['batch_size'])+\
            ' --map_size='+str(v['map_size'])+\
            ' --log_dir='+v['log_dir']+\
            ' --log_conf='+v['log_conf']+\
            ' --sc_type='+v['sc_type']+\
            ' --endpoint='+v['endpoint']+\
            ' --engineName='+v['engineName']+\
            ' --businessId='+v['businessId']+\
            ' --apiKey='+v['apiKey']+\
            ' --userId='+v['userId']
        os.system(cmd)
        print k,'done...'
        #x = raw_input('continue next...')

def main():
    start_shuffle()

if __name__ == '__main__':
    main()
