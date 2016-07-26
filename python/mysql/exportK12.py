#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
mysql test:
mysql -h0.0.0.0.0 -P0000 -uuser_name -ppassword -A mysql_db_name --default-character-set=utf8 -e "select * from table"

"""

import sys
import os
import json
from ProductDB import *

K12_TITLE_SCHEMA = [
    'pic_size_flag',
    'answer',
    'answer_text',
    'answer_url',
    #'create_time',
    #'leaf_point'
    'is_del',
    'level_id',
    'map_title_id',
    #'samequestions',
    'source_id',
    #'stage_id',
    #'subject_id',
    'title',
    'title_id',
    'title_pic',
    #'title_status',
    'title_text',
    'title_type_id',
    'title_url',
    #'update_time',
    ]
step = 5000

def exportData():
    out = open("./mfg", "a");
    pdb = ProductDB("0.0.0.0", 0000, "user_name", "password", "db_name", 1)
    pdb.connect()

    sqlStr = 'select count(1) from k12_title';
    count = int(pdb.executeOne(sqlStr))
    print count
    start = 0

    selectFields = ','.join('t.'+k for k in K12_TITLE_SCHEMA);
    
    for start in range(0, count, step):
        length = min(count - start, step)

        #sqlStr = "SELECT %s,p.point_name AS leaf_point FROM "\
        #    "k12_title AS t LEFT JOIN k12_point_title_relation as pt, k12_leaf_point as p "\
        #    "where t.title_id=pt.title_id and p.leaf_point_id=pt.leaf_point_id "\
        #    "limit %d, %d" % (selectFields, start, length);

        #sqlStr = "SELECT %s FROM k12_title as t limit %d, %d" % (selectFields, start, length);

        sqlStr = "select %s,p.point_name as leaf_point from (select * from k12_title limit %d, %d)"\
            "as t left join k12_point_title_relation as pt on t.title_id ="\
            "pt.title_id left join k12_leaf_point as p on pt.leaf_point_id ="\
            "p.leaf_point_id where pt.category != 2" % (selectFields, start, length)

        pdb.executeAll(sqlStr)
        rows = pdb.executeAllUnbuffer(sqlStr)

        for row in rows:
            row["seg_info"] = "seg_info_weight";
            doc = {
                'action': 'add',
                'id': 'mfg' + str(row['title_id']),
                'fields': row,
            }
            #print json.dumps(doc)
            print doc['id']
            out.write(json.dumps(doc))
            out.write("\n")
    pdb.close()
def main():
    exportData()

if __name__ == '__main__':
    main()
