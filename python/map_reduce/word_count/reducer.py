#!/usr/bin/python
# encoding: utf-8

import sys
cur_word = None
cur_count = 0

for line in sys.stdin:
    line = line.strip()
    word, count= line.split('\t')
    count = int(count)

    if cur_word == word:
        cur_count += count
    else:
        if cur_count:
            print '%s\t%s' % (cur_word, cur_count)
        cur_word = word
        cur_count = count

if cur_word:
    print '%s\t%s' % (cur_word, cur_count)
    
    
