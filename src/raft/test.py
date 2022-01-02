#!/usr/bin/env python
# coding=utf-8

import os
from multiprocessing import Process
from time import sleep

def exe(cmd):
    print('will exec: {}'.format(cmd))
    os.system(cmd)

ls = []

for i in range(4):
    s, t = i * 500, i * 500 + 499
    cmd = 'for i in {' + str(s) + '..' + str(t) + '}; do ./1test.sh Test $i; done'
    p = Process(target=exe, args=(cmd,))
    ls.append(p)
    p.start()
    sleep(40)

for p in ls:
    p.join()
