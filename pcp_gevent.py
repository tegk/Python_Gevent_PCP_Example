#!/usr/bin/python
# -*- coding: utf-8 -*-

# Python Version: 2.7

import sys
import os
import signal
import random

import gevent
from gevent.queue import *
from gevent.event import Event
import gevent.monkey


def worker(i):
    try:
        while not evt.is_set():
            t = q.get(block=True, timeout=5)
            print 'Worker', i, 'works on job', t
            q_status.put(t)  # send status
            gevent.sleep(random.uniform(2.50, 0.6))
        if evt.is_set():
            send_sentinals()
    except:
        send_sentinals()
        print 'Worker', i, 'processed all jobs'


def loader():
    for i in xrange(1000):
        if evt.is_set():
            break
        else:
            q.put(i)
            print 'Loader produced job', i
            gevent.sleep(random.uniform(0.1, 0.02))


def writer():
    sen_count = workers
    while True:
        t = q_status.get(block=True)
        if t == 'SENTINAL':
            sen_count -= 1
            if sen_count < 1:
                print 'Writer received all Jobs. The last Job was', tt
                break
        else:
            tt = t


def send_sentinals():
    q_status.put('SENTINAL')


def handler(signum, frame):
    print '[INFO]Shutting down gracefully.'
    evt.set()


def asynchronous():
    threads = []
    threads.append(gevent.spawn(loader))
    for i in xrange(0, workers):
        threads.append(gevent.spawn(worker, i))
    threads.append(gevent.spawn(writer))
    gevent.joinall(threads)
    evt.set()  # cleaning up


workers = 10
evt = Event()
signal.signal(signal.SIGINT, handler)
q = gevent.queue.Queue(maxsize=5000)  # loader
q_valid = gevent.queue.Queue()  # valid
q_status = gevent.queue.Queue()  # status
asynchronous()

			
