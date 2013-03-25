import os
import logging
import json

from gevent_kafka import consumer, producer
from gevent_zookeeper.framework import ZookeeperFramework
from kazoo.client import KazooClient
from kazoo.handlers.gevent import SequentialGeventHandler
import gevent


def zkmonitor(kazoo, path, into, watch):
    def child_changed(e):
        print "child changed"
        print e
        watch()
        get_child(os.path.basename(e.path))

    def get_child(child):
        child_path = os.path.join(path, child)
        data, stat = kazoo.get(child_path, watch=child_changed)
        print "got child"
        print data
        into[child] = json.loads(data)

    def get_children(e=None):
        children = kazoo.get_children(path, watch=get_children)
        for child in children:
            get_child(child)

    get_children()

logging.basicConfig(level=logging.DEBUG)

kazoo = KazooClient(handler=SequentialGeventHandler())
kazoo.start()


store = {}

def watch():
    print "watch!"
    print store


parent = '/my-path'

if not kazoo.exists(parent):
    kazoo.create(parent, ephemeral=False)

zkmonitor(kazoo, parent, store, watch)

while True:
    gevent.sleep(10)
