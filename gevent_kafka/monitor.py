import os
import json
from kazoo.exceptions import NoNodeError


def zkmonitor(kazoo, path, into, watch=None, factory=json.loads):
    def child_changed(e):
        print "child changed"
        print e
        if watch:
            watch()
        get_child(os.path.basename(e.path))

    def get_child(child):
        child_path = os.path.join(path, child)
        try:
            data, stat = kazoo.get(child_path, watch=child_changed)
        except NoNodeError:
            print "no node %s" % child_path
            if child in into:
                del into[child]
            else:
                print "Got event on child that does not exist in store or zookeeper"
        else:
            print "got child %s" % data
            into[child] = factory(data)

    def get_children(e=None):
        children = kazoo.get_children(path, get_children)
        for child in children:
            get_child(child)

    get_children()
