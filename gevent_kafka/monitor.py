import os
import json
from kazoo.exceptions import NoNodeError


def zkmonitor(kazoo, path, into, watch=None, factory=json.loads):
    def child_changed(e):
        if watch:
            watch()
        get_child(os.path.basename(e.path))

    def get_child(child):
        child_path = os.path.join(path, child)
        try:
            data, stat = kazoo.get(child_path, watch=child_changed)
        except NoNodeError:
            if child in into:
                del into[child]
        else:
            into[child] = factory(data)

    def get_children(e=None):
        children = kazoo.get_children(path, get_children)
        for child in children:
            get_child(child)

    kazoo.ensure_path(path)
    get_children()
