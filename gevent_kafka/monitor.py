# Copyright 2013 Edgeware AB.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Zookeeper monitoring utilities."""

from functools import partial
import os
import json
from kazoo.protocol.states import EventType


def zkmonitor(kazoo, path, into, watch=None, factory=json.loads):
    """A ZooKeeper monitor.

    Keeps the given dict (into) updated with changes in the children
    of a given node (path). Optionally adds a callback (watch) and a
    'factory' function (factory) that gets run on the child data upon
    changes.
    """

    def update_child(child, data, stat, event=None):
        try:
            if event and event.type == EventType.DELETED:
                into.pop(child, None)
                return False
            into[child] = factory(data)
        finally:
            if watch:
                watch()

    into.clear()
    kazoo.ensure_path(path)

    @kazoo.ChildrenWatch(path)
    def get_children(children):
        for child in children:
            child_path = os.path.join(path, child)
            if child not in into.keys():
                kazoo.DataWatch(child_path, partial(update_child, child))

        # # Make sure we clean up any children that shouldn't be there
        for child in set(into.keys()) - set(children):
            del into[child]
