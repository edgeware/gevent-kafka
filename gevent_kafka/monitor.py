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


import os
import json
from kazoo.exceptions import NoNodeError


def zkmonitor(kazoo, path, into, watch=None, factory=json.loads):
    """A ZooKeeper monitor.

    Keeps the gicen dict (into) updated with changes in the children
    of a given node (path). Optionally adds a callback (watch) and a
    'factory' function (factory) that gets run on the child data after a
    change.
    """

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
