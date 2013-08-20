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

"""Unit tests for ZooKeeper monitor.

Note that these tests use the Kazoo test harness. Therefore they
require an installation of ZooKeeper and that the environment variable
"ZOOKEEPER_PATH" points to the location of the ZooKeeper JAR file,
e.g. "/usr/share/java".

See https://kazoo.readthedocs.org/en/latest/testing.html for more.
"""

import os
import time
import logging

import gevent
from kazoo.testing import KazooTestCase
from kazoo.handlers.gevent import SequentialGeventHandler
from mock import Mock

from gevent_kafka import monitor


def keep_trying(fun, exception=AssertionError, timeout=1.0, delay=0.1):
    """Keep running a function until it doesn't raise given exception(s),
    or until timeout. In the latter case, the last exception is raised."""
    t0 = time.time()
    while True:
        try:
            fun()
        except exception:
            if time.time() > (t0 + timeout):
                raise
            gevent.sleep(delay)
        else:
            break


class GeventKafkaMonitorTestCase(KazooTestCase):

    """Unit tests for the ZooKeeper monitor."""

    path = "/a/b"

    def setUp(self):
        self.data = {}
        KazooTestCase.setUp(self)
        logger = logging.getLogger("kazoo")
        logger.setLevel(logging.ERROR)
        self.client = self._get_client(handler=SequentialGeventHandler(),
                                       logger=logger)
        self.client.start()

    def test_zkmonitor_creates_path(self):
        """Check that the monitor creates the base node if needed."""
        self.assertFalse(self.client.exists(self.path))
        monitor.zkmonitor(self.client, self.path, {})
        self.assertTrue(self.client.exists(self.path))

    def test_zkmonitor_notices_children(self):
        """Check that new child nodes are noticed."""
        monitor.zkmonitor(self.client, self.path, self.data)
        self.client.create(os.path.join(self.path, "child1"),
                           '{"data": "foo"}')
        self.client.create(os.path.join(self.path, "child2"),
                           '{"data": "bar"}')
        keep_trying(lambda: self.assertEquals(self.data.get("child2"),
                                              dict(data="bar")))
        keep_trying(lambda: self.assertEquals(self.data.get("child1"),
                                              dict(data="foo")))

    def test_zkmonitor_updates_children(self):
        """Check that the monitor notices when a child is updated."""
        self.test_zkmonitor_notices_children()
        self.client.set(os.path.join(self.path, "child1"),
                        '{"some": "potato"}')
        keep_trying(lambda: self.assertEquals(self.data.get("child1"),
                                              dict(some="potato")))
        self.client.set(os.path.join(self.path, "child1"),
                        '{"some": "zucchini"}')
        keep_trying(lambda: self.assertEquals(self.data.get("child1"),
                                              dict(some="zucchini")))

    def test_zkmonitor_removes_children(self):
        """Check that the monitor removes children that aren't there."""
        self.test_zkmonitor_notices_children()
        self.client.delete(os.path.join(self.path, "child1"))
        keep_trying(lambda: self.assertTrue("child1" not in self.data))

    def test_zkmonitor_runs_watch(self):
        """Verify that the given watch callback is called on changes."""
        watch = Mock()
        monitor.zkmonitor(self.client, self.path, self.data, watch=watch)
        child_path = os.path.join(self.path, "child1")
        self.client.create(child_path, '{"some": "data"}')
        self.client.set(os.path.join(self.path, "child1"),
                        '{"some": "potato"}')
        keep_trying(watch.assert_called_once_with)

    def test_zkmonitor_uses_factory(self):
        """Check that the factory function is used on data."""
        factory = Mock(return_value=67)
        monitor.zkmonitor(self.client, self.path, self.data, factory=factory)
        child_path = os.path.join(self.path, "child1")
        self.client.create(child_path, "73")
        keep_trying(lambda: factory.assert_called_once_with("73"))
        self.assertEquals(self.data["child1"], 67)
