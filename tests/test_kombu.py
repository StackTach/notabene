# Copyright 2014 - Dark Secret Software Inc.
# All Rights Reserved.

from contextlib import nested
import unittest

import mock
from notabene import kombu_driver


class MyException(Exception):
    """Don't use Exception in tests."""
    pass


class TestKombuDriver(unittest.TestCase):
    def setUp(self):
        self.process_patchers = []
        self.logger = mock.Mock()

    def tearDown(self):
        for p in reversed(self.process_patchers):
            p.stop()

    def _create_worker(self, callback, connection, deployment, topics):
        self.process_patchers.append(mock.patch("signal.signal").start())
        return kombu_driver.Worker(callback, "worker", connection, deployment, False,
                 {}, "exchange", topics, self.logger)


