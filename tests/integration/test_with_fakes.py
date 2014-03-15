# Copyright 2014 - Dark Secret Software Inc.
# All Rights Reserved.

from contextlib import nested
import os
import shutil
import unittest

import mock
from notabene import notabene
from notabene import queued_log
import test_driver


class TestHandler(object):
    def __init__(self, process):
        self.process = process

    def on_event(self, deployment, routing_key, body, exchange):
        self.process.logger.debug(
            "deployment: %s, routing_key: %s, body: %s, exchange: %s" % 
                (deployment, routing_key, body, exchange))

    def shutting_down(self):
        self.process.shutdown_soon = True


class TestNotaBene(unittest.TestCase):
    def setUp(self):
        self.log_dir = "./logs"
        try:
            os.mkdir(self.log_dir)
        except OSError:
            pass

        self.log_manager = queued_log.LogManager("notabene", "worker",
                                    logger_location=self.log_dir)
        self.log_manager.start()
        self.notabene = notabene.NotaBene(test_driver.start_worker, 
                                          TestHandler, self.log_manager)

    def tearDown(self):
        #shutil.rmtree(self.log_dir)
        pass

    def test_happy_day(self):
        config = {"deployments": [
            {   
                "name": "test-config",
                "id": 1,
                "topics": {
                    "nova": [],
                    "glance": [],
                    "keystone": []
                },
            }],
        }
        self.notabene.spawn_consumers(config)
        # don't call wait_for_signal, we want to exit. 
        # Also, don't exit before the processes finish.
        self.notabene.graceful_shutdown()
