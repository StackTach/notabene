# Copyright 2014 - Dark Secret Software Inc.
# All Rights Reserved.

from contextlib import nested
import unittest

import mock
from notabene import notabene


class TestNotaBene(unittest.TestCase):
    def setUp(self):
        self.notabene = notabene.NotaBene(None, None)

    def test_spawn_consumer(self):
        with mock.patch('notabene.notabene.NotaBeneProcess') as m:
            self.notabene._spawn_consumer(1, 2)
            self.assertTrue(m.run.called_once)

    def test_spawn_consumers_empty(self):
        config = {}
        with nested(
                mock.patch.object(self.notabene, "_init_logging_queue"),
                mock.patch("signal.signal"),
                mock.patch("signal.pause"),
            ):
            with mock.patch('multiprocessing.Process') as p:
                self.notabene.spawn_consumers(config)
                self.assertFalse(p.spawn.called)

    def test_spawn_consumers_some_enabled(self):
        config = {"deployments": [
            {   
                # enabled implied True
                "topics": {
                    "nova": []
                }
            },
            {
                "enabled": False,
                "topics": {
                    "nova": []
                }
            }]
        }
        with nested(
                mock.patch.object(self.notabene, "_init_logging_queue"),
                mock.patch("signal.signal"),
                mock.patch("signal.pause"),
        ):
            with nested(
                mock.patch('multiprocessing.Process'),
                mock.patch('signal.pause')
            ) as (p, pause):
                self.notabene.spawn_consumers(config)
                self.assertTrue(p.spawn.called_once)
                self.assertTrue(pause.pause.called_once)


class TestNotaBeneProcess(unittest.TestCase):
    def setUp(self):
        self.process_patchers = []

    def tearDown(self):
        for p in reverse(self.process_patchers):
            p.stop()

    def _create_notabene_process(config, exchange, log, driver, callback_class):
        self.process_patchers.append(patch("signal.signal").start())
        return notabene.NotaBeneProcess(config, exchange, log, driver, 
                                           callback_class)

