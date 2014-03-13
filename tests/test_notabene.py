# Copyright 2014 - Dark Secret Software Inc.
# All Rights Reserved.

from contextlib import nested
import unittest

import mock
from notabene import notabene


class MyException(Exception):
    """Don't use Exception in tests."""
    pass


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


class TestCallback(object):
    def __init__(self, process):
        self.process = process
        self.events = []
        self.shutdown_soon = False

    def on_event(self, deployment, args, asJson, exchange):
        self.events.append((deployment, args, asJson, exchange))

    def shutting_down(self):
        self.shutdown_soon = True


class TestNotaBeneProcess(unittest.TestCase):
    def setUp(self):
        self.process_patchers = []

    def tearDown(self):
        for p in reversed(self.process_patchers):
            p.stop()

    def _create_notabene_process(self, config, exchange, log, driver, 
                                 callback_class):
        self.process_patchers.append(mock.patch("signal.signal").start())
        return notabene.NotaBeneProcess(config, exchange, log, driver, 
                                           callback_class)

    def test_continue_running(self):
        self.assertTrue(
            self._create_notabene_process(None, None, None, None, None).
                _continue_running())

    def test_exit_or_sleep_sleep(self):
        p = self._create_notabene_process(None, None, None, None, None)
        with mock.patch("sys.exit") as exit:
            with mock.patch("time.sleep") as sleep:
                p._exit_or_sleep(exit=False)
                self.assertTrue(sleep.called_once)
                self.assertFalse(exit.called)

    def test_exit_or_sleep_exit(self):
        p = self._create_notabene_process(None, None, None, None, None)
        with mock.patch("sys.exit") as exit:
            with mock.patch("time.sleep") as sleep:
                # Generate an exception since sys.exit stops program flow. 
                exit.side_effect = MyException()
                self.assertRaises(MyException, p._exit_or_sleep, exit=True)
                self.assertFalse(sleep.called)

    def test_run_happy_day(self):
        config = {   
            "id": 1,
            "name": "my stack",
            "topics": {
                "nova": []
            }
        }

        driver = mock.Mock()
        log_manager = mock.Mock()
        p = self._create_notabene_process(config, "exchange", log_manager, 
                                          driver, TestCallback)
        with mock.patch.object(p, "_continue_running") as run:
            run.side_effect = [True, False]  # loop once. 
            p.run()

    def test_run_driver_failure(self):
        config = {   
            "id": 1,
            "name": "my stack",
            "topics": {
                "nova": []
            }
        }

        driver = mock.Mock()
        driver.side_effect = MyException()
        log_manager = mock.Mock()
        p = self._create_notabene_process(config, "exchange", log_manager, 
                                          driver, TestCallback)
        with mock.patch.object(p, "_continue_running") as run:
            with mock.patch.object(p, "_exit_or_sleep") as exit:
                run.side_effect = [True, False]  # loop once. 
                p.run()
                self.assertTrue(exit.called_once)
