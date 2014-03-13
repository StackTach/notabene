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



class TestKombuDriverStartWorker(unittest.TestCase):
    def setUp(self):
        self.logger = mock.Mock()

    def test_start_worker_no_defaults(self):
        with mock.patch('kombu.connection.BrokerConnection'):
            with mock.patch('notabene.kombu_driver.Worker') as worker:
                config = {'rabbit_host': 'my host',
                          'rabbit_port': 1234,
                          'rabbit_userid': 'userid',
                          'rabbit_password': 'password',
                          'rabbit_virtual_host': 'root',
                          'durable_queue': False,
                          'queue_arguments': {1:2, 3:4},
                          'topics': {'topic_1': 10, 'topic_2': 20}
                          }

                logger = mock.Mock()
                callback = mock.Mock()
                kombu_driver.start_worker(callback, "my name", 1, config, 
                                          "topic_1", logger)

                self.assertTrue(worker.run.called_once)

                # Ignore the BrokerConnection (hard to mock since it's a 
                # context handler)
                args = list(worker.call_args[0])
                del args[2]
                self.assertEqual([callback, "my name", 1, False,
                                  {1:2, 3:4}, "topic_1", 10, logger],
                                 args)

    def test_start_worker_all_defaults(self):
        with mock.patch('kombu.connection.BrokerConnection'):
            with mock.patch('notabene.kombu_driver.Worker') as worker:
                config = {'topics': {'topic_1': 10, 'topic_2': 20}}

                logger = mock.Mock()
                callback = mock.Mock()
                kombu_driver.start_worker(callback, "my name", 1, config, 
                                          "topic_1", logger)

                self.assertTrue(worker.run.called_once)

                # Ignore the BrokerConnection (hard to mock since it's a 
                # context handler)
                args = list(worker.call_args[0])
                del args[2]
                self.assertEqual([callback, "my name", 1, True,
                                  {}, "topic_1", 10, logger],
                                 args)

    def test_start_worker_no_topic(self):
        with mock.patch('kombu.connection.BrokerConnection'):
            with mock.patch('notabene.kombu_driver.Worker') as worker:
                config = {}

                logger = mock.Mock()
                callback = mock.Mock()
                self.assertRaises(KeyError, kombu_driver.start_worker, 
                                 callback, "my name", 1, config, "topic_1", 
                                 logger)

                self.assertFalse(worker.run.called)


