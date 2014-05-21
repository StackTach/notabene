# Copyright 2014 - Dark Secret Software Inc.
# All Rights Reserved.

from contextlib import nested
import unittest

import anyjson
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
        return kombu_driver.Worker(callback, "worker", connection, deployment,
                                   False, {}, "exchange", topics, self.logger)

    def test_create_exchange(self):
        kombu_driver.create_exchange("name", "topic")

    def test_create_queue(self):
        e = kombu_driver.create_exchange("name", "topic")
        kombu_driver.create_queue("name", e, "routing_key")

    def test_get_consumers(self):
        consumer_class = mock.Mock
        topics = [{'queue': "queue 1", "routing_key": "key 1"},
                  {'queue': "queue 2", "routing_key": "key 2"}]
        w = self._create_worker(None, None, None, topics)

        # This call normally comes from within kombu ...
        c = w.get_consumers(consumer_class, None)
        self.assertEqual(1, len(c))
        args = c[0]
        self.assertEqual(2, len(args.queues))
        self.assertEqual(w._on_notification, args.callbacks[0])

    def test_process(self):
        callback = mock.Mock()
        w = self._create_worker(callback, None, "deployment", None)
        message = mock.Mock()
        message.delivery_info = {'routing_key': "the key"}
        message.body = anyjson.dumps({'payload': 123})
        w._process(message)

        self.assertTrue(callback.on_event.called_once)
        args = callback.on_event.call_args[0]
        self.assertEqual(args[0], "deployment")
        self.assertEqual(args[1], 'the key')
        self.assertEqual(args[2], {'payload': 123})
        self.assertEqual(args[3], "exchange")

    def test_on_notification_fails(self):
        w = self._create_worker(None, None, "deployment", None)
        with mock.patch.object(w, "_process") as process:
            process.side_effect = MyException()
            self.assertRaises(MyException, w._on_notification,
                              '["body"]', "message")
            self.assertTrue(self.logger.debug.called_once)

    def test_on_notification(self):
        w = self._create_worker(None, None, "deployment", None)
        with mock.patch.object(w, "_process") as process:
            w._on_notification('["body"]', "message")
            self.assertFalse(self.logger.debug.called)

    def test_shutdown(self):
        callback = mock.Mock()
        w = self._create_worker(callback, None, None, None)
        w._shutdown(None)
        self.assertTrue(callback.shutting_down.called_once)


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
