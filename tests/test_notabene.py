# Copyright 2014 - Dark Secret Software Inc.
# All Rights Reserved.

import unittest

import mock
from notabene import notabene


class TestNotaBene(unittest.TestCase):
    def setUp(self):
        self.notabene = notabene.NotaBene()

    def test_spawn_consumer(self):
        with mock.patch('notabene.notabene.NotaBeneProcess') as m:
            self.notabene._spawn_consumer(1, 2, 3)
            self.assertTrue(m.run.called_once)

    def test_
