# Copyright 2014 - Dark Secret Software Inc.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import multiprocessing
import time
import signal
import sys

import anyjson

import queued_log


class NoopHandler(object):
    def __init__(self, process):
        self.process = process

    def on_event(self, deployment, args, asJson, exchange):
        print "deployment: %s, args: %s, payload: %s, exchange: %s" % (
            deployment, args, asJson, exchange)

    def shutting_down(self):
        self.shutdown_soon = True


class NotaBeneProcess(object):

    def __init__(self, deployment_config, exchange, log_manager, driver, 
                 callback_class):
        self.deployment_config = deployment_config
        self.exchange = exchange
        self.shutdown_soon = False
        self.driver = driver
        self.callback_class = callback_class
        self.log_manager = log_manager

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

    def _continue_running(self):
        return not self.shutdown_soon

    def _exit_or_sleep(self, exit=False):
        if exit:
            sys.exit(1)
        time.sleep(5)

    def run(self):
        name = self.deployment_config['name']
        exit_on_exception = self.deployment_config.get('exit_on_exception',
                                                       False)
        deployment_id = self.deployment_config['id']  # Mandatory.
        self.logger = self.log_manager.get_logger("worker", is_parent=False)
        callback = self.callback_class(self)

        # continue_running() is used for testing
        while self._continue_running():
            self.logger.debug("Processing on '%s %s'" % (name, self.exchange))
            try:
                # Block in driver event consumer until we 
                # exit gracefully or error out.
                self.driver(callback, name, deployment_id, 
                            self.deployment_config, self.exchange,
                            self.logger)
            except Exception as e:
                self.logger.exception(
                    "name=%s, exchange=%s, exception=%s. "
                    "Reconnecting in 5s" % (name, self.exchange, e))
                self._exit_or_sleep(exit_on_exception)
        self.logger.debug("Completed processing on '%s %s'" % (name, self.exchange))


class NotaBene(object):
    def __init__(self, driver, callback_class):
        self.processes = []
        self.log_listener = None
        self.driver = driver
        self.callback_class = callback_class

    def _kill_time(self, signal, frame):
        print "dying ..."
        for process in self.processes:
            process.terminate()
        print "rose"
        for process in self.processes:
            process.join()
        if log_listener:
            log_listener.end()
        print "bud"

    def _init_logging_queue(self):
        log_listener = queued_log.LogListener("notabene", "worker")
        log_listener.start()

    def _spawn_consumer(self, deployment, exchange):
        n = NotaBeneProcess(deployment, exchange, self.log_listener)
        n.run()

    def spawn_consumers(self, config):
        self._init_logging_queue()
        for deployment in config.get('deployments', []):
            if deployment.get('enabled', True):
                for exchange in deployment.get('topics',{}).keys():
                    process = multiprocessing.Process(
                                  target=self._spawn_consumer,
                                  args=(self.log_listener, deployment, 
                                        exchange, self.driver, 
                                        self.callback_class))
                    process.daemon = True
                    process.start()
                    self.processes.append(process)
        signal.signal(signal.SIGINT, self._kill_time)
        signal.signal(signal.SIGTERM, self._kill_time)
        signal.pause()
