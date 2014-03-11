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

import anyjson

import queued_log
import message_service



class NoopHandler(object):
    def on_event(self, deployment, args, asJson, exchange):
        print "deployment: %s, args: %s, payload: %s, exchange: %s" % (
            deployment, args, asJson, exchange)


class NotaBeneProcess(object):

    def __init__(self, deployment_config, exchange, log_manager):
        self.deployment_config = deployment_config
        self.exchange = exchange
        self.logger = log_manager.get_logger("worker", is_parent=False)
        self.shutdown_soon = False

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

    def _continue_running(self):
        return not self.hutdown_soon

    def _exit_or_sleep(self, exit=False):
        if exit:
            sys.exit(1)
        time.sleep(5)

    def run(self):
        name = self.deployment_config['name']
        host = self.deployment_config.get('rabbit_host', 'localhost')
        port = self.deployment_config.get('rabbit_port', 5672)
        user_id = self.deployment_config.get('rabbit_userid', 'rabbit')
        password = self.deployment_config.get('rabbit_password', 'rabbit')
        virtual_host = self.deployment_config.get('rabbit_virtual_host', '/')
        durable = self.deployment_config.get('durable_queue', True)
        queue_arguments = self.deployment_config.get('queue_arguments', {})
        exit_on_exception = self.deployment_config.get('exit_on_exception',
                                                       False)
        topics = self.deployment_config.get('topics', {})

        deployment_id = deployment_config['id']  # Mandatory.

        self.logger.info("%s: %s %s %s %s %s" %
                    (name, exchange, host, port, user_id, virtual_host))

        params = dict(hostname=host,
                      port=port,
                      userid=user_id,
                      password=password,
                      transport="librabbitmq",
                      virtual_host=virtual_host)

        noop_handler = NoopHandler()

        # continue_running() is used for testing
        while continue_running():
            try:
                self.logger.debug("Processing on '%s %s'" % (name, exchange))
                with message_service.BrokerConnection(**params) as conn:
                    try:
                        worker = Worker(name, conn, deployment_id, durable,
                                        queue_arguments, exchange,
                                        topics[exchange], noop_handler)
                        worker.run()
                    except Exception as e:
                        self.logger.exception(
                            "name=%s, exchange=%s, exception=%s. "
                            "Reconnecting in 5s" % (name, exchange, e))
                        exit_or_sleep(exit_on_exception)
                self.logger.debug("Completed processing on '%s %s'" %
                                          (name, exchange))
            except Exception:
                e = sys.exc_info()[0]
                msg = "Uncaught exception: deployment=%s, exchange=%s, " \
                      "exception=%s. Retrying in 5s"
                self.logger.exception(msg % (name, exchange, e))
                exit_or_sleep(exit_on_exception)
        self.logger.info("Worker exiting.")


class NotaBene(object):
    def __init__(self):
        self.processes = []
        self.log_listener = None

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
        for deployment in config.deployments():
            if deployment.get('enabled', True):
                # Close the connection before spinning up the child process,
                # otherwise the child process will attempt to use the connection
                # the parent process opened up to get/create the deployment.
                close_connection()
                for exchange in deployment.get('topics').keys():
                    process = multiprocessing.Process(target=_spawn_consumer,
                              args=(self.log_listener, deployment, exchange))
                    process.daemon = True
                    process.start()
                    processes.append(process)
        signal.signal(signal.SIGINT, _kill_time)
        signal.signal(signal.SIGTERM, _kill_time)
        signal.pause()
