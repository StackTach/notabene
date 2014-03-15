# Copyright 2014 - Dark Secret Software Inc.
# All Rights Reserved.

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

import time


class FakeMessage(object):
    def __init__(self, routing_key, body):
        self.delivery_info = {'routing_key': routing_key}
        self.body = body
        self.acked = False

    def ack(self):
        self.acked = True


class Worker(object):
    def __init__(self, callback, name, deployment_id, logger, config,
                 exchange):
        self.callback = callback
        self.deployment_id = deployment_id
        self.name = name
        self.total_processed = 0
        self.config = config
        self.exchange = exchange
        self.logger = logger

    def process(self, message):
        args = ("test", anyjson.loads(body))
        asJson = anyjson.dumps(args)
        # save raw and ack the message
        self.callback.on_event(self.deployment_id, args, asJson, self.exchange)

    def run(self):
        self.logger.debug("%s: Starting Test Worker" % self.exchange)
        messages = [FakeMessage("error", 
                                {'id': 1, 'event_name': 'create.start'}),
                    FakeMessage("error", 
                                {'id': 2, 'event_name': 'create.end'})]
        for message in messages:
            routing_key = "routing_key"
            body = {'id':1}
            self.callback.on_event(self.deployment_id, routing_key, body, 
                                   self.exchange)
            time.sleep(1)
        self.logger.debug("%s: Calling shutdown on callback" % self.exchange)
        self.callback.shutting_down()
        self.logger.debug("%s: Working finishing up." % self.exchange)

        
def start_worker(callback, name, deployment_id, deployment_config, 
                 exchange, logger):
    worker = Worker(callback, name, deployment_id, logger, deployment_config,
                    exchange)
    worker.run()
    logger.debug("start_worker finished on '%s'" % exchange)
