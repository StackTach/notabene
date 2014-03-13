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


class Worker(object):
    def __init__(self, callback, name, deployment, logger):
        self.callback = callback
        self.deployment = deployment
        self.name = name
        self.total_processed = 0
        self.topics = topics
        self.exchange = exchange
        self.logger = logger

    def _process(self, message):
        args = ("test", anyjson.loads(body))
        asJson = anyjson.dumps(args)
        # save raw and ack the message
        self.callback.on_event(self.deployment, args, asJson, self.exchange)

    def _shutdown(self):
        self.should_stop = True
        self.callback.shutting_down()


def start_worker(callback, name, deployment_id, deployment_config, 
                 exchange, logger):
    worker = Worker(callback, name, deployment_id, logger)
    worker.run()
