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

        signal.signal(signal.SIGTERM, self._shutdown)

        kombu.serialization.register('bufferjson', _loads, anyjson.dumps,
                                     content_type='application/json',
                                     content_encoding='binary')

    def _process(self, message):
        routing_key = message.delivery_info['routing_key']

        body = str(message.body)
        args = (routing_key, anyjson.loads(body))
        asJson = anyjson.dumps(args)
        # save raw and ack the message
        self.callback.on_event(self.deployment, args, asJson, self.exchange)

        self.processed += 1
        message.ack()

    def _shutdown(self, signal, stackframe=False):
        self.should_stop = True
        self.callback.shutting_down()


def start_worker(callback, name, deployment_id, deployment_config, 
                 exchange, logger, shutdown_soon):
    worker = Worker(callback, name, conn, deployment_id, durable,
                    queue_arguments, exchange, topics[exchange], 
                    logger)
    worker.run()
