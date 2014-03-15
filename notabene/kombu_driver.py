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

import signal 

import anyjson
import kombu
import kombu.entity
import kombu.pools
import kombu.connection
import kombu.common
import kombu.mixins
import kombu.serialization


def _loads(string):
    return anyjson.loads(kombu.serialization.BytesIO(string))


class Worker(kombu.mixins.ConsumerMixin):
    def __init__(self, callback, name, connection, deployment, durable,
                 queue_arguments, exchange, topics, logger):
        self.callback = callback
        self.connection = connection
        self.deployment = deployment
        self.durable = durable
        self.queue_arguments = queue_arguments
        self.name = name
        self.topics = topics
        self.exchange = exchange
        self.logger = logger

        signal.signal(signal.SIGTERM, self._shutdown)

        kombu.serialization.register('bufferjson', _loads, anyjson.dumps,
                                     content_type='application/json',
                                     content_encoding='binary')

    def _create_exchange(self, name, exchange_type, exclusive=False,
                         auto_delete=False, durable=True):
        return kombu.entity.Exchange(name,
                                     type=exchange_type,
                                     exclusive=exclusive,
                                     auto_delete=auto_delete, durable=durable)


    def _create_queue(self, name, exchange, routing_key, exclusive=False,
                     auto_delete=False, durable=True):
        return kombu.Queue(name, exchange, durable=durable,
                           auto_delete=auto_delete, exclusive=exclusive,
                           queue_arguments=self.queue_arguments,
                           routing_key=routing_key)

    def get_consumers(self, Consumer, channel):
        exchange = self._create_exchange(self.exchange, "topic")

        queues = [self._create_queue(topic['queue'], exchange,
                                     topic['routing_key'])
                      for topic in self.topics]

        return [Consumer(queues=queues, callbacks=[self._on_notification])]

    def _process(self, message):
        routing_key = message.delivery_info['routing_key']
        body = anyjson.loads(str(message.body))
        # save raw and ack the message
        self.callback.on_event(self.deployment, routing_key, body, 
                               self.exchange)
        message.ack()

    def _on_notification(self, body, message):
        try:
            self._process(message)
        except Exception, e:
            self.logger.debug("Problem: %s\nFailed message body:\n%s" %
                      (e, anyjson.loads(str(body))))
            raise

    def _shutdown(self, signal, stackframe=False):
        self.should_stop = True
        self.callback.shutting_down()


def start_worker(callback, name, deployment_id, deployment_config, 
                 exchange, logger):
    host = deployment_config.get('rabbit_host', 'localhost')
    port = deployment_config.get('rabbit_port', 5672)
    user_id = deployment_config.get('rabbit_userid', 'rabbit')
    password = deployment_config.get('rabbit_password', 'rabbit')
    virtual_host = deployment_config.get('rabbit_virtual_host', '/')
    durable = deployment_config.get('durable_queue', True)
    queue_arguments = deployment_config.get('queue_arguments', {})
    topics = deployment_config.get('topics', {})

    params = dict(hostname=host,
                  port=port,
                  userid=user_id,
                  password=password,
                  transport="librabbitmq",
                  virtual_host=virtual_host)

    logger.info("%s: %s %s %s %s %s" %
                (name, exchange, host, port, user_id, virtual_host))
    with kombu.connection.BrokerConnection(**params) as conn:
        worker = Worker(callback, name, conn, deployment_id, durable,
                        queue_arguments, exchange, topics[exchange], 
                        logger)
        worker.run()
