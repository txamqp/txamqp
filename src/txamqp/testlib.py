#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from txamqp.content import Content
import txamqp.spec

from txamqp.protocol import AMQChannel, AMQClient, TwistedDelegate

from twisted.internet import protocol, reactor
from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue, DeferredQueue, DeferredLock
from twisted.python import failure
from txamqp.queue import Empty

class TestBase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

        self.host = 'localhost'
        self.port = 5672
        self.spec = '../specs/qpid/amqp.0-8.xml'
        self.user = 'guest'
        self.password = 'guest'
        self.vhost = 'localhost'
        self.heartbeat = 0
        self.queues = []
        self.exchanges = []
        self.connectors = []

    @inlineCallbacks
    def connect(self, host=None, port=None, spec=None, user=None,
                           password=None, vhost=None, heartbeat=None):
        host = host or self.host
        port = port or self.port
        spec = spec or self.spec
        user = user or self.user
        password = password or self.password
        vhost = vhost or self.vhost
        heartbeat = heartbeat or self.heartbeat

        delegate = TwistedDelegate()
        onConn = Deferred()
        p = AMQClient(delegate, vhost, heartbeat=heartbeat, spec=txamqp.spec.load(spec))
        f = protocol._InstanceFactory(reactor, p, onConn)
        c = reactor.connectTCP(host, port, f)
        self.connectors.append(c)
        client = yield onConn

        yield client.authenticate(user, password)
        returnValue(client)
 
    @inlineCallbacks
    def setUp(self):
        self.client = yield self.connect()

        self.channel = yield self.client.channel(1)
        yield self.channel.channel_open()

    @inlineCallbacks
    def tearDown(self):
        for ch, q in self.queues:
            yield ch.queue_delete(queue=q)
        for ch, ex in self.exchanges:
            yield ch.exchange_delete(exchange=ex)
        for connector in self.connectors:
            yield connector.disconnect()

    @inlineCallbacks
    def queue_declare(self, channel=None, *args, **keys):
        channel = channel or self.channel
        reply = yield channel.queue_declare(*args, **keys)
        self.queues.append((channel, reply.queue))
        returnValue(reply)

    @inlineCallbacks
    def exchange_declare(self, channel=None, ticket=0, exchange='',
                         type='', passive=False, durable=False,
                         auto_delete=False, internal=False, nowait=False,
                         arguments={}):
        channel = channel or self.channel
        reply = yield channel.exchange_declare(ticket, exchange, type, passive, durable, auto_delete, internal, nowait, arguments)
        self.exchanges.append((channel,exchange))
        returnValue(reply)

    def assertChannelException(self, expectedCode, message):
        self.assertEqual("channel", message.method.klass.name)
        self.assertEqual("close", message.method.name)
        self.assertEqual(expectedCode, message.reply_code)

    def assertConnectionException(self, expectedCode, message):
        self.assertEqual("connection", message.method.klass.name)
        self.assertEqual("close", message.method.name)
        self.assertEqual(expectedCode, message.reply_code)

    @inlineCallbacks
    def consume(self, queueName):
        """Consume from named queue returns the Queue object."""
        reply = yield self.channel.basic_consume(queue=queueName, no_ack=True)
        returnValue((yield self.client.queue(reply.consumer_tag)))

    @inlineCallbacks
    def assertEmpty(self, queue):
        """Assert that the queue is empty"""
        try:
            yield queue.get(timeout=1)
            self.fail("Queue is not empty.")
        except Empty: None              # Ignore

    @inlineCallbacks
    def assertPublishGet(self, queue, exchange="", routing_key="", properties=None):
        """
        Publish to exchange and assert queue.get() returns the same message.
        """
        body = self.uniqueString()
        self.channel.basic_publish(exchange=exchange,
                                   content=Content(body, properties=properties),
                                   routing_key=routing_key)
        msg = yield queue.get(timeout=1)
        self.assertEqual(body, msg.content.body)
        if (properties): self.assertEqual(properties, msg.content.properties)

    def uniqueString(self):
        """Generate a unique string, unique for this TestBase instance"""
        if not "uniqueCounter" in dir(self): self.uniqueCounter = 1;
        return "Test Message " + str(self.uniqueCounter)

    @inlineCallbacks
    def assertPublishConsume(self, queue="", exchange="", routing_key="", properties=None):
        """
        Publish a message and consume it, assert it comes back intact.
        Return the Queue object used to consume.
        """
        yield self.assertPublishGet((yield self.consume(queue)), exchange, routing_key, properties)

    @inlineCallbacks
    def consume(self, queueName):
        """Consume from named queue returns the Queue object."""
        reply = yield self.channel.basic_consume(queue=queueName, no_ack=True)
        returnValue((yield self.client.queue(reply.consumer_tag)))
