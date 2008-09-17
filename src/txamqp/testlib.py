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

from qpid.content import Content
import qpid.spec

from txamqp.protocol import AMQChannel, AMQClient

from twisted.internet import protocol, reactor
from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue, DeferredQueue, DeferredLock
from twisted.python import failure
from qpid.peer import Closed
from qpid.queue import Empty

class TimeoutDeferredQueue(DeferredQueue):

    def _timeout(self, deferred):
        if not deferred.called:
            if deferred in self.waiting:
                self.waiting.remove(deferred)
                deferred.errback(Empty())

    def get(self, timeout=None):
        deferred = DeferredQueue.get(self)
        if timeout:
            deferred.setTimeout(timeout, timeoutFunc=self._timeout)
        return deferred

class TestAMQChannel(AMQChannel):

    def messageReceived(self, message):
        method = message.method
        name = "%s_%s" % (qpid.spec.pythonize(method.klass.name),
                        qpid.spec.pythonize(method.name))

        if name == 'basic_deliver':
            self.proto.queue(message.consumer_tag).addCallback(self._messageReceived, message)
        elif name == 'channel_close' or name == 'connection_close':
            deferred, self.deferred = self.deferred, Deferred()
            deferred.errback(Closed(message))
        else:
            deferred, self.deferred = self.deferred, Deferred()
            deferred.callback(message)

    def _messageReceived(self, queue, message):
        queue.put(message)

class TestAMQClient(AMQClient):

    greetDeferred = None
    channelClass = TestAMQChannel

    def __init__(self, *args, **kwargs):
        AMQClient.__init__(self, *args, **kwargs)
        self.lock = DeferredLock()
        self.queues = {}

    def _queue(self, lock, key):
        try:
            try:
                q = self.queues[key]
            except KeyError:
                q = TimeoutDeferredQueue()
                self.queues[key] = q
        finally:
            lock.release()
        return q

    def queue(self, key):
        return self.lock.acquire().addCallback(self._queue, key)

    def serverGreeting(self):
        if self.greetDeferred is not None:
            d, self.greetDeferred = self.greetDeferred, None
            d.callback(self)

class TestAMQClientFactory(protocol.ClientFactory):

    protocol = TestAMQClient

    def __init__(self, spec, onConn):
        self.spec = spec
        self.onConn = onConn

    def buildProtocol(self, addr):
        # We need to override buildProtocol, AMQClient needs the AMQP spec
        # as a constructor parameter
        p = self.protocol(self.spec)
        p.factory = self
        p.greetDeferred = self.onConn
        return p

class TestBase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

        self.host = 'localhost'
        self.port = 5672
        self.spec = '../specs/amqp.0-8.xml'
        self.user = 'guest'
        self.password = 'guest'

    @inlineCallbacks
    def connect(self, host=None, port=None, spec=None, user=None, password=None):
        host = host or self.host
        port = port or self.port
        spec = spec or self.spec
        user = user or self.user
        password = password or self.password

        onConn = Deferred()
        connector = reactor.connectTCP(host, port,
            TestAMQClientFactory(qpid.spec.load(spec), onConn))
        self.connectors.append(connector)

        client = yield onConn
        yield client.start({"LOGIN": user, "PASSWORD": password})
        returnValue(client)
 
    @inlineCallbacks
    def setUp(self):
        self.queues = []
        self.exchanges = []

        self.connectors = []

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
