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

import os
import warnings

from txamqp.content import Content
import txamqp.spec

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate

from twisted.internet import error, protocol, reactor
from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue
from txamqp.queue import Empty


RABBITMQ = "RABBITMQ"
OPENAMQ = "OPENAMQ"
QPID = "QPID"


class supportedBrokers(object):

    def __init__(self, *supporterBrokers):
        self.supporterBrokers = supporterBrokers

    def __call__(self, f):
        if _get_broker() not in self.supporterBrokers:
            f.skip = "Not supported for this broker."
        return f


def _get_broker():
    return os.environ.get("TXAMQP_BROKER")

USERNAME='guest'
PASSWORD='guest'
VHOST='/'
HEARTBEAT = 0

class TestBase(unittest.TestCase):

    clientClass = AMQClient
    heartbeat = HEARTBEAT

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

        self.host = 'localhost'
        self.port = 5672
        self.broker = _get_broker()
        if self.broker is None:
            warnings.warn(
                "Using default broker rabbitmq. Define TXAMQP_BROKER "
                "environment variable to customized it.")
            self.broker = RABBITMQ
        if self.broker == RABBITMQ:
            self.spec = '../specs/standard/amqp0-9.stripped.xml'
        elif self.broker == OPENAMQ:
            self.spec = '../specs/standard/amqp0-9.stripped.xml'
        elif self.broker == QPID:
            self.spec = '../specs/qpid/amqp.0-8.xml'
        else:
            raise RuntimeError(
                "Unsupported broker '%s'. Use one of RABBITMQ, OPENAMQ or "
                "QPID" % self.broker)
        self.user = USERNAME
        self.password = PASSWORD
        self.vhost = VHOST
        self.queues = []
        self.exchanges = []
        self.connectors = []

    @inlineCallbacks
    def connect(self, host=None, port=None, spec=None, user=None, password=None, vhost=None,
            heartbeat=None, clientClass=None):
        host = host or self.host
        port = port or self.port
        spec = spec or self.spec
        user = user or self.user
        password = password or self.password
        vhost = vhost or self.vhost
        heartbeat = heartbeat or self.heartbeat
        clientClass = clientClass or self.clientClass

        delegate = TwistedDelegate()
        onConn = Deferred()
        p = clientClass(delegate, vhost, txamqp.spec.load(spec), heartbeat=heartbeat)
        f = protocol._InstanceFactory(reactor, p, onConn)
        c = reactor.connectTCP(host, port, f)
        def errb(thefailure):
            thefailure.trap(error.ConnectionRefusedError)
            print "failed to connect to host: %s, port: %s; These tests are designed to run against a running instance" \
                  " of the %s AMQP broker on the given host and port.  failure: %r" % (host, port, self.broker, thefailure,)
            thefailure.raiseException()
        onConn.addErrback(errb)

        self.connectors.append(c)
        client = yield onConn

        yield self.authenticate(client, user, password)
        returnValue(client)

    @inlineCallbacks
    def authenticate(self,client,user,password):
        yield client.authenticate(user, password)

    @inlineCallbacks
    def setUp(self):
        try:
            self.client = yield self.connect()
        except txamqp.client.Closed, le:
            le.args = tuple(("Unable to connect to AMQP broker in order to run tests (perhaps due to auth failure?). " \
                "The tests assume that an instance of the %s AMQP broker is already set up and that this test script " \
                "can connect to it and use it as user '%s', password '%s', vhost '%s'." % (_get_broker(),
                    USERNAME, PASSWORD, VHOST),) + le.args)
            raise

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
