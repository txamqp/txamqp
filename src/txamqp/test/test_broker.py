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
from twisted.internet.defer import inlineCallbacks

from txamqp.client import ConnectionClosed
from txamqp.queue import Empty
from txamqp.content import Content
from txamqp.testlib import TestBase, SupportedBrokers, QPID, OPENAMQ


class ASaslPlainAuthenticationTest(TestBase):
    """Test for SASL PLAIN authentication Broker functionality"""

    @inlineCallbacks
    def authenticate(self, client, user, password):
        yield client.authenticate(user, password, mechanism='PLAIN')

    @inlineCallbacks
    def test_sasl_plain(self):
        channel = yield self.client.channel(200)
        yield channel.channel_open()
        yield channel.channel_close()


class ASaslAmqPlainAuthenticationTest(TestBase):
    """Test for SASL AMQPLAIN authentication Broker functionality"""

    @inlineCallbacks
    def authenticate(self, client, user, password):
        yield client.authenticate(user, password, mechanism='AMQPLAIN')

    @inlineCallbacks
    def test_sasl_amq_plain(self):
        channel = yield self.client.channel(200)
        yield channel.channel_open()
        yield channel.channel_close()


class BrokerTests(TestBase):
    """Tests for basic Broker functionality"""

    @inlineCallbacks
    def test_amqp_basic_13(self):
        """
        First, this test tries to receive a message with a no-ack
        consumer. Second, this test tries to explicitely receive and
        acknowledge a message with an acknowledging consumer.
        """
        ch = self.channel
        yield self.queue_declare(ch, queue="myqueue")

        # No ack consumer
        ctag = (yield ch.basic_consume(queue="myqueue", no_ack=True)).consumer_tag
        body = "test no-ack"
        ch.basic_publish(routing_key="myqueue", content=Content(body))
        msg = yield ((yield self.client.queue(ctag)).get(timeout=5))
        self.assert_(msg.content.body == body)

        # Acknowleding consumer
        yield self.queue_declare(ch, queue="otherqueue")
        ctag = (yield ch.basic_consume(queue="otherqueue", no_ack=False)).consumer_tag
        body = "test ack"
        ch.basic_publish(routing_key="otherqueue", content=Content(body))
        msg = yield ((yield self.client.queue(ctag)).get(timeout=5))
        ch.basic_ack(delivery_tag=msg.delivery_tag)
        self.assert_(msg.content.body == body)

    @inlineCallbacks
    def test_basic_delivery_immediate(self):
        """
        Test basic message delivery where consume is issued before publish.

        Will be skipped for RabbitMQ 3.0 or higher, since support for the
        'immediate' flag was removed, see:

        http://www.rabbitmq.com/blog/2012/11/19/breaking-things-with-rabbitmq-3-0
        """
        server_properties = self.client.delegate.server_properties
        if server_properties["product"] == "RabbitMQ":
            version = tuple(map(int, server_properties["version"].split(".")))
            if version >= (3, 0, 0):
                self.skipTest("Not supported for this broker.")

        channel = self.channel
        yield self.exchange_declare(channel, exchange="test-exchange", type="direct")
        yield self.queue_declare(channel, queue="test-queue") 
        yield channel.queue_bind(queue="test-queue", exchange="test-exchange", routing_key="key")
        reply = yield channel.basic_consume(queue="test-queue", no_ack=True)
        queue = yield self.client.queue(reply.consumer_tag)

        body = "Immediate Delivery"
        channel.basic_publish(exchange="test-exchange", routing_key="key", content=Content(body), immediate=True)
        msg = yield queue.get(timeout=5)
        self.assert_(msg.content.body == body)

        # TODO: Ensure we fail if immediate=True and there's no consumer.

    @inlineCallbacks
    def test_basic_delivery_queued(self):
        """
        Test basic message delivery where publish is issued before consume
        (i.e. requires queueing of the message)
        """
        channel = self.channel
        yield self.exchange_declare(channel, exchange="test-exchange", type="direct")
        yield self.queue_declare(channel, queue="test-queue")
        yield channel.queue_bind(queue="test-queue", exchange="test-exchange", routing_key="key")
        body = "Queued Delivery"
        channel.basic_publish(exchange="test-exchange", routing_key="key", content=Content(body))
        reply = yield channel.basic_consume(queue="test-queue", no_ack=True)
        queue = yield self.client.queue(reply.consumer_tag)
        msg = yield queue.get(timeout=5)
        self.assert_(msg.content.body == body)

    @inlineCallbacks
    def test_invalid_channel(self):
        channel = yield self.client.channel(200)
        try:
            yield channel.queue_declare(exclusive=True)
            self.fail("Expected error on queue_declare for invalid channel")
        except ConnectionClosed as e:
            self.assertConnectionException(504, e.args[0])

    @inlineCallbacks
    def test_closed_channel(self):
        channel = yield self.client.channel(200)
        yield channel.channel_open()
        yield channel.channel_close()
        try:
            yield channel.queue_declare(exclusive=True)
            self.fail("Expected error on queue_declare for closed channel")
        except ConnectionClosed as e:
            self.assertConnectionException(504, e.args[0])

    @SupportedBrokers(QPID, OPENAMQ)
    @inlineCallbacks
    def test_channel_flow(self):
        channel = self.channel
        yield channel.queue_declare(queue="flow_test_queue", exclusive=True)
        yield channel.basic_consume(consumer_tag="my-tag", queue="flow_test_queue")
        incoming = yield self.client.queue("my-tag")

        yield channel.channel_flow(active=False)        
        channel.basic_publish(routing_key="flow_test_queue", content=Content("abcdefghijklmnopqrstuvwxyz"))
        try:
            yield incoming.get(timeout=1) 
            self.fail("Received message when flow turned off.")
        except Empty:
            pass

        yield channel.channel_flow(active=True)
        msg = yield incoming.get(timeout=1)
        self.assertEqual("abcdefghijklmnopqrstuvwxyz", msg.content.body)

    @inlineCallbacks
    def test_close_cleanly(self):
        """
        Test closing a client cleanly, by sending 'close' and waiting for
        'close-ok'.
        """
        yield self.client.close(within=5)
        yield self.client.disconnected.wait()
        self.assertTrue(self.client.closed)
