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

from txamqp.client import Closed
from txamqp.content import Content
from txamqp.testlib import TestBase, SupportedBrokers, QPID, OPENAMQ


class QueueTests(TestBase):
    """Tests for 'methods' on the amqp queue 'class'"""

    @SupportedBrokers(QPID, OPENAMQ)
    @inlineCallbacks
    def test_purge(self):
        """
        Test that the purge method removes messages from the queue
        """
        channel = self.channel
        # setup, declare a queue and add some messages to it:
        yield channel.exchange_declare(exchange="test-exchange", type="direct")
        yield channel.queue_declare(queue="test-queue", exclusive=True)
        yield channel.queue_bind(queue="test-queue", exchange="test-exchange", routing_key="key")
        channel.basic_publish(exchange="test-exchange", routing_key="key", content=Content("one"))
        channel.basic_publish(exchange="test-exchange", routing_key="key", content=Content("two"))
        channel.basic_publish(exchange="test-exchange", routing_key="key", content=Content("three"))

        # check that the queue now reports 3 messages:
        reply = yield channel.queue_declare(queue="test-queue")
        self.assertEqual(3, reply.message_count)

        # now do the purge, then test that three messages are purged and the count drops to 0
        reply = yield channel.queue_purge(queue="test-queue")
        self.assertEqual(3, reply.message_count)
        reply = yield channel.queue_declare(queue="test-queue")
        self.assertEqual(0, reply.message_count)

        # send a further message and consume it, ensuring that the other messages are really gone
        channel.basic_publish(exchange="test-exchange", routing_key="key", content=Content("four"))
        reply = yield channel.basic_consume(queue="test-queue", no_ack=True)
        queue = yield self.client.queue(reply.consumer_tag)
        msg = yield queue.get(timeout=1)
        self.assertEqual("four", msg.content.body)

        # check error conditions (use new channels):
        channel = yield self.client.channel(2)
        yield channel.channel_open()
        try:
            # queue specified but doesn't exist:
            yield channel.queue_purge(queue="invalid-queue")
            self.fail("Expected failure when purging non-existent queue")
        except Closed as e:
            self.assertChannelException(404, e.args[0])

        channel = yield self.client.channel(3)
        yield channel.channel_open()
        try:
            # queue not specified and none previously declared for channel:
            yield channel.queue_purge()
            self.fail("Expected failure when purging unspecified queue")
        except Closed as e:
            self.assertConnectionException(530, e.args[0])

        # cleanup
        other = yield self.connect()
        channel = yield other.channel(1)
        yield channel.channel_open()
        yield channel.exchange_delete(exchange="test-exchange")

    @inlineCallbacks
    def test_declare_exclusive(self):
        """
        Test that the exclusive field is honoured in queue.declare
        """
        # TestBase.setUp has already opened channel(1)
        c1 = self.channel
        # Here we open a second separate connection:
        other = yield self.connect()
        c2 = yield other.channel(1)
        yield c2.channel_open()

        # declare an exclusive queue:
        yield c1.queue_declare(queue="exclusive-queue", exclusive="True")
        try:
            # other connection should not be allowed to declare this:
            yield c2.queue_declare(queue="exclusive-queue", exclusive="True")
            self.fail("Expected second exclusive queue_declare to raise a channel exception")
        except Closed as e:
            self.assertChannelException(405, e.args[0])

    @inlineCallbacks
    def test_declare_passive(self):
        """
        Test that the passive field is honoured in queue.declare
        """
        channel = self.channel
        # declare an exclusive queue:
        yield channel.queue_declare(queue="passive-queue-1", exclusive="True")
        yield channel.queue_declare(queue="passive-queue-1", passive="True")
        try:
            # other connection should not be allowed to declare this:
            yield channel.queue_declare(queue="passive-queue-2", passive="True")
            self.fail("Expected passive declaration of non-existant queue to raise a channel exception")
        except Closed as e:
            self.assertChannelException(404, e.args[0])

    @inlineCallbacks
    def test_bind(self):
        """
        Test various permutations of the queue.bind method
        """
        channel = self.channel
        yield channel.queue_declare(queue="queue-1", exclusive="True")

        # straightforward case, both exchange & queue exist so no errors expected:
        yield channel.queue_bind(queue="queue-1", exchange="amq.direct", routing_key="key1")

        # bind the default queue for the channel (i.e. last one declared):
        yield channel.queue_bind(exchange="amq.direct", routing_key="key2")

        # use the queue name where neither routing key nor queue are specified:
        yield channel.queue_bind(exchange="amq.direct")

        # try and bind to non-existant exchange
        try:
            yield channel.queue_bind(queue="queue-1", exchange="an-invalid-exchange", routing_key="key1")
            self.fail("Expected bind to non-existant exchange to fail")
        except Closed as e:
            self.assertChannelException(404, e.args[0])

        # need to reopen a channel:
        channel = yield self.client.channel(2)
        yield channel.channel_open()

        # try and bind non-existant queue:
        try:
            yield channel.queue_bind(queue="queue-2", exchange="amq.direct", routing_key="key1")
            self.fail("Expected bind of non-existant queue to fail")
        except Closed as e:
            self.assertChannelException(404, e.args[0])

    @inlineCallbacks
    def test_delete_simple(self):
        """
        Test basic queue deletion
        """
        channel = self.channel

        # straight-forward case:
        yield channel.queue_declare(queue="delete-me")
        channel.basic_publish(routing_key="delete-me", content=Content("a"))
        channel.basic_publish(routing_key="delete-me", content=Content("b"))
        channel.basic_publish(routing_key="delete-me", content=Content("c"))
        reply = yield channel.queue_delete(queue="delete-me")
        self.assertEqual(3, reply.message_count)
        # check that it has gone by declaring passively
        try:
            yield channel.queue_declare(queue="delete-me", passive="True")
            self.fail("Queue has not been deleted")
        except Closed as e:
            self.assertChannelException(404, e.args[0])

    @inlineCallbacks
    def test_delete_non_existing(self):
        """
        Attempting to delete a non-existing queue raises a 404.

        Will be skipped for Rabbitmq 3.2 or higher, since deleting a non
        existing queue silently succeeds, see:

        http://hg.rabbitmq.com/rabbitmq-server/rev/8eaec8dfbc41
        http://lists.rabbitmq.com/pipermail/rabbitmq-discuss/2014-January/032970.html
        http://www.rabbitmq.com/specification.html (search for "queue.delete")
        """
        server_properties = self.client.delegate.server_properties
        if server_properties["product"] == "RabbitMQ":
            version = tuple(map(int, server_properties["version"].split(".")))
            if version >= (3, 2, 0):
                self.skipTest("Not supported for this broker.")

        # check attempted deletion of non-existant queue is handled correctly:
        channel = yield self.client.channel(2)
        yield channel.channel_open()
        try:
            result = yield channel.queue_delete(queue="i-dont-exist", if_empty="True")
            print(result)
            self.fail("Expected delete of non-existant queue to fail")
        except Closed as e:
            self.assertChannelException(404, e.args[0])

    @inlineCallbacks
    def test_delete_ifempty(self):
        """
        Test that if_empty field of queue_delete is honoured
        """
        channel = self.channel

        # create a queue and add a message to it (use default binding):
        yield channel.queue_declare(queue="delete-me-2")
        yield channel.queue_declare(queue="delete-me-2", passive="True")
        channel.basic_publish(routing_key="delete-me-2", content=Content("message"))

        # try to delete, but only if empty:
        try:
            yield channel.queue_delete(queue="delete-me-2", if_empty="True")
            self.fail("Expected delete if_empty to fail for non-empty queue")
        except Closed as e:
            self.assertChannelException(406, e.args[0])

        # need new channel now:
        channel = yield self.client.channel(2)
        yield channel.channel_open()

        # empty queue:
        reply = yield channel.basic_consume(queue="delete-me-2", no_ack=True)
        queue = yield self.client.queue(reply.consumer_tag)
        msg = yield queue.get(timeout=1)
        self.assertEqual("message", msg.content.body)
        yield channel.basic_cancel(consumer_tag=reply.consumer_tag)

        # retry deletion on empty queue:
        yield channel.queue_delete(queue="delete-me-2", if_empty="True")

        # check that it has gone by declaring passively:
        try:
            yield channel.queue_declare(queue="delete-me-2", passive="True")
            self.fail("Queue has not been deleted")
        except Closed as e:
            self.assertChannelException(404, e.args[0])

    @inlineCallbacks
    def test_delete_ifunused(self):
        """
        Test that if_unused field of queue_delete is honoured
        """
        channel = self.channel

        # create a queue and register a consumer:
        yield channel.queue_declare(queue="delete-me-3")
        yield channel.queue_declare(queue="delete-me-3", passive="True")
        reply = yield channel.basic_consume(queue="delete-me-3", no_ack=True)

        # need new channel now:
        channel2 = yield self.client.channel(2)
        yield channel2.channel_open()
        # try to delete, but only if empty:
        try:
            yield channel2.queue_delete(queue="delete-me-3", if_unused="True")
            self.fail("Expected delete if_unused to fail for queue with existing consumer")
        except Closed as e:
            self.assertChannelException(406, e.args[0])

        yield channel.basic_cancel(consumer_tag=reply.consumer_tag)
        yield channel.queue_delete(queue="delete-me-3", if_unused="True")

        # check that it has gone by declaring passively:
        try:
            yield channel.queue_declare(queue="delete-me-3", passive="True")
            self.fail("Queue has not been deleted")
        except Closed as e:
            self.assertChannelException(404, e.args[0])

    @inlineCallbacks
    def test_close_queue(self):
        from txamqp.queue import Closed as QueueClosed
        channel = self.channel

        yield channel.queue_declare(queue="test-queue")
        reply = yield channel.basic_consume(queue="test-queue")

        queue = yield self.client.queue(reply.consumer_tag)
        d = self.assertFailure(queue.get(timeout=1), QueueClosed)
        self.client.close(None)
        yield d
