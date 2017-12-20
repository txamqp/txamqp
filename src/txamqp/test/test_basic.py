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
import pickle
from twisted.internet.defer import inlineCallbacks

from txamqp.client import ConnectionClosed, ChannelClosed
from txamqp.queue import Empty
from txamqp.content import Content
from txamqp.testlib import TestBase, SupportedBrokers, QPID, OPENAMQ, RABBITMQ


class BasicTests(TestBase):
    """Tests for 'methods' on the amqp basic 'class'"""

    @SupportedBrokers(QPID, OPENAMQ)
    @inlineCallbacks
    def test_consume_no_local(self):
        """
        Test that the no_local flag is honoured in the consume method
        """
        channel = self.channel
        # setup, declare two queues:
        yield channel.queue_declare(queue="test-queue-1a", exclusive=True)
        yield channel.queue_declare(queue="test-queue-1b", exclusive=True)
        # establish two consumers one of which excludes delivery of locally sent messages
        yield channel.basic_consume(consumer_tag="local_included", queue="test-queue-1a")
        yield channel.basic_consume(consumer_tag="local_excluded", queue="test-queue-1b", no_local=True)

        # send a message
        channel.basic_publish(routing_key="test-queue-1a", content=Content("consume_no_local"))
        channel.basic_publish(routing_key="test-queue-1b", content=Content("consume_no_local"))

        # check the queues of the two consumers
        excluded = yield self.client.queue("local_excluded")
        included = yield self.client.queue("local_included")
        msg = yield included.get()
        self.assertEqual("consume_no_local", msg.content.body)
        try:
            yield excluded.get(timeout=1)
            self.fail("Received locally published message though no_local=true")
        except Empty:
            pass

    @inlineCallbacks
    def test_consume_exclusive(self):
        """
        Test that the exclusive flag is honoured in the consume method
        """
        channel = self.channel
        # setup, declare a queue:
        yield channel.queue_declare(queue="test-queue-2", exclusive=True)

        # check that an exclusive consumer prevents other consumer being created:
        yield channel.basic_consume(consumer_tag="first", queue="test-queue-2", exclusive=True)
        try:
            yield channel.basic_consume(consumer_tag="second", queue="test-queue-2")
            self.fail("Expected consume request to fail due to previous exclusive consumer")
        except ChannelClosed as e:
            self.assertChannelException(403, e.args[0])

        # open new channel and cleanup last consumer:
        channel = yield self.client.channel(2)
        yield channel.channel_open()

        # check that an exclusive consumer cannot be created if a consumer already exists:
        yield channel.basic_consume(consumer_tag="first", queue="test-queue-2")
        try:
            yield channel.basic_consume(consumer_tag="second", queue="test-queue-2", exclusive=True)
            self.fail("Expected exclusive consume request to fail due to previous consumer")
        except ChannelClosed as e:
            self.assertChannelException(403, e.args[0])

    @inlineCallbacks
    def test_consume_queue_not_found(self):
        """
        C{basic_consume} fails with a channel exception with a C{404} code when
        the specified queue doesn't exist.
        """
        channel = self.channel
        try:
            # queue specified but doesn't exist:
            yield channel.basic_consume(queue="invalid-queue")
            self.fail("Expected failure when consuming from non-existent queue")
        except ChannelClosed as e:
            self.assertChannelException(404, e.args[0])

    @SupportedBrokers(QPID, OPENAMQ)
    @inlineCallbacks
    def test_consume_queue_unspecified(self):
        """
        C{basic_consume} fails with a connection exception with a C{503} code
        when no queue is specified.
        """
        channel = self.channel
        try:
            # queue not specified and none previously declared for channel:
            yield channel.basic_consume(queue="")
            self.fail("Expected failure when consuming from unspecified queue")
        except ConnectionClosed as e:
            self.assertConnectionException(530, e.args[0])

    @SupportedBrokers(RABBITMQ)
    @inlineCallbacks
    def test_consume_queue_unspecified_rabbit(self):
        """
        C{basic_consume} fails with a channel exception with a C{404} code
        when no queue is specified.
        """
        channel = self.channel
        try:
            # queue not specified and none previously declared for channel:
            yield channel.basic_consume(queue="")
            self.fail("Expected failure when consuming from unspecified queue")
        except ChannelClosed as e:
            self.assertChannelException(404, e.args[0])

    @inlineCallbacks
    def test_consume_unique_consumers(self):
        """
        Ensure unique consumer tags are enforced
        """
        channel = self.channel
        # setup, declare a queue:
        yield channel.queue_declare(queue="test-queue-3", exclusive=True)

        # check that attempts to use duplicate tags are detected and prevented:
        yield channel.basic_consume(consumer_tag="first", queue="test-queue-3")
        try:
            yield channel.basic_consume(consumer_tag="first", queue="test-queue-3")
            self.fail("Expected consume request to fail due to non-unique tag")
        except ConnectionClosed as e:
            self.assertConnectionException(530, e.args[0])

    @inlineCallbacks
    def test_cancel(self):
        """
        Test compliance of the basic.cancel method
        """
        channel = self.channel
        # setup, declare a queue:
        yield channel.queue_declare(queue="test-queue-4", exclusive=True)
        yield channel.basic_consume(consumer_tag="my-consumer", queue="test-queue-4")
        channel.basic_publish(routing_key="test-queue-4", content=Content("One"))

        # cancel should stop messages being delivered
        yield channel.basic_cancel(consumer_tag="my-consumer")
        channel.basic_publish(routing_key="test-queue-4", content=Content("Two"))
        myqueue = yield self.client.queue("my-consumer")
        msg = yield myqueue.get(timeout=1)
        self.assertEqual("One", msg.content.body)
        try:
            msg = yield myqueue.get(timeout=1)
            self.fail("Got message after cancellation: " + msg)
        except Empty:
            pass

        # cancellation of non-existant consumers should be handled without error
        yield channel.basic_cancel(consumer_tag="my-consumer")
        yield channel.basic_cancel(consumer_tag="this-never-existed")

    @SupportedBrokers(QPID, OPENAMQ)
    @inlineCallbacks
    def test_ack(self):
        """
        Test basic ack/recover behaviour
        """
        channel = self.channel
        yield channel.queue_declare(queue="test-ack-queue", exclusive=True)

        reply = yield channel.basic_consume(queue="test-ack-queue", no_ack=False)
        queue = yield self.client.queue(reply.consumer_tag)

        channel.basic_publish(routing_key="test-ack-queue", content=Content("One"))
        channel.basic_publish(routing_key="test-ack-queue", content=Content("Two"))
        channel.basic_publish(routing_key="test-ack-queue", content=Content("Three"))
        channel.basic_publish(routing_key="test-ack-queue", content=Content("Four"))
        channel.basic_publish(routing_key="test-ack-queue", content=Content("Five"))

        msg1 = yield queue.get(timeout=1)
        msg2 = yield queue.get(timeout=1)
        msg3 = yield queue.get(timeout=1)
        msg4 = yield queue.get(timeout=1)
        msg5 = yield queue.get(timeout=1)

        self.assertEqual("One", msg1.content.body)
        self.assertEqual("Two", msg2.content.body)
        self.assertEqual("Three", msg3.content.body)
        self.assertEqual("Four", msg4.content.body)
        self.assertEqual("Five", msg5.content.body)

        channel.basic_ack(delivery_tag=msg2.delivery_tag, multiple=True)  # One & Two
        channel.basic_ack(delivery_tag=msg4.delivery_tag, multiple=False)  # Four

        yield channel.basic_recover(requeue=False)

        msg3b = yield queue.get(timeout=1)
        msg5b = yield queue.get(timeout=1)

        self.assertEqual("Three", msg3b.content.body)
        self.assertEqual("Five", msg5b.content.body)

        try:
            extra = yield queue.get(timeout=1)
            self.fail("Got unexpected message: " + extra.content.body)
        except Empty:
            pass

    @inlineCallbacks
    def test_recover_requeue(self):
        """
        Test requeing on recovery
        """
        channel = self.channel
        yield channel.queue_declare(queue="test-requeue", exclusive=True)

        subscription = yield channel.basic_consume(queue="test-requeue", no_ack=False)
        queue = yield self.client.queue(subscription.consumer_tag)

        channel.basic_publish(routing_key="test-requeue", content=Content("One"))
        channel.basic_publish(routing_key="test-requeue", content=Content("Two"))
        channel.basic_publish(routing_key="test-requeue", content=Content("Three"))
        channel.basic_publish(routing_key="test-requeue", content=Content("Four"))
        channel.basic_publish(routing_key="test-requeue", content=Content("Five"))

        msg1 = yield queue.get(timeout=1)
        msg2 = yield queue.get(timeout=1)
        msg3 = yield queue.get(timeout=1)
        msg4 = yield queue.get(timeout=1)
        msg5 = yield queue.get(timeout=1)

        self.assertEqual("One", msg1.content.body)
        self.assertEqual("Two", msg2.content.body)
        self.assertEqual("Three", msg3.content.body)
        self.assertEqual("Four", msg4.content.body)
        self.assertEqual("Five", msg5.content.body)

        channel.basic_ack(delivery_tag=msg2.delivery_tag, multiple=True)  # One & Two
        channel.basic_ack(delivery_tag=msg4.delivery_tag, multiple=False)  # Four

        yield channel.basic_cancel(consumer_tag=subscription.consumer_tag)
        subscription2 = yield channel.basic_consume(queue="test-requeue")
        queue2 = yield self.client.queue(subscription2.consumer_tag)

        yield channel.basic_recover(requeue=True)

        msg3b = yield queue2.get()
        msg5b = yield queue2.get()

        self.assertEqual("Three", msg3b.content.body)
        self.assertEqual("Five", msg5b.content.body)

        self.assertEqual(True, msg3b.redelivered)
        self.assertEqual(True, msg5b.redelivered)

        try:
            extra = yield queue2.get(timeout=1)
            self.fail("Got unexpected message in second queue: " + extra.content.body)
        except Empty:
            pass

        try:
            extra = yield queue.get(timeout=1)
            self.fail("Got unexpected message in original queue: " + extra.content.body)
        except Empty:
            pass

    @inlineCallbacks
    def test_qos_prefetch_count(self):
        """
        Test that the prefetch count specified is honoured
        """
        # setup: declare queue and subscribe
        channel = self.channel

        # set prefetch to 5:
        yield channel.basic_qos(prefetch_count=5)

        yield channel.queue_declare(queue="test-prefetch-count", exclusive=True)
        subscription = yield channel.basic_consume(queue="test-prefetch-count", no_ack=False)
        queue = yield self.client.queue(subscription.consumer_tag)

        # publish 10 messages:
        for i in range(1, 11):
            channel.basic_publish(routing_key="test-prefetch-count", content=Content("Message %d" % i))

        # only 5 messages should have been delivered:
        for i in range(1, 6):
            msg = yield queue.get(timeout=1)
            self.assertEqual("Message %d" % i, msg.content.body)

        try:
            extra = yield queue.get(timeout=1)
            self.fail("Got unexpected 6th message in original queue: " + extra.content.body)
        except Empty:
            pass

        # ack messages and check that the next set arrive ok:
        channel.basic_ack(delivery_tag=msg.delivery_tag, multiple=True)

        for i in range(6, 11):
            msg = yield queue.get(timeout=1)
            self.assertEqual("Message %d" % i, msg.content.body)

        channel.basic_ack(delivery_tag=msg.delivery_tag, multiple=True)

        try:
            extra = yield queue.get(timeout=1)
            self.fail("Got unexpected 11th message in original queue: " + extra.content.body)
        except Empty:
            pass

    @SupportedBrokers(QPID, OPENAMQ)
    @inlineCallbacks
    def test_qos_prefetch_size(self):
        """
        Test that the prefetch size specified is honoured
        """
        # setup: declare queue and subscribe
        channel = self.channel
        yield channel.queue_declare(queue="test-prefetch-size", exclusive=True)
        subscription = yield channel.basic_consume(queue="test-prefetch-size", no_ack=False)
        queue = yield self.client.queue(subscription.consumer_tag)

        # set prefetch to 50 bytes (each message is 9 or 10 bytes):
        channel.basic_qos(prefetch_size=50)

        # publish 10 messages:
        for i in range(1, 11):
            channel.basic_publish(routing_key="test-prefetch-size", content=Content("Message %d" % i))

        # only 5 messages should have been delivered (i.e. 45 bytes worth):
        for i in range(1, 6):
            msg = yield queue.get(timeout=1)
            self.assertEqual("Message %d" % i, msg.content.body)

        try:
            extra = yield queue.get(timeout=1)
            self.fail("Got unexpected 6th message in original queue: " + extra.content.body)
        except Empty:
            pass

        # ack messages and check that the next set arrive ok:
        channel.basic_ack(delivery_tag=msg.delivery_tag, multiple=True)

        for i in range(6, 11):
            msg = yield queue.get(timeout=1)
            self.assertEqual("Message %d" % i, msg.content.body)

        channel.basic_ack(delivery_tag=msg.delivery_tag, multiple=True)

        try:
            extra = yield queue.get(timeout=1)
            self.fail("Got unexpected 11th message in original queue: " + extra.content.body)
        except Empty:
            pass

        # make sure that a single oversized message still gets delivered
        large = "abcdefghijklmnopqrstuvwxyz"
        large = large + "-" + large
        channel.basic_publish(routing_key="test-prefetch-size", content=Content(large))
        msg = yield queue.get(timeout=1)
        self.assertEqual(large, msg.content.body)

    @inlineCallbacks
    def test_get(self):
        """
        Test basic_get method
        """
        channel = self.channel
        yield channel.queue_declare(queue="test-get", exclusive=True)

        # publish some messages (no_ack=True) with persistent messaging
        for i in range(1, 11):
            msg = Content("Message %d" % i)
            msg["delivery mode"] = 2
            channel.basic_publish(routing_key="test-get", content=msg)

        # use basic_get to read back the messages, and check that we get an empty at the end
        for i in range(1, 11):
            reply = yield channel.basic_get(no_ack=True)
            self.assertEqual(reply.method.klass.name, "basic")
            self.assertEqual(reply.method.name, "get-ok")
            self.assertEqual("Message %d" % i, reply.content.body)

        reply = yield channel.basic_get(no_ack=True)
        self.assertEqual(reply.method.klass.name, "basic")
        self.assertEqual(reply.method.name, "get-empty")

        # publish some messages (no_ack=True) transient messaging
        for i in range(11, 21):
            channel.basic_publish(routing_key="test-get", content=Content("Message %d" % i))

        # use basic_get to read back the messages, and check that we get an empty at the end
        for i in range(11, 21):
            reply = yield channel.basic_get(no_ack=True)
            self.assertEqual(reply.method.klass.name, "basic")
            self.assertEqual(reply.method.name, "get-ok")
            self.assertEqual("Message %d" % i, reply.content.body)

        reply = yield channel.basic_get(no_ack=True)
        self.assertEqual(reply.method.klass.name, "basic")
        self.assertEqual(reply.method.name, "get-empty")

        # repeat for no_ack=False

        # publish some messages (no_ack=False) with persistent messaging
        for i in range(21, 31):
            msg = Content("Message %d" % i)
            msg["delivery mode"] = 2
            channel.basic_publish(routing_key="test-get", content=msg)

        # use basic_get to read back the messages, and check that we get an empty at the end
        for i in range(21, 31):
            reply = yield channel.basic_get(no_ack=False)
            self.assertEqual(reply.method.klass.name, "basic")
            self.assertEqual(reply.method.name, "get-ok")
            self.assertEqual("Message %d" % i, reply.content.body)

        reply = yield channel.basic_get(no_ack=True)
        self.assertEqual(reply.method.klass.name, "basic")
        self.assertEqual(reply.method.name, "get-empty")

        # public some messages (no_ack=False) with transient messaging
        for i in range(31, 41):
            channel.basic_publish(routing_key="test-get", content=Content("Message %d" % i))

        for i in range(31, 41):
            reply = yield channel.basic_get(no_ack=False)
            self.assertEqual(reply.method.klass.name, "basic")
            self.assertEqual(reply.method.name, "get-ok")
            self.assertEqual("Message %d" % i, reply.content.body)
            if i == 33:
                channel.basic_ack(delivery_tag=reply.delivery_tag, multiple=True)
            if i in (35, 37, 39):
                channel.basic_ack(delivery_tag=reply.delivery_tag)

        reply = yield channel.basic_get(no_ack=True)
        self.assertEqual(reply.method.klass.name, "basic")
        self.assertEqual(reply.method.name, "get-empty")

        # recover(requeue=True)
        yield channel.basic_recover(requeue=True)

        # get the unacked messages again (34, 36, 38, 40)
        for i in [34, 36, 38, 40]:
            reply = yield channel.basic_get(no_ack=False)
            self.assertEqual(reply.method.klass.name, "basic")
            self.assertEqual(reply.method.name, "get-ok")
            self.assertEqual("Message %d" % i, reply.content.body)
            channel.basic_ack(delivery_tag=reply.delivery_tag)

        reply = yield channel.basic_get(no_ack=True)
        self.assertEqual(reply.method.klass.name, "basic")
        self.assertEqual(reply.method.name, "get-empty")

        yield channel.basic_recover(requeue=True)

        reply = yield channel.basic_get(no_ack=True)
        self.assertEqual(reply.method.klass.name, "basic")
        self.assertEqual(reply.method.name, "get-empty")

    @inlineCallbacks
    def test_get_binary(self):
        """
        Test basic_get method
        """
        channel = self.channel
        yield channel.queue_declare(queue="test-get-bin", exclusive=True)

        # publish some messages (no_ack=True) with persistent messaging
        for i in range(1, 11):
            msg = "Message %d" % i
            msg_binary = Content(pickle.dumps(msg, 2))
            # msg_binary = Content(msg)
            msg_binary["delivery mode"] = 2

            channel.basic_publish(routing_key="test-get-bin", content=msg_binary)

        # use basic_get to read back the messages, and check that we get an empty at the end
        for i in range(1, 11):
            reply = yield channel.basic_get(no_ack=True)
            self.assertEqual(reply.method.klass.name, "basic")
            self.assertEqual(reply.method.name, "get-ok")
            self.assertEqual("Message %d" % i, pickle.loads(reply.content.body))

    @inlineCallbacks
    def test_fragment_body(self):
        """
        Body frames must be fragmented according to the broker frame_max
        parameter.
        """

        channel = self.channel
        yield channel.queue_declare(queue="test-get", exclusive=True)

        large_string = ''.join('x' for _ in range(self.client.MAX_LENGTH * 2))

        msg = Content(large_string)
        msg["delivery mode"] = 2
        channel.basic_publish(routing_key="test-get", content=msg)

        reply = yield channel.basic_get(no_ack=True)
        self.assertEqual(reply.method.klass.name, "basic")
        self.assertEqual(reply.method.name, "get-ok")
        self.assertEqual(large_string, reply.content.body)
