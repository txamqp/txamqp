from twisted.trial.unittest import TestCase
from twisted.internet.task import Clock
from twisted.internet.error import ConnectionLost
from twisted.logger import Logger

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate, Closed, ConnectionClosed, ChannelClosed
from txamqp.testing import AMQPump
from txamqp.spec import DEFAULT_SPEC, load
from txamqp.queue import Closed as QueueClosed


class AMQClientTest(TestCase):
    """Unit tests for the AMQClient protocol."""

    def setUp(self):
        super(AMQClientTest, self).setUp()
        self.delegate = TwistedDelegate()
        self.clock = Clock()
        self.heartbeat = 1
        self.protocol = AMQClient(
            self.delegate, "/", load(DEFAULT_SPEC), clock=self.clock,
            heartbeat=self.heartbeat)
        self.transport = AMQPump(Logger())
        self.transport.connect(self.protocol)

    def test_connection_close(self):
        """Test handling a connection-close method sent by the broker."""
        self.transport.channel(0).connection_close()
        # We send close-ok before shutting down the connection
        frame = self.transport.outgoing[0][0]
        self.assertEqual("close-ok", frame.payload.method.name)
        self.assertTrue(self.protocol.closed)
        channel0 = self.successResultOf(self.protocol.channel(0))
        self.assertTrue(channel0.closed)

    def test_connection_close_raises_error(self):
        """Test receiving a connection-close method raises ConnectionClosed."""
        channel = self.successResultOf(self.protocol.channel(0))
        d = channel.basic_consume(queue="test-queue")
        self.transport.channel(0).connection_close(reply_code=320)
        failure = self.failureResultOf(d)
        self.assertIsInstance(failure.value, ConnectionClosed)

    def test_close(self):
        """Test explicitely closing a client."""
        d = self.protocol.close()

        # Since 'within' defaults to 0, no outgoing 'close' frame is there.
        self.assertEqual({}, self.transport.outgoing)

        self.assertIsNone(self.successResultOf(d))
        self.assertTrue(self.protocol.closed)

    def test_close_within(self):
        """Test closing a client cleanly."""
        d = self.protocol.close(within=1)

        # Since we passed within=1, we have an outgoing 'close' frame.
        frame = self.transport.outgoing[0][0]
        self.assertEqual("close", frame.payload.method.name)

        # At this point the client is not yet closed, since we're waiting for
        # the 'close-ok' acknowledgement from the broker.
        self.assertFalse(self.protocol.closed)

        self.transport.channel(0).connection_close_ok()
        self.assertIsNone(self.successResultOf(d))
        self.assertTrue(self.protocol.closed)
        self.assertEqual([], self.clock.calls)

    def test_close_within_hits_timeout(self):
        """Test trying to close a client cleanly but hitting the timeout."""
        d = self.protocol.close(within=1)

        self.clock.advance(1)
        self.assertIsNone(self.successResultOf(d))
        self.assertTrue(self.protocol.closed)

    def test_close_closes_channels(self):
        """Test closing a client also closes channels."""
        channel = self.successResultOf(self.protocol.channel(0))
        self.protocol.close()
        d = channel.basic_consume(queue="test-queue")
        failure = self.failureResultOf(d)
        self.assertIsInstance(failure.value, Closed)

    def test_close_closes_queues(self):
        """Test closing a client also closes queues."""
        queue = self.successResultOf(self.protocol.queue("tag"))
        d = queue.get()
        self.protocol.close()
        failure = self.failureResultOf(d)
        self.assertIsInstance(failure.value, QueueClosed)

    def test_hearbeat_check_failure(self):
        """Test closing a client after a heartbeat check failure."""
        self.protocol.started.fire()
        channel = self.successResultOf(self.protocol.channel(0))
        d = channel.basic_consume(queue="test-queue")
        self.clock.advance(self.heartbeat * AMQClient.MAX_UNSEEN_HEARTBEAT)
        self.assertTrue(self.protocol.closed)
        failure = self.failureResultOf(d)
        self.assertIsInstance(failure.value, Closed)
        self.assertTrue(self.transport.aborted)

    def test_connection_lost(self):
        """Test closing a client after the connection is lost."""
        channel = self.successResultOf(self.protocol.channel(0))
        d = channel.basic_consume(queue="test-queue")
        self.transport.abortConnection()
        self.assertTrue(self.protocol.closed)
        failure = self.failureResultOf(d)
        self.assertIsInstance(failure.value, Closed)
        self.assertIsInstance(failure.value.args[0].value, ConnectionLost)

    def test_channel_close(self):
        """Test receiving a channel-close method raises ChannelClosed."""
        channel = self.successResultOf(self.protocol.channel(0))
        d = channel.basic_consume(queue="non-existing-queue")
        self.transport.channel(0).channel_close(reply_code=404)
        failure = self.failureResultOf(d)
        self.assertIsInstance(failure.value, ChannelClosed)

    def test_sending_method_on_closed_channel(self):
        """Sending a method on a closed channel fails immediately."""
        channel = self.successResultOf(self.protocol.channel(0))
        self.transport.channel(0).connection_close(reply_code=320)
        self.transport.outgoing.clear()
        d = channel.basic_consume(queue="test-queue")
        # No frames were sent
        self.assertEqual({}, self.transport.outgoing)
        failure = self.failureResultOf(d)
        self.assertIsInstance(failure.value, ConnectionClosed)

    def test_disconnected_event(self):
        """Test disconnected event fired after the connection is lost."""
        deferred = self.protocol.disconnected.wait()
        self.protocol.close()
        self.assertTrue(self.successResultOf(deferred))
