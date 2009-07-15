from time import time
from txamqp.testlib import TestBase

from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks

class HeartbeatTests(TestBase):
    """
    Tests handling of heartbeat frames
    """

    @inlineCallbacks
    def setUp(self):
        """ Set up a heartbeat frame per second """
        self.client = yield self.connect(heartbeat=1)

        self.channel = yield self.client.channel(1)
        yield self.channel.channel_open()

    def test_heartbeat(self):
        """
        Test that heartbeat frames are sent and received
        """
        d = Deferred()
        def checkPulse(dummy):
            t = time()
            self.assertTrue(self.client.lastSent > t - 2,
                        "A heartbeat frame was recently sent")
            self.assertTrue(self.client.lastReceived > t - 2,
                        "A heartbeat frame was recently received")
        d.addCallback(checkPulse)
        reactor.callLater(3, d.callback, None)
        return d
