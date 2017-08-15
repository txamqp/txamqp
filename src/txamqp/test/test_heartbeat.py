from twisted.internet import reactor
from twisted.internet.defer import Deferred

from txamqp.testlib import TestBase
from txamqp.protocol import AMQClient


class SpyAMQClient(AMQClient):
    called_reschedule_check = 0
    called_send_hb = 0

    def reschedule_check_heartbeat(self, dummy=None):
        AMQClient.reschedule_check_heartbeat(self)
        self.called_reschedule_check += 1

    def send_heartbeat(self):
        AMQClient.send_heartbeat(self)
        self.called_send_hb += 1


class HeartbeatTests(TestBase):
    """
    Tests handling of heartbeat frames
    """
    heartbeat = 1
    clientClass = SpyAMQClient

    def test_heartbeat(self):
        """
        Test that heartbeat frames are sent and received
        """
        d = Deferred()

        def check_pulse(_):
            self.assertTrue(self.client.called_send_hb,  "A heartbeat frame was recently sent")
            self.assertTrue(self.client.called_reschedule_check, "A heartbeat frame was recently received")
        d.addCallback(check_pulse)
        reactor.callLater(3, d.callback, None)
        return d
