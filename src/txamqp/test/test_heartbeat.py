from time import time
import txamqp
from txamqp.testlib import TestBase
from txamqp.protocol import AMQClient, TwistedDelegate
from twisted.internet import reactor, protocol
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue

class SpyAMQClient(AMQClient):
    called_reschedule_check = 0
    called_send_hb = 0
    
    def reschedule_checkHB(self, dummy=None):
        AMQClient.reschedule_checkHB(self)
        self.called_reschedule_check += 1
    
    def sendHeartbeat(self):
        AMQClient.sendHeartbeat(self)
        self.called_send_hb += 1

class HeartbeatTests(TestBase):

    heartbeat = 1
    clientClass = SpyAMQClient

    """
    Tests handling of heartbeat frames
    """
    def test_heartbeat(self):
        """
        Test that heartbeat frames are sent and received
        """
        d = Deferred()
        def checkPulse(dummy):
            t = time()
            self.assertTrue(self.client.called_send_hb,
                        "A heartbeat frame was recently sent")
            self.assertTrue(self.client.called_reschedule_check,
                        "A heartbeat frame was recently received")
        d.addCallback(checkPulse)
        reactor.callLater(3, d.callback, None)
        return d
