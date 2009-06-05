from twisted.internet import defer

from txamqp.client import TwistedDelegate


class ThriftTwistedDelegate(TwistedDelegate):

    @defer.inlineCallbacks
    def basic_return_(self, ch, msg):
        thriftClientName = msg.content['headers']['thriftClientName']
        (yield self.client.thriftBasicReturnQueue(thriftClientName)).put(msg)
