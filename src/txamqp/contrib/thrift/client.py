from twisted.internet import defer
from twisted.python import log

from txamqp.client import TwistedDelegate


class ThriftTwistedDelegate(TwistedDelegate):

    @defer.inlineCallbacks
    def basic_return_(self, ch, msg):
        try:
            headers = msg.content['headers']
        except KeyError:
            log.msg("'headers' not in msg.content: %r" % msg.content)
        else:
            try:
                thriftClientName = headers['thriftClientName']
            except KeyError:
                log.msg("'thriftClientName' not in msg.content['headers']: %r" %
                        msg.content['headers'])
            else:
                (yield self.client.thriftBasicReturnQueue(thriftClientName))\
                       .put(msg)
            
