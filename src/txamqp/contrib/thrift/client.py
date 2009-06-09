from twisted.internet import defer

from txamqp.client import TwistedDelegate


class ThriftTwistedDelegate(TwistedDelegate):

    @defer.inlineCallbacks
    def basic_return_(self, ch, msg):
        try:
            thriftClientName = msg.content['headers']['thriftClientName']
        except KeyError:
            from twisted.python import log
            if 'headers' in msg.content:
                log.msg("'headers' not in msg.content: %r" % msg.content)
            else:
                log.msg("'thriftClientName' not in msg.content headers: %r" %
                        msg.content['headers'])
        else:
            (yield self.client.thriftBasicReturnQueue(thriftClientName))\
                   .put(msg)
