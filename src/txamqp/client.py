# coding: utf-8
from twisted.internet import defer
from txamqp.delegate import Delegate

class Closed(Exception):
    pass

class TwistedEvent(object):
    def __init__(self):
        self.deferred = defer.Deferred()
        self.alreadyCalled = False

    def set(self):
        deferred, self.deferred = self.deferred, defer.Deferred()
        deferred.callback(None)

    def wait(self):
        return self.deferred

class TwistedDelegate(Delegate):

    def connection_start(self, ch, msg):
        ch.connection_start_ok(mechanism=self.client.mechanism,
                               response=self.client.response,
                               locale=self.client.locale)

    def connection_tune(self, ch, msg):
        ch.connection_tune_ok(*msg.fields)
        self.client.started.set()

    @defer.inlineCallbacks
    def basic_deliver(self, ch, msg):
        (yield self.client.queue(msg.consumer_tag)).put(msg)

    def channel_close(self, ch, msg):
        ch.close(msg)

    def connection_close(self, ch, msg):
        self.client.close(msg)

    def close(self, reason):
        self.client.closed = True
        self.client.started.set()
