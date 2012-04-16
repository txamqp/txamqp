# coding: utf-8
from twisted.internet import defer
from txamqp.delegate import Delegate

class Closed(Exception):
    pass

class AlreadyFiredError(Exception):
    pass

class TwistedEvent(object):
    """
    An asynchronous event that is in one of three states:

      1. Not fired
      2. Fired succesfully
      3. Failed

    Clients wishing to be notified when the event has
    either occurred or failed can call wait() to receive
    a deferred that will be fired with callback(True) for
    state 2 and with errback(reason) for state 3.

    Each waiter gets an independent deferred that is not
    affected by other waiters.
    """

    def __init__(self):
        self._waiters = [] # [Deferred]
        self._result = None # or ('callback'|'errback', result)

    def fire(self):
        """
        Fire the event as successful. If the event was already fired,
        raise AlreadyFiredError.
        """
        self._fire(('callback', True))

    def fail(self, reason):
        """
        Fire the event as failed with the given reason. If the event
        was already fired, raise AlreadyFiredError.
        """
        self._fire(('errback', reason))

    def fail_if_not_fired(self, reason):
        """
        Fire the event as failed if it has not been fired. Otherwise
        do nothing.
        """
        if self._result is None:
            self.fail(reason)

    def wait(self):
        """
        Return a deferred that will be fired when the event is fired.
        """
        d = defer.Deferred()
        if self._result is None:
            self._waiters.append(d)
        else:
            self._fire_deferred(d)
        return d

    def _fire(self, result):
        if self._result is not None:
            raise AlreadyFiredError()
        self._result = result
        waiters, self._waiters = self._waiters, []
        for w in waiters:
            self._fire_deferred(w)

    def _fire_deferred(self, d):
        getattr(d, self._result[0])(self._result[1])


class TwistedDelegate(Delegate):

    def connection_start(self, ch, msg):
        ch.connection_start_ok(mechanism=self.client.mechanism,
                               response=self.client.response,
                               locale=self.client.locale)

    def connection_tune(self, ch, msg):
        self.client.MAX_LENGTH = msg.frame_max
        args = msg.channel_max, msg.frame_max, self.client.heartbeatInterval
        ch.connection_tune_ok(*args)
        self.client.started.fire()

    @defer.inlineCallbacks
    def basic_deliver(self, ch, msg):
        (yield self.client.queue(msg.consumer_tag)).put(msg)

    def basic_return_(self, ch, msg):
        self.client.basic_return_queue.put(msg)

    def channel_flow(self, ch, msg):
        ch.channel_flow_ok(active=msg.active)

    def channel_close(self, ch, msg):
        ch.channel_close_ok()
        ch.doClose(msg)

    def connection_close(self, ch, msg):
        self.client.close(msg)

    def close(self, reason):
        self.client.closed = True
        self.client.started.fail_if_not_fired(Closed(reason))
        self.client.transport.loseConnection()
