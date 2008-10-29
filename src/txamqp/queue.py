# coding: utf-8
from twisted.internet.defer import DeferredQueue

class Empty(Exception):
    pass

class Closed(Exception):
    pass

class TimeoutDeferredQueue(DeferredQueue):

    END = object()

    def _timeout(self, deferred):
        if not deferred.called:
            if deferred in self.waiting:
                self.waiting.remove(deferred)
                deferred.errback(Empty())

    def _raiseIfClosed(self, result):
        if result == TimeoutDeferredQueue.END:
            self.put(TimeoutDeferredQueue.END)

            raise Closed()
        else:
            return result

    def get(self, timeout=None):
        deferred = DeferredQueue.get(self)

        deferred.addCallback(self._raiseIfClosed)

        if timeout:
            deferred.setTimeout(timeout, timeoutFunc=self._timeout)
        return deferred

    def close(self):
        self.put(TimeoutDeferredQueue.END)
