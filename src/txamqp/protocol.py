# coding: utf-8
from twisted.python import log
from twisted.internet import defer, protocol
from twisted.protocols import basic
from qpid import spec
from qpid.codec import Codec, EOF
from qpid.connection import Header, Frame, Method, Body
from qpid.message import Message
from qpid.content import Content
from qpid.peer import Closed
from qpid.queue import Empty, Closed as QueueClosed
from qpid.delegate import Delegate
from cStringIO import StringIO
import struct

class GarbageException(Exception):
    pass

class FakeEvent(object):
    def __init__(self):
        self.deferred = defer.Deferred()
        self.alreadyCalled = False

    def set(self):
        if not self.alreadyCalled:
            self.deferred.callback(None)
            self.alreadyCalled = True

    def wait(self):
        return self.deferred

class TwistedDelegate(Delegate):

    def __init__(self, client):
        Delegate.__init__(self)
        self.client = client

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

class TimeoutDeferredQueue(defer.DeferredQueue):

    END = object()

    def _timeout(self, deferred):
        if not deferred.called:
            if deferred in self.waiting:
                self.waiting.remove(deferred)
                deferred.errback(Empty())

    def _raiseIfClosed(self, result):
        if result == TimeoutDeferredQueue.END:
            self.put(TimeoutDeferredQueue.END)

            raise QueueClosed()
        else:
            return result

    def get(self, timeout=None):
        deferred = defer.DeferredQueue.get(self)

        deferred.addCallback(self._raiseIfClosed)

        if timeout:
            deferred.setTimeout(timeout, timeoutFunc=self._timeout)
        return deferred

    def close(self):
        self.put(TimeoutDeferredQueue.END)

# An AMQP channel is a virtual connection that shares the
# same socket with others channels. One can have many channels
# per connection
class AMQChannel(object):

    def __init__(self, id, outgoing):
        self.id = id
        self.outgoing = outgoing
        self.incoming = TimeoutDeferredQueue()
        self.responses = TimeoutDeferredQueue()

        self.queue = None
        
        self.closed = False
        self.reason = None
    
    def close(self, reason):
        if self.closed:
            return
        self.closed = True
        self.reason = reason
        self.incoming.close()
        self.responses.close()

    def dispatch(self, frame, work):
        payload = frame.payload
        if isinstance(payload, Method):
            if payload.method.response:
                self.queue = self.responses
            else:
                self.queue = self.incoming
                work.put(self.incoming)
        self.queue.put(frame)

    @defer.inlineCallbacks
    def invoke(self, method, args, content = None):
        if self.closed:
            raise Closed(self.reason)
        frame = Frame(self.id, Method(method, *args))
        self.outgoing.put(frame)

        if method.content:
            if content == None:
                content = Content()
            self.writeContent(method.klass, content, self.outgoing)

        try:
            # here we depend on all nowait fields being named nowait
            f = method.fields.byname["nowait"]
            nowait = args[method.fields.index(f)]
        except KeyError:
            nowait = False

        try:
            if not nowait and method.responses:
                resp = (yield self.responses.get()).payload

                if resp.method.content:
                    content = yield readContent(self.responses)
                else:
                    content = None
                if resp.method in method.responses:
                    defer.returnValue(Message(resp.method, resp.args, content))
                else:
                    raise ValueError(resp)
        except QueueClosed, e:
            if self.closed:
                raise Closed(self.reason)
            else:
                raise e

    def writeContent(self, klass, content, queue):
        size = content.size()
        header = Frame(self.id, Header(klass, content.weight(), size, **content.properties))
        queue.put(header)
        for child in content.children:
            self.writeContent(klass, child, queue)
        # should split up if content.body exceeds max frame size
        if size > 0:
            queue.put(Frame(self.id, Body(content.body)))

class FrameReceiver(protocol.Protocol, basic._PauseableMixin):

    frame_mode = False
    MAX_LENGTH = 16384
    __buffer = ''

    def __init__(self, spec):
        self.spec = spec
        self.FRAME_END = self.spec.constants.bypyname["frame_end"].id

    # packs a frame and writes it to the underlying transport
    def sendFrame(self, frame):
        data = self._packFrame(frame)
        self.transport.write(data)

    # packs a frame, see qpid.connection.Connection#write
    def _packFrame(self, frame):
        s = StringIO()
        c = Codec(s)
        c.encode_octet(self.spec.constants.bypyname[frame.payload.type].id)
        c.encode_short(frame.channel)
        frame.payload.encode(c)
        c.encode_octet(self.FRAME_END)
        data = s.getvalue()
        return data

    # packs a frame, see qpid.connection.Connection#read
    def _unpackFrame(self, data):
        try:
            s = StringIO(data)
            c = Codec(s)
            frameType = spec.pythonize(self.spec.constants.byid[c.decode_octet()].name)
            channel = c.decode_short()
            payload = Frame.DECODERS[frameType].decode(self.spec, c)
            end = c.decode_octet()
            if end != self.FRAME_END:
                raise GarbageException('frame error: expected %r, got %r' % (self.FRAME_END, end))
            frame = Frame(channel, payload)
            return frame, s.read()
        except EOF:
            return data
        except struct.error:
            return data

    def setRawMode(self):
        self.frame_mode = False

    def setFrameMode(self, extra=''):
        self.frame_mode = True
        if extra:
            return self.dataReceived(extra)

    def dataReceived(self, data):
        self.__buffer = self.__buffer + data
        while self.frame_mode and not self.paused:
            try:
                frame, self.__buffer = self._unpackFrame(self.__buffer)
            except ValueError:
                if len(self.__buffer) > self.MAX_LENGTH:
                    frame, self.__buffer = self.__buffer, ''
                    return self.frameLengthExceeded(frame)
                break
            else:
                why = self.frameReceived(frame)
                if why or self.transport and self.transport.disconnecting:
                    return why
        else:
            if not self.paused:
                data = self.__buffer
                self.__buffer = ''
                if data:
                    return self.rawDataReceived(data)

    def sendInitString(self):
        initString = "!4s4B"
        s = StringIO()
        c = Codec(s)
        c.pack(initString, "AMQP", 1, 1, self.spec.major, self.spec.minor)
        self.transport.write(s.getvalue())

@defer.inlineCallbacks
def readContent(queue):
    frame = yield queue.get()
    header = frame.payload
    children = []
    for i in range(header.weight):
        content = yield readContent(queue)
        children.append(content)
    size = header.size
    read = 0
    buf = StringIO()
    while read < size:
        body = yield queue.get()
        content = body.payload.content
        buf.write(content)
        read += len(content)
    defer.returnValue(Content(buf.getvalue(), children, header.properties.copy()))

class AMQClient(FrameReceiver):

    channelClass = AMQChannel

    def __init__(self, *args, **kwargs):
        self.vhost = kwargs.pop('vhost', 'localhost')
        FrameReceiver.__init__(self, *args, **kwargs)

        self.channelFactory = type("Channel%s" % self.spec.klass.__name__,
                                    (self.channelClass, self.spec.klass), {})
        self.channels = {}
        self.channelLock = defer.DeferredLock()

        self.outgoing = defer.DeferredQueue()
        self.work = defer.DeferredQueue()

        self.states = {}

        self.started = FakeEvent()

        self.queueLock = defer.DeferredLock()

        self.queues = {}

    @defer.inlineCallbacks
    def channel(self, id):
        yield self.channelLock.acquire()
        try:
            try:
                ch = self.channels[id]
            except KeyError:
                ch = self.channelFactory(id, self.outgoing)
                self.channels[id] = ch
        finally:
            self.channelLock.release()
        defer.returnValue(ch)

    @defer.inlineCallbacks
    def queue(self, key):
        yield self.queueLock.acquire()
        try:
            try:
                q = self.queues[key]
            except KeyError:
                q = TimeoutDeferredQueue()
                self.queues[key] = q
        finally:
            self.queueLock.release()
        defer.returnValue(q)
 
    def startQueues(self):
        self.outgoing.get().addCallback(self.writer)
        self.work.get().addCallback(self.worker)

    def close(self, reason):
        for ch in self.channels.values():
            ch.close(reason)
        self.delegate.close(reason)

    def writer(self, frame):
        self.sendFrame(frame)
        self.outgoing.get().addCallback(self.writer)

    def _worker(self, _, queue):
        self.work.get().addCallback(self.worker)

    def _raise(self, e):
        self.close(e)

    def worker(self, queue):
        self.dispatch(queue).addCallbacks(callback=self._worker, errback=self._raise, callbackArgs=[queue])

    @defer.inlineCallbacks
    def dispatch(self, queue):
        frame = yield queue.get()
        channel = yield self.channel(frame.channel)
        payload = frame.payload
        if payload.method.content:
            content = yield readContent(queue)
        else:
            content = None
        # Let the caller deal with exceptions thrown here.
        message = Message(payload.method, payload.args, content)
        self.delegate.dispatch(channel, message)

    # As soon as we connect to the target AMQP broker, send the init string
    def connectionMade(self):
        self.sendInitString()
        self.state = 'WELCOME'
        self.setFrameMode()

    def frameReceived(self, frame):
        state = self.state
        self.state = None

        state = getattr(self, 'state_' + state)(frame) or state
        if self.state is None:
            self.state = state

    def _state_WELCOME(self, ch, frame):
        ch.dispatch(frame, self.work)

    def state_WELCOME(self, frame):
        self.serverGreeting()

        self.startQueues()

        self.channel(frame.channel).addCallback(self._state_WELCOME, frame)

        return 'MESSAGE'

    def _state_MESSAGE(self, ch, frame):
        ch.dispatch(frame, self.work)
       
    def state_MESSAGE(self, frame):
        self.channel(frame.channel).addCallback(self._state_MESSAGE, frame)

        return 'MESSAGE'

    @defer.inlineCallbacks
    def start(self, response, mechanism='AMQPLAIN', locale='en_US'):
        self.response = response
        self.mechanism = mechanism
        self.locale = locale

        yield self.started.wait()

        channel0 = yield self.channel(0)
        yield channel0.connection_open(self.vhost)
