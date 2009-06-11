# coding: utf-8
from twisted.python import log
from twisted.internet import defer, protocol, reactor
from twisted.protocols import basic
from txamqp import spec
from txamqp.codec import Codec, EOF
from txamqp.connection import Header, Frame, Method, Body, Heartbeat
from txamqp.message import Message
from txamqp.content import Content
from txamqp.queue import TimeoutDeferredQueue, Empty, Closed as QueueClosed
from txamqp.client import TwistedEvent, TwistedDelegate, Closed
from cStringIO import StringIO
import struct
from time import time

class GarbageException(Exception):
    pass

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
    MAX_LENGTH = 4096
    HEADER_LENGTH = 1 + 2 + 4 + 1
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

    # unpacks a frame, see qpid.connection.Connection#read
    def _unpackFrame(self, data):
        s = StringIO(data)
        c = Codec(s)
        frameType = spec.pythonize(self.spec.constants.byid[c.decode_octet()].name)
        channel = c.decode_short()
        payload = Frame.DECODERS[frameType].decode(self.spec, c)
        end = c.decode_octet()
        if end != self.FRAME_END:
            raise GarbageException('frame error: expected %r, got %r' % (self.FRAME_END, end))
        frame = Frame(channel, payload)
        return frame

    def setRawMode(self):
        self.frame_mode = False

    def setFrameMode(self, extra=''):
        self.frame_mode = True
        if extra:
            return self.dataReceived(extra)

    def dataReceived(self, data):
        self.__buffer = self.__buffer + data
        while self.frame_mode and not self.paused:
            sz = len(self.__buffer) - self.HEADER_LENGTH
            if sz >= 0:
                length, = struct.unpack("!I", self.__buffer[3:7]) # size = 4 bytes
                if sz >= length:
                    packet = self.__buffer[:self.HEADER_LENGTH + length]
                    self.__buffer = self.__buffer[self.HEADER_LENGTH + length:]
                    frame = self._unpackFrame(packet)

                    why = self.frameReceived(frame)
                    if why or self.transport and self.transport.disconnecting:
                        return why
                    else:
                        continue
            if len(self.__buffer) > self.MAX_LENGTH:
                frame, self.__buffer = self.__buffer, ''
                return self.frameLengthExceeded(frame)
            break
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

    def __init__(self, delegate, vhost, heartbeat=0, *args, **kwargs):
        FrameReceiver.__init__(self, *args, **kwargs)
        self.delegate = delegate

        # XXX Cyclic dependency
        self.delegate.client = self

        self.vhost = vhost

        self.channelFactory = type("Channel%s" % self.spec.klass.__name__,
                                    (self.channelClass, self.spec.klass), {})
        self.channels = {}
        self.channelLock = defer.DeferredLock()

        self.outgoing = defer.DeferredQueue()
        self.work = defer.DeferredQueue()

        self.started = TwistedEvent()
        self.lastSent = time()
        self.lastReceived = time()

        self.queueLock = defer.DeferredLock()

        self.queues = {}

        self.outgoing.get().addCallback(self.writer)
        self.work.get().addCallback(self.worker)
        self.heartbeatInterval = heartbeat
        self.pendingHeartbeat = None
        if self.heartbeatInterval > 0:
            self.started.wait().addCallback(self.heartbeatHandler)

    def check_0_8(self):
        return (self.spec.minor, self.spec.major) == (0, 8)

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
 
    def close(self, reason):
        for ch in self.channels.values():
            ch.close(reason)
        for q in self.queues.values():
            q.close()
        self.delegate.close(reason)

    def writer(self, frame):
        self.sendFrame(frame)
        self.outgoing.get().addCallback(self.writer)

    def worker(self, queue):
        d = self.dispatch(queue)
        def cb(ign):
            self.work.get().addCallback(self.worker)
        d.addCallback(cb)
        d.addErrback(self.close)

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
        self.setFrameMode()

    def connectionLost(self, reason):
        if self.pendingHeartbeat is not None and self.pendingHeartbeat.active():
            self.pendingHeartbeat.cancel()
            self.pendingHeartbeat = None

    def frameReceived(self, frame):
        self.processFrame(frame)

    def sendFrame(self, frame):
        self.lastSent = time()
        FrameReceiver.sendFrame(self, frame)

    @defer.inlineCallbacks
    def processFrame(self, frame):
        self.lastReceived = time()
        ch = yield self.channel(frame.channel)
        if frame.payload.type != Frame.HEARTBEAT:
            ch.dispatch(frame, self.work)

    @defer.inlineCallbacks
    def authenticate(self, username, password, mechanism='AMQPLAIN', locale='en_US'):
        if self.check_0_8():
            response = {"LOGIN": username, "PASSWORD": password}
        else:
            response = "\0" + username + "\0" + password

        yield self.start(response, mechanism, locale)

    @defer.inlineCallbacks
    def start(self, response, mechanism='AMQPLAIN', locale='en_US'):
        self.response = response
        self.mechanism = mechanism
        self.locale = locale

        yield self.started.wait()

        channel0 = yield self.channel(0)
        yield channel0.connection_open(self.vhost)

    def heartbeatHandler (self, dummy=None):
        now = time()
        if self.lastSent + self.heartbeatInterval < now:
            self.sendFrame(Frame(0, Heartbeat()))
        if self.lastReceived + self.heartbeatInterval * 3 < now:
            self.transport.loseConnection()
        tple = None
        if self.transport.connected:
            tple = reactor.callLater(self.heartbeatInterval, self.heartbeatHandler)
        self.pendingHeartbeat = tple
            
