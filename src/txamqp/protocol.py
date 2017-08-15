# coding: utf-8
from io import BytesIO
import struct
from time import time

from twisted.internet import defer, protocol
from twisted.internet.task import LoopingCall
from twisted.internet.error import ConnectionDone
from twisted.protocols import basic
from twisted.python.failure import Failure
import six

from txamqp import spec
from txamqp.codec import Codec
from txamqp.connection import Header, Frame, Method, Body, Heartbeat
from txamqp.message import Message
from txamqp.content import Content
from txamqp.queue import TimeoutDeferredQueue, Closed as QueueClosed
from txamqp.client import TwistedEvent, Closed, ConnectionClosed, ChannelClosed


class GarbageException(Exception):
    pass


# An AMQP channel is a virtual connection that shares the
# same socket with others channels. One can have many channels
# per connection
class AMQChannel(object):
    def __init__(self, id, outgoing, client):
        self.id = id
        self.outgoing = outgoing
        self.client = client
        self.incoming = TimeoutDeferredQueue()
        self.responses = TimeoutDeferredQueue()

        self.queue = None

        self._closing = False
        self.closed = False
        self.reason = None

    def close(self, reason):
        """Explicitly close a channel"""
        self._closing = True
        self.do_close(reason)
        self._closing = False

    def do_close(self, reason):
        """Called when channel_close() is received"""
        if self.closed:
            return
        self.closed = True
        self.reason = reason
        self.incoming.close()
        self.responses.close()
        if not self._closing:
            self.client.channel_failed(self, Failure(reason))

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
    def invoke(self, method, args, content=None):
        if self.closed:
            self._raise_closed(self.reason)
        frame = Frame(self.id, Method(method, *args))
        self.outgoing.put(frame)

        if method.content:
            if content is None:
                content = Content()
            self.write_content(method.klass, content, self.outgoing)

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
                    content = yield read_content(self.responses)
                else:
                    content = None
                if resp.method in method.responses:
                    defer.returnValue(Message(resp.method, resp.args, content))
                else:
                    raise ValueError(resp)
        except QueueClosed as e:
            if self.closed:
                self._raise_closed(self.reason)
            else:
                raise e

    def write_content(self, klass, content, queue):
        size = content.size()
        header = Frame(self.id, Header(klass, content.weight(), size, **content.properties))
        queue.put(header)
        for child in content.children:
            self.write_content(klass, child, queue)
        if size > 0:
            max_chunk_size = self.client.MAX_LENGTH - 8
            for i in range(0, len(content.body), max_chunk_size):
                chunk = content.body[i:i + max_chunk_size]
                queue.put(Frame(self.id, Body(chunk)))

    @staticmethod
    def _raise_closed(reason):
        """Raise the appropriate Closed-based error for the given reason."""
        if isinstance(reason, Message):
            if reason.method.klass.name == "channel":
                raise ChannelClosed(reason)
            elif reason.method.klass.name == "connection":
                raise ConnectionClosed(reason)
        raise Closed(reason)


class FrameReceiver(protocol.Protocol, basic._PauseableMixin):

    frame_mode = False
    MAX_LENGTH = 4096
    HEADER_LENGTH = 1 + 2 + 4 + 1
    __buffer = b''

    def __init__(self, spec):
        self.spec = spec
        self.FRAME_END = self.spec.constants.bypyname["frame_end"].id

    # packs a frame and writes it to the underlying transport
    def send_frame(self, frame):
        data = self._pack_frame(frame)
        self.transport.write(data)

    # packs a frame, see qpid.connection.Connection#write
    def _pack_frame(self, frame):
        s = BytesIO()
        c = Codec(s)
        c.encode_octet(self.spec.constants.bypyname[frame.payload.type].id)
        c.encode_short(frame.channel)
        frame.payload.encode(c)
        c.encode_octet(self.FRAME_END)
        data = s.getvalue()
        return data

    # unpacks a frame, see qpid.connection.Connection#read
    def _unpack_frame(self, data):
        s = BytesIO(data)
        c = Codec(s)
        frame_type = spec.pythonize(self.spec.constants.byid[c.decode_octet()].name)
        channel = c.decode_short()
        payload = Frame.DECODERS[frame_type].decode(self.spec, c)
        end = c.decode_octet()
        if end != self.FRAME_END:
            raise GarbageException('frame error: expected %r, got %r' % (self.FRAME_END, end))
        frame = Frame(channel, payload)
        return frame

    def set_raw_mode(self):
        self.frame_mode = False

    def set_frame_mode(self, extra=''):
        self.frame_mode = True
        if extra:
            return self.dataReceived(extra)

    def frame_received(self, frame):
        raise NotImplementedError()

    def dataReceived(self, data):
        self.__buffer = self.__buffer + data
        while self.frame_mode and not self.paused:
            sz = len(self.__buffer) - self.HEADER_LENGTH
            if sz >= 0:
                length, = struct.unpack("!I", self.__buffer[3:7])  # size = 4 bytes
                if sz >= length:
                    packet = self.__buffer[:self.HEADER_LENGTH + length]
                    self.__buffer = self.__buffer[self.HEADER_LENGTH + length:]
                    frame = self._unpack_frame(packet)

                    why = self.frame_received(frame)
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

    def send_init_string(self):
        s = BytesIO()
        c = Codec(s)
        c.pack("!4s4B", b"AMQP", 1, 1, self.spec.major, self.spec.minor)
        self.transport.write(s.getvalue())


@defer.inlineCallbacks
def read_content(queue):
    frame = yield queue.get()
    header = frame.payload
    children = []
    for i in range(header.weight):
        content = yield read_content(queue)
        children.append(content)
    size = header.size
    read = 0
    buf = six.StringIO()
    while read < size:
        body = yield queue.get()
        content = body.payload.content
        buf.write(content)
        read += len(content)
    defer.returnValue(Content(buf.getvalue(), children, header.properties.copy()))


class AMQClient(FrameReceiver):
    channelClass = AMQChannel

    # Max unreceived heartbeat frames. The AMQP standard says it's 3.
    MAX_UNSEEN_HEARTBEAT = 3

    def __init__(self, delegate, vhost, spec, heartbeat=0, clock=None, insist=False):
        FrameReceiver.__init__(self, spec)
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
        self.disconnected = TwistedEvent()  # Fired upon connection shutdown
        self.closed = False

        self.queueLock = defer.DeferredLock()
        self.basic_return_queue = TimeoutDeferredQueue()

        self.queues = {}

        self.outgoing.get().addCallback(self.writer)
        self.work.get().addCallback(self.worker)
        self.heartbeatInterval = heartbeat
        self.insist = insist
        if clock is None:
            from twisted.internet import reactor
            clock = reactor
        self.clock = clock
        if self.heartbeatInterval > 0:
            self.checkHB = self.clock.callLater(self.heartbeatInterval *
                                                self.MAX_UNSEEN_HEARTBEAT, self.check_heartbeat)
            self.sendHB = LoopingCall(self.send_heartbeat)
            self.sendHB.clock = self.clock
            d = self.started.wait()
            d.addCallback(lambda _: self.reschedule_send_heartbeat())
            d.addCallback(lambda _: self.reschedule_check_heartbeat())
            # If self.started fails, don't start the heartbeat.
            d.addErrback(lambda _: None)

    def reschedule_send_heartbeat(self):
        if self.heartbeatInterval > 0:
            if self.sendHB.running:
                self.sendHB.stop()
            self.sendHB.start(self.heartbeatInterval, now=False)

    def reschedule_check_heartbeat(self):
        if self.checkHB.active():
            self.checkHB.cancel()
        self.checkHB = self.clock.callLater(self.heartbeatInterval *
                                            self.MAX_UNSEEN_HEARTBEAT, self.check_heartbeat)

    def check_0_8(self):
        return (self.spec.minor, self.spec.major) == (0, 8)

    @defer.inlineCallbacks
    def channel(self, id):
        yield self.channelLock.acquire()
        try:
            try:
                ch = self.channels[id]
            except KeyError:
                ch = self.channelFactory(id, self.outgoing, self)
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
                q = TimeoutDeferredQueue(clock=self.clock)
                self.queues[key] = q
        finally:
            self.queueLock.release()
        defer.returnValue(q)

    @defer.inlineCallbacks
    def close(self, reason=None, within=0):
        """Explicitely close the connection.

        @param reason: Optional closing reason. If not given, ConnectionDone
            will be used.
        @param within: Shutdown the client within this amount of seconds. If
            zero (the default), all channels and queues will be closed
            immediately. If greater than 0, try to close the AMQP connection
            cleanly, by sending a "close" method and waiting for "close-ok". If
            no reply is received within the given amount of seconds, the
            transport will be forcely shutdown.
        """
        if self.closed:
            return

        if reason is None:
            reason = ConnectionDone()

        if within > 0:
            channel0 = yield self.channel(0)
            deferred = channel0.connection_close()
            call = self.clock.callLater(within, deferred.cancel)
            try:
                yield deferred
            except defer.CancelledError:
                pass
            else:
                call.cancel()

        self.do_close(reason)

    def do_close(self, reason):
        """Called when connection_close() is received"""
        # Let's close all channels and queues, since we don't want to write
        # any more data and no further read will happen.
        for ch in self.channels.values():
            ch.close(reason)
        for q in self.queues.values():
            q.close()
        self.delegate.close(reason)

    def writer(self, frame):
        self.send_frame(frame)
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
            content = yield read_content(queue)
        else:
            content = None
        # Let the caller deal with exceptions thrown here.
        message = Message(payload.method, payload.args, content)
        self.delegate.dispatch(channel, message)

    # As soon as we connect to the target AMQP broker, send the init string
    def connectionMade(self):
        self.send_init_string()
        self.set_frame_mode()

    def frame_received(self, frame):
        self.process_frame(frame)

    def send_frame(self, frame):
        if frame.payload.type != Frame.HEARTBEAT:
            self.reschedule_send_heartbeat()
        FrameReceiver.send_frame(self, frame)

    @defer.inlineCallbacks
    def process_frame(self, frame):
        ch = yield self.channel(frame.channel)
        if frame.payload.type == Frame.HEARTBEAT:
            self.last_heartbeat_received = time()
        else:
            ch.dispatch(frame, self.work)
        if self.heartbeatInterval > 0 and not self.closed:
            self.reschedule_check_heartbeat()

    @defer.inlineCallbacks
    def authenticate(self, username, password, mechanism='AMQPLAIN', locale='en_US'):
        if mechanism == 'AMQPLAIN':
            response = {"LOGIN": username, "PASSWORD": password}
        elif mechanism == 'PLAIN':
            response = "\0" + username + "\0" + password
        else:
            raise ValueError('Unknown mechanism:'+mechanism)

        yield self.start(response, mechanism, locale)

    @defer.inlineCallbacks
    def start(self, response, mechanism='AMQPLAIN', locale='en_US'):
        self.response = response
        self.mechanism = mechanism
        self.locale = locale

        yield self.started.wait()

        channel0 = yield self.channel(0)
        if self.check_0_8():
            result = yield channel0.connection_open(self.vhost, insist=self.insist)
        else:
            result = yield channel0.connection_open(self.vhost)
        defer.returnValue(result)

    def send_heartbeat(self):
        self.send_frame(Frame(0, Heartbeat()))
        self.last_heartbeat_sent = time()

    def check_heartbeat(self):
        if self.checkHB.active():
            self.checkHB.cancel()
        # Abort the connection, since the other pear is unresponsive and
        # there's no point in shutting down cleanly and trying to flush
        # pending data.
        self.transport.abortConnection()

    def connectionLost(self, reason):
        if self.heartbeatInterval > 0:
            if self.sendHB.running:
                self.sendHB.stop()
            if self.checkHB.active():
                self.checkHB.cancel()
        self.close(reason)
        self.disconnected.fire()

    def channel_failed(self, channel, reason):
        """Unexpected channel close"""
        pass
