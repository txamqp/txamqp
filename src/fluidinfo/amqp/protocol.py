# coding: utf-8
from twisted.python import log
from twisted.internet import defer, protocol
from twisted.protocols import basic
from qpid import spec, codec, connection
from qpid.message import Message
from qpid.content import Content
from qpid.peer import Closed
from StringIO import StringIO
import struct

class GarbageException(Exception):
    pass

# An AMQP channel is a virtual connection that shares the
# same socket with others channels. One can have many channels
# per connection
class AMQChannel(object):

    def __init__(self, id, proto, spec):
        self.id = id
        self.proto = proto
        self.spec = spec
        self.deferred = defer.Deferred()
        self.incoming = defer.DeferredQueue()
        self.worker()
        self.closed = False
        self.reason = None

    def _dispatchContent(self, content, payload):
        message = Message(payload.method, payload.args, content)
        self.messageReceived(message)

    def worker(self, frame=None):
        if frame:
            payload = frame.payload
            if payload.method.content:
                d = readContent(self.incoming)
            else:
                d = defer.succeed(None)
            d.addCallback(self._dispatchContent, payload).addCallback(self.worker)
        else:
            self.incoming.get().addCallback(self.worker)

    def sendFrame(self, frame):
        self.proto.sendFrame(frame)

        payload = frame.payload
        if isinstance(payload, connection.Method):
            if len(payload.method.responses) > 0:
                self.deferred = defer.Deferred()
        return self.deferred
 
    def close(self, reason):
        self.closed = True
        self.reason = reason
        raise Closed(self.reason)

    def invoke(self, method, args, content=None):
        if self.closed:
            raise Closed(self.reason)
        frame = connection.Frame(self.id, connection.Method(method, *args))

        d = self.sendFrame(frame)

        if method.content:
          if content is None:
            content = Content()
          self.writeContent(method.klass, content)

        return d

    def writeContent(self, klass, content):
        size = content.size()
        header = connection.Frame(self.id, connection.Header(klass, content.weight(), size, **content.properties))
        self.sendFrame(header)
        for child in content.children:
            self.writeContent(klass, child)
        # should split up if content.body exceeds max frame size
        if size > 0:
            self.sendFrame(connection.Frame(self.id, connection.Body(content.body)))

    def frameReceived(self, frame):
        self.incoming.put(frame)
 
    def messageReceived(self, message):
        deferred, self.deferred = self.deferred, defer.Deferred()
        deferred.callback(message)
        return deferred

class FrameReceiver(protocol.Protocol, basic._PauseableMixin):

    frame_mode = False
    MAX_LENGTH = 16384
    __buffer = ''

    def __init__(self, spec, *args, **kwargs):
        self.spec = spec
        self.FRAME_END = self.spec.constants.bypyname["frame_end"].id

    # packs a frame and writes it to the underlying transport
    def sendFrame(self, frame):
        data = self._packFrame(frame)
        self.transport.write(data)

    # packs a frame, see qpid.connection.Connection#write
    def _packFrame(self, frame):
        s = StringIO()
        c = codec.Codec(s)
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
            c = codec.Codec(s)
            frameType = spec.pythonize(self.spec.constants.byid[c.decode_octet()].name)
            channel = c.decode_short()
            payload = connection.Frame.DECODERS[frameType].decode(self.spec, c)
            end = c.decode_octet()
            if end != self.FRAME_END:
                raise GarbageException('frame error: expected %r, got %r' % (self.FRAME_END, end))
            frame = connection.Frame(channel, payload)
            return frame, data[s.pos:]
        except codec.EOF:
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
        c = codec.Codec(s)
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
        self.incoming = defer.DeferredQueue()

        self.channelFactory = type("Channel%s" % self.spec.klass.__name__,
                                    (self.channelClass, self.spec.klass), {})
        self.channels = {}
        self.lock = defer.DeferredLock()

    def _channel(self, lock, id):
        try:
            try:
                ch = self.channels[id]
            except KeyError:
                ch = self.channelFactory(id, self, self.spec)
                self.channels[id] = ch
        finally:
            lock.release()
        return ch
 
    def channel(self, id):
        return self.lock.acquire().addCallback(self._channel, id)

    # As soon as we connect to the target AMQP broker, send the init string
    def connectionMade(self):
        self.sendInitString()
        self.state = 'WELCOME'
        self.deferred = defer.Deferred()
        self.setFrameMode()

    def frameReceived(self, frame):
        state = self.state
        self.state = None

        state = getattr(self, 'state_' + state)(frame) or state
        if self.state is None:
            self.state = state

    def state_WELCOME(self, frame):
        self.serverGreeting()

        return 'MESSAGE'

    def _state_MESSAGE(self, channel, frame):
        channel.frameReceived(frame)
       
    def state_MESSAGE(self, frame):
        self.channel(frame.channel).addCallback(self._state_MESSAGE, frame)

        return 'MESSAGE'

    @defer.inlineCallbacks
    def start(self, response, mechanism='AMQPLAIN', locale='en_US'):
        channel0 = yield self.channel(0)
        reply = yield channel0.connection_start_ok(mechanism=mechanism, response=response, locale=locale)
        channel0.connection_tune_ok(*reply.fields)
        yield channel0.connection_open(self.vhost)
