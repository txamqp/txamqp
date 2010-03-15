from zope.interface import Interface, Attribute
from txamqp.protocol import AMQClient
from txamqp.contrib.thrift.transport import TwistedAMQPTransport
from txamqp.queue import TimeoutDeferredQueue, Closed

from twisted.internet import defer
from twisted.python import log

from thrift.transport import TTransport

class ThriftAMQClient(AMQClient):

    def __init__(self, *args, **kwargs):
        AMQClient.__init__(self, *args, **kwargs)

        if self.check_0_8():
            self.replyToField = "reply to"
        else:
            self.replyToField = "reply-to"

        self.thriftBasicReturnQueueLock = defer.DeferredLock()
        self.thriftBasicReturnQueues = {}

    @defer.inlineCallbacks
    def thriftBasicReturnQueue(self, key):
        yield self.thriftBasicReturnQueueLock.acquire()
        try:
            try:
                q = self.thriftBasicReturnQueues[key]
            except KeyError:
                q = TimeoutDeferredQueue()
                self.thriftBasicReturnQueues[key] = q
        finally:
            self.thriftBasicReturnQueueLock.release()
        defer.returnValue(q)

    @defer.inlineCallbacks
    def createThriftClient(self, responsesExchange, serviceExchange,
        routingKey, clientClass, channel=1, responseQueue=None, iprot_factory=None,
        oprot_factory=None):

        channel = yield self.channel(channel)

        if responseQueue is None:
            reply = yield channel.queue_declare(exclusive=True,
                auto_delete=True)

            responseQueue = reply.queue

            yield channel.queue_bind(queue=responseQueue,
                exchange=responsesExchange, routing_key=responseQueue)

        reply = yield channel.basic_consume(queue=responseQueue)

        log.msg("Consuming messages on queue: %s" % responseQueue)

        thriftClientName = clientClass.__name__ + routingKey
        
        amqpTransport = TwistedAMQPTransport(
            channel, serviceExchange, routingKey, clientName=thriftClientName,
            replyTo=responseQueue, replyToField=self.replyToField)

        if iprot_factory is None:
            iprot_factory = self.factory.iprot_factory

        if oprot_factory is None:
            oprot_factory = self.factory.oprot_factory

        thriftClient = clientClass(amqpTransport, oprot_factory)

        queue = yield self.queue(reply.consumer_tag)
        d = queue.get()
        d.addCallback(self.parseClientMessage, channel, queue, thriftClient,
            iprot_factory=iprot_factory)
        d.addErrback(self.catchClosedClientQueue)
        d.addErrback(self.handleClientQueueError)


        basicReturnQueue = yield self.thriftBasicReturnQueue(thriftClientName)
        
        d = basicReturnQueue.get()
        d.addCallback(self.parseClientUnrouteableMessage, channel,
            basicReturnQueue, thriftClient, iprot_factory=iprot_factory)
        d.addErrback(self.catchClosedClientQueue)
        d.addErrback(self.handleClientQueueError)

        defer.returnValue(thriftClient)

    def parseClientMessage(self, msg, channel, queue, thriftClient,
                           iprot_factory=None):
        deliveryTag = msg.delivery_tag
        tr = TTransport.TMemoryBuffer(msg.content.body)
        if iprot_factory is None:
            iprot = self.factory.iprot_factory.getProtocol(tr)
        else:
            iprot = iprot_factory.getProtocol(tr)
        (fname, mtype, rseqid) = iprot.readMessageBegin()

        if rseqid in thriftClient._reqs:
            # log.msg('Got reply: fname = %r, rseqid = %s, mtype = %r, routing key = %r, client = %r, msg.content.body = %r' % (fname, rseqid, mtype, msg.routing_key, thriftClient, msg.content.body))
            pass
        else:
            log.msg('Missing rseqid! fname = %r, rseqid = %s, mtype = %r, routing key = %r, client = %r, msg.content.body = %r' % (fname, rseqid, mtype, msg.routing_key, thriftClient, msg.content.body))
            
        method = getattr(thriftClient, 'recv_' + fname)
        method(iprot, mtype, rseqid)

        channel.basic_ack(deliveryTag, True)
        
        d = queue.get()
        d.addCallback(self.parseClientMessage, channel, queue, thriftClient,
            iprot_factory=iprot_factory)
        d.addErrback(self.catchClosedClientQueue)
        d.addErrback(self.handleClientQueueError)

    def parseClientUnrouteableMessage(self, msg, channel, queue, thriftClient,
                                      iprot_factory=None):
        tr = TTransport.TMemoryBuffer(msg.content.body)
        if iprot_factory is None:
            iprot = self.factory.iprot_factory.getProtocol(tr)
        else:
            iprot = iprot_factory.getProtocol(tr)
        (fname, mtype, rseqid) = iprot.readMessageBegin()

        # log.msg('Got unroutable. fname = %r, rseqid = %s, mtype = %r, routing key = %r, client = %r, msg.content.body = %r' % (fname, rseqid, mtype, msg.routing_key, thriftClient, msg.content.body))

        try:
            d = thriftClient._reqs.pop(rseqid)
        except KeyError:
            # KeyError will occur if the remote Thrift method is oneway,
            # since there is no outstanding local request deferred for
            # oneway calls.
            pass
        else:
            d.errback(TTransport.TTransportException(
                type=TTransport.TTransportException.NOT_OPEN,
                message='Unrouteable message, routing key = %r calling function %r'
                % (msg.routing_key, fname)))

        d = queue.get()
        d.addCallback(self.parseClientUnrouteableMessage, channel, queue,
            thriftClient, iprot_factory=iprot_factory)
        d.addErrback(self.catchClosedClientQueue)
        d.addErrback(self.handleClientQueueError)

    def catchClosedClientQueue(self, failure):
        # The queue is closed. Catch the exception and cleanup as needed.
        failure.trap(Closed)
        self.handleClosedClientQueue(failure)
    
    def handleClientQueueError(self, failure):
        pass

    def handleClosedClientQueue(self, failure):
        pass

    @defer.inlineCallbacks
    def createThriftServer(self, responsesExchange, serviceExchange,
        routingKey, processor, serviceQueue, channel=1, iprot_factory=None,
        oprot_factory=None):

        channel = yield self.channel(channel)

        yield channel.channel_open()
        yield channel.exchange_declare(exchange=serviceExchange, type="direct")

        yield channel.queue_declare(queue=serviceQueue, auto_delete=True)
        yield channel.queue_bind(queue=serviceQueue, exchange=serviceExchange,
            routing_key=routingKey)

        reply = yield channel.basic_consume(queue=serviceQueue)
        queue = yield self.queue(reply.consumer_tag)
        d = queue.get()
        d.addCallback(self.parseServerMessage, channel, responsesExchange,
            queue, processor, iprot_factory=iprot_factory, oprot_factory=oprot_factory)
        d.addErrback(self.catchClosedServerQueue)
        d.addErrback(self.handleServerQueueError)

    def parseServerMessage(self, msg, channel, exchange, queue, processor,
        iprot_factory=None, oprot_factory=None):
        deliveryTag = msg.delivery_tag
        try:
            replyTo = msg.content[self.replyToField]
        except KeyError:
            replyTo = None

        tmi = TTransport.TMemoryBuffer(msg.content.body)
        tr = TwistedAMQPTransport(channel, exchange, replyTo)

        if iprot_factory is None:
            iprot = self.factory.iprot_factory.getProtocol(tmi)
        else:
            iprot = iprot_factory.getProtocol(tmi)

        if oprot_factory is None:
            oprot = self.factory.oprot_factory.getProtocol(tr)
        else:
            oprot = oprot_factory.getProtocol(tr)

        d = processor.process(iprot, oprot)
        channel.basic_ack(deliveryTag, True)

        d = queue.get()
        d.addCallback(self.parseServerMessage, channel, exchange, queue,
            processor, iprot_factory, oprot_factory)
        d.addErrback(self.catchClosedServerQueue)
        d.addErrback(self.handleServerQueueError)

    def catchClosedServerQueue(self, failure):
        # The queue is closed. Catch the exception and cleanup as needed.
        failure.trap(Closed)
        self.handleClosedServerQueue(failure)
    
    def handleServerQueueError(self, failure):
        pass

    def handleClosedServerQueue(self, failure):
        pass

class IThriftAMQClientFactory(Interface):

    iprot_factory = Attribute("Input protocol factory")
    oprot_factory = Attribute("Input protocol factory")
    processor = Attribute("Thrift processor")
