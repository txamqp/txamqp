from zope.interface import Interface, Attribute
from txamqp.protocol import AMQClient
from txamqp.contrib.thrift.transport import TwistedAMQPTransport
from txamqp.content import Content

from twisted.internet import defer
from twisted.python import log

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTwisted, TTransport

class ThriftAMQClient(AMQClient):

    def __init__(self, *args, **kwargs):
        AMQClient.__init__(self, *args, **kwargs)

        if self.check_0_8():
            self.replyToField = "reply to"
        else:
            self.replyToField = "reply-to"

    @defer.inlineCallbacks
    def createThriftClient(self, responsesExchange, serviceExchange,
        routingKey, clientClass, responseQueue=None, iprot_factory=None,
        oprot_factory=None):

        channel = yield self.channel(1)

        if responseQueue is None:
            reply = yield channel.queue_declare(exclusive=True,
                auto_delete=True)

            responseQueue = reply.queue

            yield channel.queue_bind(queue=responseQueue,
                exchange=responsesExchange, routing_key=responseQueue)

        reply = yield channel.basic_consume(queue=responseQueue)

        log.msg("Consuming messages on queue: %s" % responseQueue)

        amqpTransport = TwistedAMQPTransport(channel, serviceExchange,
            routingKey, replyTo=responseQueue, replyToField=self.replyToField)

        if iprot_factory is None:
            iprot_factory = self.factory.iprot_factory

        if oprot_factory is None:
            oprot_factory = self.factory.oprot_factory

        thriftClient = clientClass(amqpTransport, oprot_factory)

        queue = yield self.queue(reply.consumer_tag)
        queue.get().addCallback(self.parseClientMessage, channel, queue,
            thriftClient, iprot_factory=iprot_factory)

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

        method = getattr(thriftClient, 'recv_' + fname)
        method(iprot, mtype, rseqid)

        channel.basic_ack(deliveryTag, True)
        queue.get().addCallback(self.parseClientMessage, channel, queue,
            thriftClient, iprot_factory=iprot_factory)

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

        return queue.get().addCallback(self.parseServerMessage, channel,
            exchange, queue, processor, iprot_factory, oprot_factory)


class IThriftAMQClientFactory(Interface):

    iprot_factory = Attribute("Input protocol factory")
    oprot_factory = Attribute("Input protocol factory")
    processor = Attribute("Thrift processor")
