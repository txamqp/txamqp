from txamqp.content import Content
from thrift.transport import TTwisted

class TwistedAMQPTransport(TTwisted.TMessageSenderTransport):
    def __init__(self, channel, exchange, routingKey, clientName=None,
                 replyTo=None, replyToField=None):
        TTwisted.TMessageSenderTransport.__init__(self)
        self.channel = channel
        self.exchange = exchange
        self.routingKey = routingKey
        # clientName is the name of the client class we are trying to get
        # the message through to. We need to send it seeing as the message
        # may be unroutable and we need a basic return that will tell us
        # who we are trying to reach.
        self.clientName = clientName
        self.replyTo = replyTo
        self.replyToField = replyToField

    def sendMessage(self, message):
        content = Content(body=message)
        if self.clientName:
            content['headers'] = { 'thriftClientName' : self.clientName }
        if self.replyTo:
            content[self.replyToField] = self.replyTo

        self.channel.basic_publish(exchange=self.exchange,
            routing_key=self.routingKey, content=content, mandatory=True)
