from txamqp.content import Content
from thrift.transport import TTwisted

class TwistedAMQPTransport(TTwisted.TMessageSenderTransport):
    def __init__(self, channel, exchange, routingKey, replyTo=None, replyToField=None):
        TTwisted.TMessageSenderTransport.__init__(self)
        self.channel = channel
        self.exchange = exchange
        self.routingKey = routingKey
        self.replyTo = replyTo
        self.replyToField = replyToField

    def sendMessage(self, message):
        content = Content(body=message)
        if self.replyTo:
            content[self.replyToField] = self.replyTo

        self.channel.basic_publish(exchange=self.exchange,
            routing_key=self.routingKey, content=content, immediate=True)
