from txamqp.content import Content

class TwistedAMQPTransport(object):
    def __init__(self, channel, exchange, routingKey, replyTo=None,
        replyToField="reply to"):
        self.channel = channel
        self.exchange = exchange
        self.routingKey = routingKey
        self.replyTo = replyTo
        self.replyToField = replyToField

    def write(self, data):
        content = Content(body=data)
        if self.replyTo:
            content[self.replyToField] = self.replyTo

        self.channel.basic_publish(exchange=self.exchange,
            routing_key=self.routingKey, content=content)
