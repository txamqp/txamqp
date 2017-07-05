from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor, task
from twisted.internet.protocol import ClientCreator
from twisted.python import log
from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

@inlineCallbacks
def gotConnection(conn, username, password, body, count=1):
    print("Connected to broker.")
    yield conn.authenticate(username, password)

    print("Authenticated. Ready to send messages")
    chan = yield conn.channel(1)
    yield chan.channel_open()

    def send_messages():
        def message_iterator():
            for i in range(count):
                content = body + "-%d" % i
                msg = Content(content)
                msg["delivery mode"] = 2
                chan.basic_publish(exchange="chatservice", content=msg, routing_key="txamqp_chatroom")
                print("Sending message: %s" % content)
                yield None
        return task.coiterate(message_iterator())

    yield send_messages()
    
    stopToken = "STOP"
    msg = Content(stopToken)
    msg["delivery mode"] = 2
    chan.basic_publish(exchange="chatservice", content=msg, routing_key="txamqp_chatroom")
    print("Sending message: %s" % stopToken)

    yield chan.channel_close()

    chan0 = yield conn.channel(0)
    yield chan0.connection_close()
    
    reactor.stop()
    
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Example publisher",
        usage="%(prog)s localhost 5672 / guest guest ../../specs/standard/amqp0-8.stripped.xml hello 1000"
    )
    parser.add_argument('host')
    parser.add_argument('port', type=int)
    parser.add_argument('vhost')
    parser.add_argument('username')
    parser.add_argument('password')
    parser.add_argument('spec_path')
    parser.add_argument('content')
    parser.add_argument('count', type=int, default=1)
    args = parser.parse_args()

    spec = txamqp.spec.load(args.spec_path)

    delegate = TwistedDelegate()

    d = ClientCreator(reactor, AMQClient, delegate=delegate, vhost=args.vhost,
        spec=spec).connectTCP(args.host, args.port)

    d.addCallback(gotConnection, args.username, args.password, args.content, args.count)

    def whoops(err):
        if reactor.running:
            log.err(err)
            reactor.stop()

    d.addErrback(whoops)

    reactor.run()
