from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator
from twisted.python import log

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate

import txamqp.spec

@inlineCallbacks
def gotConnection(conn, username, password):
    print("Connected to broker.")
    yield conn.authenticate(username, password)

    print("Authenticated. Ready to receive messages")
    chan = yield conn.channel(1)
    yield chan.channel_open()

    yield chan.queue_declare(queue="chatrooms", durable=True, exclusive=False, auto_delete=False)
    yield chan.exchange_declare(exchange="chatservice", type="direct", durable=True, auto_delete=False)

    yield chan.queue_bind(queue="chatrooms", exchange="chatservice", routing_key="txamqp_chatroom")

    yield chan.basic_consume(queue='chatrooms', no_ack=True, consumer_tag="testtag")

    queue = yield conn.queue("testtag")

    while True:
        msg = yield queue.get()
        print('Received: {0} from channel #{1}'.format(msg.content.body, chan.id))
        if msg.content.body == "STOP":
            break

    yield chan.basic_cancel("testtag")

    yield chan.channel_close()

    chan0 = yield conn.channel(0)

    yield chan0.connection_close()

    reactor.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Example consumer",
        usage="%(prog)s localhost 5672 / guest guest ../../specs/standard/amqp0-8.stripped.xml"
    )
    parser.add_argument('host')
    parser.add_argument('port', type=int)
    parser.add_argument('vhost')
    parser.add_argument('username')
    parser.add_argument('password')
    parser.add_argument('spec_path')
    args = parser.parse_args()

    spec = txamqp.spec.load(args.spec_path)

    delegate = TwistedDelegate()
    d = ClientCreator(reactor, AMQClient, delegate=delegate, vhost=args.vhost,
        spec=spec).connectTCP(args.host, args.port)

    d.addCallback(gotConnection, args.username, args.password)

    def whoops(err):
        if reactor.running:
            log.err(err)
            reactor.stop()

    d.addErrback(whoops)

    reactor.run()
