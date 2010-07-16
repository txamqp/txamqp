from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator
from twisted.python import log

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate

import txamqp.spec

@inlineCallbacks
def gotConnection(conn, username, password):
    print "Connected to broker."
    yield conn.authenticate(username, password)

    print "Authenticated. Ready to receive messages"
    chan = yield conn.channel(1)
    yield chan.channel_open()

    yield chan.queue_declare(queue="chatrooms", durable=True, exclusive=False, auto_delete=False)
    yield chan.exchange_declare(exchange="chatservice", type="direct", durable=True, auto_delete=False)

    yield chan.queue_bind(queue="chatrooms", exchange="chatservice", routing_key="txamqp_chatroom")

    yield chan.basic_consume(queue='chatrooms', no_ack=True, consumer_tag="testtag")

    queue = yield conn.queue("testtag")

    while True:
        msg = yield queue.get()
        print 'Received: ' + msg.content.body + ' from channel #' + str(chan.id)
        if msg.content.body == "STOP":
            break

    yield chan.basic_cancel("testtag")

    yield chan.channel_close()

    chan0 = yield conn.channel(0)

    yield chan0.connection_close()

    reactor.stop()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 7:
        print "%s host port vhost username password path_to_spec" % sys.argv[0]
        print "e.g. %s localhost 5672 / guest guest ../../specs/standard/amqp0-8.stripped.xml" % sys.argv[0]
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    vhost = sys.argv[3]
    username = sys.argv[4]
    password = sys.argv[5]

    spec = txamqp.spec.load(sys.argv[6])

    delegate = TwistedDelegate()

    d = ClientCreator(reactor, AMQClient, delegate=delegate, vhost=vhost,
        spec=spec).connectTCP(host, port)

    d.addCallback(gotConnection, username, password)

    def whoops(err):
        if reactor.running:
            log.err(err)
            reactor.stop()

    d.addErrback(whoops)

    reactor.run()
