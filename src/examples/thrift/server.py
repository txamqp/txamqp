#!/usr/bin/env python

# based on the 'calculator' demo in the Thrift source

import sys, os.path
sys.path.insert(0, os.path.join(os.path.abspath(os.path.split(sys.argv[0])[0]), 'gen-py'))
import tutorial.Calculator
from tutorial.ttypes import *
from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol

from twisted.internet import reactor, defer
from twisted.internet.protocol import ClientCreator

import txamqp.spec
from txamqp.client import TwistedDelegate
from txamqp.contrib.thrift.transport import TwistedAMQPTransport
from txamqp.contrib.thrift.protocol import ThriftAMQClient

from zope.interface import implements

servicesExchange = "services"
responsesExchange = "responses"
calculatorQueue = "calculator_pool"
calculatorKey = "calculator"

class CalculatorHandler(object):
    implements(tutorial.Calculator.Iface)

    operations = {
        Operation.ADD: int.__add__,
        Operation.SUBTRACT: int.__sub__,
        Operation.MULTIPLY: int.__mul__,
        Operation.DIVIDE: int.__div__,
    }

    def _dispatchWork(self, w):
        # Just assume that it may take a long time
        results = self.operations[w.op](w.num1, w.num2)
        d = defer.Deferred()
        reactor.callLater(0, d.callback, results)
        return d

    def ping(self):
        print "ping() called from client"

    def add(self, num1, num2):
        print "add(num1, num2) called from client"
        return num1 + num2

    def calculate(self, logid, w):
        print "calculate(logid, w) called from client"
        try:
            return self._dispatchWork(w)
        except Exception, e:
            return defer.fail(InvalidOperation(what=logid, why=e.message))
            
    def zip(self):
        print "zip() called from client"

@defer.inlineCallbacks
def prepareClient(client, username, password):
    yield client.authenticate(username, password)

    handler = CalculatorHandler()
    processor = tutorial.Calculator.Processor(handler)
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    yield client.createThriftServer(responsesExchange, servicesExchange,
        calculatorKey, processor, calculatorQueue, iprot_factory=pfactory,
        oprot_factory=pfactory)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 7:
        print "%s host port vhost username password path_to_spec" % sys.argv[0]
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    vhost = sys.argv[3]
    username = sys.argv[4]
    password = sys.argv[5]
    specFile = sys.argv[6]

    spec = txamqp.spec.load(specFile)

    delegate = TwistedDelegate()

    print 'Starting the server...'

    d = ClientCreator(reactor, ThriftAMQClient, delegate, vhost,
        spec).connectTCP(host, port)
    d.addCallback(prepareClient, username, password)
    reactor.run()
