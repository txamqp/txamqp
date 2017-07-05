#!/usr/bin/env python

# based on the 'calculator' demo in the Thrift source

import sys, os.path
sys.path.insert(0, os.path.join(os.path.abspath(os.path.split(sys.argv[0])[0]), 'gen-py.twisted'))

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
        print("ping() called from client")

    def add(self, num1, num2):
        print("add(num1, num2) called from client")
        return num1 + num2

    def calculate(self, logid, w):
        print("calculate(logid, w) called from client")
        try:
            return self._dispatchWork(w)
        except Exception as e:
            return defer.fail(InvalidOperation(what=logid, why=e.message))
            
    def zip(self):
        print("zip() called from client")

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
    import argparse

    parser = argparse.ArgumentParser(
        description="Example Thrift server",
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

    print('Starting the server...')

    d = ClientCreator(reactor, ThriftAMQClient, delegate, args.vhost,
        spec).connectTCP(args.host, args.port)
    d.addCallback(prepareClient, args.username, args.password)
    reactor.run()
