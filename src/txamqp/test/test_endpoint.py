#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from twisted.trial.unittest import TestCase
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock
from twisted.test.proto_helpers import MemoryReactorClock, StringTransport

from txamqp.testlib import TestBase as IntegrationTest
from txamqp.factory import AMQFactory
from txamqp.endpoint import AMQEndpoint


class AMQEndpointTest(TestCase):
    def setUp(self):
        super(AMQEndpointTest, self).setUp()
        self.reactor = MemoryReactorClock()
        self.factory = AMQFactory(clock=Clock())

    def test_connect(self):
        """
        The endpoint connects to the broker and performs the AMQP
        authentication.
        """
        endpoint = AMQEndpoint(self.reactor, "1.2.3.4", "1234", username="me", password="pw")
        endpoint.connect(self.factory)
        self.assertEqual(("1.2.3.4", 1234), self.reactor.tcpClients[0][:2])
        # _WrappingFactory from twisted.internet.endpoints
        factory = self.reactor.tcpClients[0][2]
        protocol = factory.buildProtocol(None)
        protocol.makeConnection(StringTransport())
        client = protocol._wrappedProtocol
        self.assertEqual({"LOGIN": "me", "PASSWORD": "pw"}, client.response)
        self.assertEqual("AMQPLAIN", client.mechanism)

    def test_connect_with_vhost_and_heartbeat(self):
        """
        It's possible to specify a custom vhost and a custom heartbeat.
        """
        endpoint = AMQEndpoint(self.reactor, "1.2.3.4", "1234", username="me", password="pw", vhost="foo", heartbeat=10)
        endpoint.connect(self.factory)
        # _WrappingFactory from twisted.internet.endpoints
        factory = self.reactor.tcpClients[0][2]
        protocol = factory.buildProtocol(None)
        protocol.makeConnection(StringTransport())
        client = protocol._wrappedProtocol
        self.assertEqual("foo", client.vhost)
        self.assertEqual(10, client.heartbeatInterval)

    def test_from_uri(self):
        """
        It's possible to build an AMQEndpoint from an AMQP URI string.
        """
        endpoint = AMQEndpoint.from_uri(
            self.reactor, "amqp://me:pw@some.broker/foo?heartbeat=10")
        endpoint.connect(self.factory)
        self.assertEqual(("some.broker", 5672), self.reactor.tcpClients[0][:2])
        # _WrappingFactory from twisted.internet.endpoints
        factory = self.reactor.tcpClients[0][2]
        protocol = factory.buildProtocol(None)
        protocol.makeConnection(StringTransport())
        client = protocol._wrappedProtocol
        self.assertEqual("foo", client.vhost)
        self.assertEqual(10, client.heartbeatInterval)
        self.assertEqual({"LOGIN": "me", "PASSWORD": "pw"}, client.response)
        self.assertEqual("AMQPLAIN", client.mechanism)


class AMQEndpointIntegrationTest(IntegrationTest):
    @inlineCallbacks
    def test_connect(self):
        """
        The endpoint returns a connected and authenticated client.
        """
        factory = AMQFactory(spec=self.spec)
        endpoint = AMQEndpoint(
            reactor, self.host, self.port, username=self.user,
            password=self.password, vhost=self.vhost)
        client = yield endpoint.connect(factory)
        channel = yield client.channel(1)
        yield channel.channel_open()
        yield client.close()
