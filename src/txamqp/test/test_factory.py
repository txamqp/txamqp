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
from twisted.internet.address import IPv4Address
from twisted.internet.task import Clock

from txamqp.protocol import AMQClient
from txamqp.factory import AMQFactory, DEFAULT_SPEC


class AMQFactoryTest(TestCase):
    def test_build_protocol(self):
        """Test building AMQClient instances with default parameters."""
        address = IPv4Address("TCP", "127.0.0.1", 5672)
        factory = AMQFactory()
        client = factory.buildProtocol(address)
        self.assertIsInstance(client, AMQClient)
        self.assertEqual("/", client.vhost)
        self.assertEqual(DEFAULT_SPEC, client.spec.file)
        self.assertEqual(0, client.heartbeatInterval)

    def test_build_protocol_custom_parameters(self):
        """Test building AMQClient instances with custom parameters."""
        address = IPv4Address("TCP", "127.0.0.1", 5672)
        spec = "../specs/rabbitmq/amqp0-8.stripped.rabbitmq.xml"
        clock = Clock()
        factory = AMQFactory(spec=spec, clock=clock)
        factory.set_vhost("foo")
        factory.set_heartbeat(1)
        client = factory.buildProtocol(address)
        self.assertEqual("foo", client.vhost)
        self.assertEqual(spec, client.spec.file)
        self.assertEqual(1, client.heartbeatInterval)
        self.assertEqual(1, len(clock.getDelayedCalls()))

    def test_build_protocol_different_delegates(self):
        """Test building AMQClient getting different delegates."""
        address = IPv4Address("TCP", "127.0.0.1", 5672)
        factory = AMQFactory()
        client1 = factory.buildProtocol(address)
        client2 = factory.buildProtocol(address)
        self.assertIsNot(client2.delegate, client1.delegate)
