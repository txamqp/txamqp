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
from twisted.internet.task import Clock

from txamqp.protocol import AMQClient
from txamqp.spec import load
from txamqp.factory import DEFAULT_SPEC
from txamqp.client import TwistedDelegate
from txamqp.testing import AMQPump


class AMQPumpTest(TestCase):

    def setUp(self):
        super(AMQPumpTest, self).setUp()
        delegate = TwistedDelegate()
        spec = load(DEFAULT_SPEC)
        self.client = AMQClient(delegate, "/", spec, clock=Clock())
        self.transport = AMQPump()
        self.transport.connect(self.client)

    def test_send_receive(self):
        """
        Test sending and receiving frames.
        """
        client_channel = self.successResultOf(self.client.channel(1))
        server_channel = self.transport.channel(1)
        d = client_channel.basic_consume(queue="test-queue")
        server_channel.basic_consume_ok(consumer_tag="consumer")
        reply = self.successResultOf(d)
        queue = self.successResultOf(self.client.queue(reply.consumer_tag))
        d = queue.get(timeout=1)
        server_channel.deliver("hello", consumer_tag="consumer")
        message = self.successResultOf(d)
        self.assertEqual("hello", message.content.body)
