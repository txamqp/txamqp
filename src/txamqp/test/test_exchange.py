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

"""
Tests for exchange behaviour.

Test classes ending in 'RuleTests' are derived from rules in amqp.xml.
"""
from twisted.internet.defer import inlineCallbacks

from txamqp.queue import Empty
from txamqp.testlib import TestBase, SupportedBrokers, QPID, OPENAMQ, RABBITMQ
from txamqp.content import Content
from txamqp.client import ChannelClosed, ConnectionClosed


class StandardExchangeVerifier:
    """
    Verifies standard exchange behavior.

    Used as base class for classes that test standard exchanges.
    """

    @inlineCallbacks
    def verify_direct_exchange(self, ex):
        """Verify that ex behaves like a direct exchange."""
        yield self.queue_declare(queue="q")
        yield self.channel.queue_bind(queue="q", exchange=ex, routing_key="k")
        yield self.assertPublishConsume(exchange=ex, queue="q", routing_key="k")
        try:
            yield self.assertPublishConsume(exchange=ex, queue="q", routing_key="kk")
            self.fail("Expected Empty exception")
        except Empty:  # Expected
            pass

    @inlineCallbacks
    def verify_fan_out_exchange(self, ex):
        """Verify that ex behaves like a fanout exchange."""
        yield self.queue_declare(queue="q") 
        yield self.channel.queue_bind(queue="q", exchange=ex)
        yield self.queue_declare(queue="p") 
        yield self.channel.queue_bind(queue="p", exchange=ex)
        for qname in ["q", "p"]:
            yield self.assertPublishGet((yield self.consume(qname)), ex)

    @inlineCallbacks
    def verify_topic_exchange(self, ex):
        """Verify that ex behaves like a topic exchange"""
        yield self.queue_declare(queue="a")
        yield self.channel.queue_bind(queue="a", exchange=ex, routing_key="a.#.b.*")
        q = yield self.consume("a")
        yield self.assertPublishGet(q, ex, "a.b.x")
        yield self.assertPublishGet(q, ex, "a.x.b.x")
        yield self.assertPublishGet(q, ex, "a.x.x.b.x")
        # Shouldn't match
        self.channel.basic_publish(exchange=ex, routing_key="a.b")        
        self.channel.basic_publish(exchange=ex, routing_key="a.b.x.y")        
        self.channel.basic_publish(exchange=ex, routing_key="x.a.b.x")        
        self.channel.basic_publish(exchange=ex, routing_key="a.b")
        yield self.assertEmpty(q)

    @inlineCallbacks
    def verify_headers_exchange(self, ex):
        """Verify that ex is a headers exchange"""
        yield self.queue_declare(queue="q")
        yield self.channel.queue_bind(queue="q", exchange=ex, arguments={"x-match": "all", "name": "fred", "age": 3})
        q = yield self.consume("q")
        headers = {"name": "fred", "age": 3}
        yield self.assertPublishGet(q, exchange=ex, properties={'headers': headers})
        self.channel.basic_publish(exchange=ex)  # No headers, won't deliver
        yield self.assertEmpty(q)



class RecommendedTypesRuleTests(TestBase, StandardExchangeVerifier):
    """
    The server SHOULD implement these standard exchange types: topic, headers.

    Client attempts to declare an exchange with each of these standard types.
    """

    @inlineCallbacks
    def test_direct(self):
        """Declare and test a direct exchange"""
        yield self.exchange_declare(0, exchange="d", type="direct")
        yield self.verify_direct_exchange("d")

    @inlineCallbacks
    def test_fanout(self):
        """Declare and test a fanout exchange"""
        yield self.exchange_declare(0, exchange="f", type="fanout")
        yield self.verify_fan_out_exchange("f")

    @inlineCallbacks
    def test_topic(self):
        """Declare and test a topic exchange"""
        yield self.exchange_declare(0, exchange="t", type="topic")
        yield self.verify_topic_exchange("t")

    @inlineCallbacks
    def test_headers(self):
        """Declare and test a headers exchange"""
        yield self.exchange_declare(0, exchange="h", type="headers")
        yield self.verify_headers_exchange("h")


class RequiredInstancesRuleTests(TestBase, StandardExchangeVerifier):
    """
    The server MUST, in each virtual host, pre-declare an exchange instance
    for each standard exchange type that it implements, where the name of the
    exchange instance is amq. followed by the exchange type name.
    
    Client creates a temporary queue and attempts to bind to each required
    exchange instance (amq.fanout, amq.direct, and amq.topic, amq.match if
    those types are defined).
    """
    @inlineCallbacks
    def test_amq_direct(self):
        yield self.verify_direct_exchange("amq.direct")

    @inlineCallbacks
    def test_amq_fan_out(self):
        yield self.verify_fan_out_exchange("amq.fanout")

    @inlineCallbacks
    def test_amq_topic(self):
        yield self.verify_topic_exchange("amq.topic")

    @inlineCallbacks
    def test_amq_match(self):
        yield self.verify_headers_exchange("amq.match")


class DefaultExchangeRuleTests(TestBase, StandardExchangeVerifier):
    """
    The server MUST predeclare a direct exchange to act as the default exchange
    for content Publish methods and for default queue bindings.
    
    Client checks that the default exchange is active by specifying a queue
    binding with no exchange name, and publishing a message with a suitable
    routing key but without specifying the exchange name, then ensuring that
    the message arrives in the queue correctly.
    """
    @SupportedBrokers(QPID, OPENAMQ)
    @inlineCallbacks
    def test_default_exchange(self):
        # Test automatic binding by queue name.
        yield self.queue_declare(queue="d")
        yield self.assertPublishConsume(queue="d", routing_key="d")
        # Test explicit bind to default queue
        yield self.verify_direct_exchange("")


# TODO aconway 2006-09-27: Fill in empty tests:

class DefaultAccessRuleTests(TestBase):
    """
    The server MUST NOT allow clients to access the default exchange except
    by specifying an empty exchange name in the Queue.Bind and content Publish
    methods.
    """


class ExtensionsRuleTests(TestBase):
    """
    The server MAY implement other exchange types as wanted.
    """


class DeclareMethodMinimumRuleTests(TestBase):
    """
    The server SHOULD support a minimum of 16 exchanges per virtual host and
    ideally, impose no limit except as defined by available resources.
    
    The client creates as many exchanges as it can until the server reports
    an error; the number of exchanges successfuly created must be at least
    sixteen.
    """


class DeclareMethodTicketFieldValidityRuleTests(TestBase):
    """
    The client MUST provide a valid access ticket giving "active" access to
    the realm in which the exchange exists or will be created, or "passive"
    access if the if-exists flag is set.
    
    Client creates access ticket with wrong access rights and attempts to use
    in this method.
    """


class DeclareMethodExchangeFieldReservedRuleTests(TestBase):
    """
    Exchange names starting with "amq." are reserved for predeclared and
    standardised exchanges. The client MUST NOT attempt to create an exchange
    starting with "amq.".
    """


class DeclareMethodTypeFieldTypedRuleTests(TestBase):
    """
    Exchanges cannot be redeclared with different types.  The client MUST not
    attempt to redeclare an existing exchange with a different type than used
    in the original Exchange.Declare method.
    """


class DeclareMethodTypeFieldSupportRuleTests(TestBase):
    """
    The client MUST NOT attempt to create an exchange with a type that the
    server does not support.
    """


class DeclareMethodPassiveFieldNotFoundRuleTests(TestBase):
    """
    If set, and the exchange does not already exist, the server MUST raise a
    channel exception with reply code 404 (not found).    
    """
    @inlineCallbacks
    def test(self):
        try:
            yield self.channel.exchange_declare(exchange="humpty_dumpty", passive=True)
            self.fail("Expected 404 for passive declaration of unknown exchange.")
        except ChannelClosed as e:
            self.assertChannelException(404, e.args[0])


class DeclareMethodDurableFieldSupportRuleTests(TestBase):
    """
    The server MUST support both durable and transient exchanges.
    """


class DeclareMethodDurableFieldStickyRuleTests(TestBase):
    """
    The server MUST ignore the durable field if the exchange already exists.
    """


class DeclareMethodAutoDeleteFieldStickyRuleTests(TestBase):
    """
    The server MUST ignore the auto-delete field if the exchange already
    exists.
    """


class DeleteMethodTicketFieldValidityRuleTests(TestBase):
    """
    The client MUST provide a valid access ticket giving "active" access
    rights to the exchange's access realm.
    
    Client creates access ticket with wrong access rights and attempts to use
    in this method.
    """


class DeleteMethodExchangeFieldExistsRuleTests(TestBase):
    """
    The client MUST NOT attempt to delete an exchange that does not exist.
    """


class HeadersExchangeTests(TestBase):
    """
    Tests for headers exchange functionality.
    """
    @inlineCallbacks
    def setUp(self):
        yield TestBase.setUp(self)
        yield self.queue_declare(queue="q")
        self.q = yield self.consume("q")

    @inlineCallbacks
    def _assert_publish_get(self, headers):
        yield self.assertPublishGet(self.q, exchange="amq.match", properties={'headers': headers})

    def _basic_publish(self, headers):
        self.channel.basic_publish(exchange="amq.match", content=Content("foobar", properties={'headers': headers}))

    @inlineCallbacks
    def test_match_all(self):
        yield self.channel.queue_bind(queue="q", exchange="amq.match", arguments={'x-match': 'all', "name": "fred", "age": 3})
        yield self._assert_publish_get({"name": "fred", "age": 3})
        yield self._assert_publish_get({"name": "fred", "age": 3, "extra": "ignoreme"})

        # None of these should match
        self._basic_publish({})
        self._basic_publish({"name": "barney"})
        self._basic_publish({"name": 10})
        self._basic_publish({"name": "fred", "age": 2})
        yield self.assertEmpty(self.q)

    @inlineCallbacks
    def test_match_any(self):
        yield self.channel.queue_bind(queue="q", exchange="amq.match", arguments={'x-match': 'any', "name": "fred", "age": 3})
        yield self._assert_publish_get({"name": "fred"})
        yield self._assert_publish_get({"name": "fred", "ignoreme": 10})
        yield self._assert_publish_get({"ignoreme": 10, "age": 3})

        # Wont match
        self._basic_publish({})
        self._basic_publish({"irrelevant": 0})
        yield self.assertEmpty(self.q)


class MiscellaneousErrorsTests(TestBase):
    """
    Test some miscellaneous error conditions
    """
    @inlineCallbacks
    def test_type_not_known(self):
        try:
            yield self.channel.exchange_declare(exchange="test_type_not_known_exchange", type="invalid_type")
            self.fail("Expected 503 for declaration of unknown exchange type.")
        except ConnectionClosed as e:
            self.assertConnectionException(503, e.args[0])

    @SupportedBrokers(QPID, OPENAMQ)
    @inlineCallbacks
    def test_different_declared_type(self):
        yield self.channel.exchange_declare(exchange="test_different_declared_type_exchange", type="direct")
        try:
            yield self.channel.exchange_declare(exchange="test_different_declared_type_exchange", type="topic")
            self.fail("Expected 530 for redeclaration of exchange with different type.")
        except ConnectionClosed as e:
            self.assertConnectionException(530, e.args[0])
        # cleanup
        other = yield self.connect()
        c2 = yield other.channel(1)
        yield c2.channel_open()
        yield c2.exchange_delete(exchange="test_different_declared_type_exchange")

    @SupportedBrokers(RABBITMQ)
    @inlineCallbacks
    def test_different_declared_type_rabbit(self):
        """Test redeclaration of exchange with different type on RabbitMQ."""
        yield self.channel.exchange_declare(
            exchange="test_different_declared_type_exchange", type="direct")
        try:
            yield self.channel.exchange_declare(
                exchange="test_different_declared_type_exchange", type="topic")
            self.fail(
                "Expected 406 for redeclaration of exchange with "
                "different type.")
        except ChannelClosed as e:
            self.assertChannelException(406, e.args[0])
        finally:
            # cleanup
            other = yield self.connect()
            c2 = yield other.channel(1)
            yield c2.channel_open()
            yield c2.exchange_delete(
                exchange="test_different_declared_type_exchange")
