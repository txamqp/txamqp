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
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.endpoints import clientFromString
from twisted.web.client import URI
from twisted.web.http import parse_qs


class AMQEndpoint(object):
    """An endpoint that knows how to connect to AMQP brokers.

    The class implements the same API as IStreamClientEndpoint, however it
    requires the protocol factory to be able to speak AMQP for perfoming
    the authentication.

    @note: Currently TLS connections are not supported.
    """

    def __init__(self, reactor, host, port, username="", password="",
                 vhost="/", heartbeat=0, auth_mechanism="AMQPLAIN",
                 timeout=30):
        """
        @param reactor: An L{IReactorTCP} provider.

        @param username: The username to use when authenticating.
        @type username: L{bytes}

        @param password: The password to use when authenticating.
        @type password: L{bytes}

        @param host: Host name or IP address of the AMQP broker.
        @type host: L{bytes}

        @type port: L{int}
        @param port: Port number.

        @param vhost: The vhost to open the connection against.
        @type vhost: L{bytes}

        @param heartbeat: AMQP heartbeat in seconds.
        @type heartbeat: L{int}

        @type auth_mechanism: Authentication mechanism. Currently only AMQPLAIN
            and PLAIN are supported.
        @type mechanism: L{bytes}

        @param timeout: Number of seconds to wait before assuming the
            connection has failed.
        @type timeout: int
        """
        self._reactor = reactor
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._vhost = vhost
        self._heartbeat = heartbeat
        self._auth_mechanism = auth_mechanism
        self._timeout = timeout

    @classmethod
    def from_uri(cls, reactor, uri):
        """Return an AMQEndpoint instance configured with the given AMQP uri.

        @see: https://www.rabbitmq.com/uri-spec.html
        """
        uri = URI.fromBytes(uri.encode(), defaultPort=5672)
        kwargs = {}
        host = uri.host.decode()
        if "@" in host:
            auth, host = uri.netloc.decode().split("@")
            username, password = auth.split(":")
            kwargs.update({"username": username, "password": password})

        vhost = uri.path.decode()
        if len(vhost) > 1:
            vhost = vhost[1:]  # Strip leading "/"
        kwargs["vhost"] = vhost

        params = parse_qs(uri.query)
        kwargs.update({name.decode(): value[0].decode() for name, value in params.items()})

        if "heartbeat" in kwargs:
            kwargs["heartbeat"] = int(kwargs["heartbeat"])
        return cls(reactor, host, uri.port, **kwargs)

    def connect(self, protocol_factory):
        """
        Connect to the C{protocolFactory} to the AMQP broker specified by the
        URI of this endpoint.

        @param protocol_factory: An L{AMQFactory} building L{AMQClient} objects.
        @return: A L{Deferred} that results in an L{AMQClient} upon successful
            connection otherwise a L{Failure} wrapping L{ConnectError} or
            L{NoProtocol <twisted.internet.error.NoProtocol>}.
        """
        # XXX Since AMQClient requires these parameters at __init__ time, we
        #     need to override them in the provided factory.
        protocol_factory.set_vhost(self._vhost)
        protocol_factory.set_heartbeat(self._heartbeat)

        description = "tcp:{}:{}:timeout={}".format(
            self._host, self._port, self._timeout)
        endpoint = clientFromString(self._reactor, description)

        deferred = endpoint.connect(protocol_factory)
        return deferred.addCallback(self._authenticate)

    @inlineCallbacks
    def _authenticate(self, client):
        """Perform AMQP authentication."""
        yield client.authenticate(
            self._username, self._password, mechanism=self._auth_mechanism)
        returnValue(client)
