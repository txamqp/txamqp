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
"""Utilities for synchronous unit tests.

Classes in this module work without any actual network connection.
"""
from functools import partial

from twisted.python.failure import Failure
from twisted.internet.error import ConnectionDone, ConnectionLost

from txamqp.connection import Method, Frame, Header, Body


class AMQPump(object):
    """Track outgoing frames from an AMQClient and pump incoming frames to it.

    It implements the ITransport API so it can be used a transport for
    AMQClient protocols.

    @ivar client: The AMQClient to pump frames to. Must be set with connect().
    @ivar incoming: A dict mapping channel IDs to a list of frames that were
        received by the client through it.
    @ivar outgoing: A dict mapping channel IDs to a list of frames that were
        sent by the client through it.
    @ivar initialized: Whether the AMQP init string was sent by the client yet.
    @ivar connected: Whether we're still connected.
    @ivar aborted: Whether the connection was aborted.
    """

    def __init__(self, logger=None):
        """
        @param logger: Optional Twisted logger, to log incoming and outgoing
            frames (e.g. for debugging).
        """
        self.client = None
        self.incoming = {}
        self.outgoing = {}
        self.initialized = False
        self.connected = False
        self.aborted = False
        self.logger = logger

    def connect(self, client):
        """Connect this transport to the given AMQClient."""
        self._log("Client connected: {client}", client=client)
        self.connected = True
        self.client = client
        self.clock = client.clock
        self.client.makeConnection(self)

    def write(self, data):
        """Simulate sending data over the network.

        The data will be unpacked back into a frame and appeneded to the
        outgoing list associated with its channel.
        """
        if not self.initialized:
            # Ignore the init string
            self.initialized = True
            return
        frame = self.client._unpack_frame(data)
        self.outgoing.setdefault(frame.channel, []).append(frame)
        self._log("Outgoing frame: {frame}", frame=frame)

    def loseConnection(self):
        if not self.connected:
            return
        self.connected = False
        self.client.connectionLost(Failure(ConnectionDone()))

    def abortConnection(self):
        if not self.connected:
            return
        self.connected = False
        self.aborted = True
        self.client.connectionLost(Failure(ConnectionLost()))

    def channel(self, id):
        """Get an _AMPServerChannel, to be used as convenience to pump frames.

        @param id: The ID of the channel frames will be sent to.
        """
        return _AMPServerChannel(self, id)

    def pumpMethod(self, channel, klass, method, **fields):
        """Convenience for pumping a method frame.

        @param channel: The channel ID of the frame.
        @param klass: The class name of the method.
        @param method: The name of the method.
        @param fields: Fields for the method. Missing fields will be filled
            with None.
        """
        klass = self._classFromPyName(klass)
        method = self._methodFromPyName(klass, method)
        args = [None] * len(method.fields.items)
        for key, value in fields.items():
            field = method.fields.bypyname[key]
            args[method.fields.indexes[field]] = value
        self.pump(channel, Method(method, *args))

    def pumpHeader(self, channel, klass, weight, size, **properties):
        """Convenience for pumping an header frame.

        @param channel: The channel ID of the frame.
        @param klass: The class name of the header.
        @param weight: The weight of the header.
        @param size: Number of bytes in the follow-up body frame.
        @param properties: Properties of the header, if any.
        """
        klass = self._classFromPyName(klass)
        self.pump(channel, Header(klass, weight, size, **properties))

    def pumpBody(self, channel, body):
        """Convenience for pumping a body frame.

        @param channel: The channel ID of the frame.
        @param body: The data of the body frame.
        """
        self.pump(channel, Body(body))

    def pump(self, channel, payload):
        """Pump a single frame.

        @param channel: The channel ID of the frame.
        @param payload: The Payload object of the frame.
        """
        frame = Frame(channel, payload)
        self._log("Incoming frame: {frame}", frame=frame)
        self.incoming.setdefault(channel, []).append(frame)
        self.client.frame_received(frame)

    def _classFromPyName(self, name):
        """Return the spec class metadata object with given name."""
        return self.client.spec.classes.byname[name]

    def _methodFromPyName(self, klass, name):
        """Return the spec method metadata object with given name."""
        return klass.methods.byname[name]

    def _log(self, message, **params):
        """Log the given message if we were given a logger."""
        if self.logger:
            self.logger.debug(message, **params)


class _AMPServerChannel(object):
    """Convenience to pump frames into a connected AMQClient.

    You can invoke any method defined by the connected AMQClient's spec, in
    a way similar you would for an AMQChannel.

    For example:

        transport = AMQPump()
        transport.connect(client)
        channel = transport.serverChannel(1)
        channel.basic_consume_ok(consumer_tag="foo")
    """

    def __init__(self, pump, id):
        """
        @param pump: The AMQPump used to pump frames to a connected client.
        @param id: The channel ID frames will be pumped into.
        """
        self.pump = pump
        self.id = id

    def deliver(self, body, **fields):
        """Convenience for pumping a basic-deliver method frame plus data.

        This will send a frame for the basic-deliver method, one for the
        message header and one for the message body.
        """
        self.pump.pumpMethod(self.id, "basic", "deliver", **fields)
        self.pump.pumpHeader(self.id, "basic", 0, len(body))
        self.pump.pumpBody(self.id, body)

    def __getattr__(self, name):
        """Get a callable that will send the associated method frame."""
        words = name.split("_")
        klass = words[0]
        method = "-".join(words[1:])
        return partial(self.pump.pumpMethod, self.id, klass, method)
