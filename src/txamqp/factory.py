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
from twisted.internet.protocol import Factory

from txamqp.protocol import AMQClient
from txamqp.spec import DEFAULT_SPEC, load
from txamqp.client import TwistedDelegate


class AMQFactory(Factory):
    """A factory building AMQClient instances."""

    protocol = AMQClient

    def __init__(self, spec=None, clock=None):
        """
        @param spec: Path to the spec file. Defaults to the standard AMQP 0.9.
        @type spec: L{str} (native string)
        """
        if spec is None:
            spec = DEFAULT_SPEC
        self._spec = load(spec)
        self._clock = clock
        self._vhost = "/"
        self._heartbeat = 0

    def set_vhost(self, vhost):
        """Set a custom vhost."""
        self._vhost = vhost

    def set_heartbeat(self, heartbeat):
        """Set a custom heartbeat."""
        self._heartbeat = heartbeat

    def buildProtocol(self, addr):
        delegate = TwistedDelegate()
        protocol = self.protocol(
            delegate, vhost=self._vhost, spec=self._spec,
            heartbeat=self._heartbeat, clock=self._clock)
        return protocol
