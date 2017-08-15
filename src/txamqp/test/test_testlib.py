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

#
# Tests for the testlib itself.
# 
from traceback import print_stack
from twisted.internet.defer import inlineCallbacks

from txamqp.testlib import TestBase
from txamqp.queue import Empty


def mytrace(frame, event, arg):
    print_stack(frame)
    print("====")
    return mytrace


class TestBaseTest(TestBase):
    """Verify TestBase functions work as expected""" 

    @inlineCallbacks
    def test_assert_empty_pass(self):
        """Test assert empty works"""
        yield self.queue_declare(queue="empty")
        q = yield self.consume("empty")
        yield self.assertEmpty(q)
        try:
            yield q.get(timeout=1)
            self.fail("Queue is not empty.")
        except Empty:
            pass  # Ignore

    @inlineCallbacks
    def test_assert_empty_fail(self):
        yield self.queue_declare(queue="full")
        q = yield self.consume("full")
        self.channel.basic_publish(routing_key="full")
        try:
            yield self.assertEmpty(q)
            self.fail("assertEmpty did not assert on non-empty queue")
        except AssertionError:
            pass  # Ignore

    @inlineCallbacks
    def test_message_properties(self):
        """Verify properties are passed with message"""
        props = {"headers": {"x": 1, "y": 2}}
        yield self.queue_declare(queue="q")
        q = yield self.consume("q")
        yield self.assertPublishGet(q, routing_key="q", properties=props)
