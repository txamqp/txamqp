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
from twisted.trial import unittest
from twisted.python.failure import Failure

from txamqp.client import TwistedEvent, AlreadyFiredError


class EventTest(unittest.TestCase):
    def test_fire(self):
        """Test event success."""
        result = []

        def fired(res):
            result.append(res)

        e = TwistedEvent()

        e.wait().addCallback(fired)
        self.assertEqual(result, [])

        e.fire()
        self.assertEqual(result, [True])

        self.assertRaises(AlreadyFiredError, e.fire)
        self.assertRaises(AlreadyFiredError, e.fail, None)

        e.wait().addCallback(fired)
        self.assertEqual(result, [True, True])

        e.fail_if_not_fired(None)

    def test_fail(self):
        """Test event failure."""
        result = []

        def failed(res):
            result.append(res)

        e = TwistedEvent()

        e.wait().addErrback(failed)
        self.assertEqual(result, [])

        f = Failure(Exception())
        e.fail(f)
        self.assertEqual(result, [f])

        self.assertRaises(AlreadyFiredError, e.fire)
        self.assertRaises(AlreadyFiredError, e.fail, f)

        e.wait().addErrback(failed)
        self.assertEqual(result, [f, f])

        e.fail_if_not_fired(None)

        e = TwistedEvent()

        e.wait().addErrback(failed)
        e.fail_if_not_fired(f)
        self.assertEqual(result, [f, f, f])

    def test_independent(self):
        """Test that waiters are independent."""
        result = []

        def bad(res):
            result.append(res)
            raise Exception()

        def fired(res):
            result.append(res)

        e = TwistedEvent()

        d = e.wait().addCallback(bad)
        e.wait().addCallback(fired)
        d.addErrback(lambda _: None)

        e.fire()

        self.assertEqual(result, [True, True])
