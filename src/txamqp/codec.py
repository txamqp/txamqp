#!/usr/bin/env python

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
Utility code to translate between python objects and AMQP encoded data
fields.
"""

import six
from io import BytesIO
from struct import pack, calcsize, unpack


class EOF(Exception):
    pass


class Codec(object):

    def __init__(self, stream):
        self.stream = stream
        self.nwrote = 0
        self.nread = 0
        self.incoming_bits = []
        self.outgoing_bits = []

    def read(self, n):
        data = self.stream.read(n)
        if n > 0 and len(data) == 0:
            raise EOF()
        self.nread += len(data)
        return data

    def write(self, s):
        self.flushbits()
        self.stream.write(s)
        self.nwrote += len(s)

    def flush(self):
        self.flushbits()
        self.stream.flush()

    def flushbits(self):
        if len(self.outgoing_bits) > 0:
            bytes_list = []
            index = 0
            for b in self.outgoing_bits:
                if index == 0:
                    bytes_list.append(0)
                if b:
                    bytes_list[-1] |= 1 << index
                index = (index + 1) % 8
            del self.outgoing_bits[:]
            for byte in bytes_list:
                self.encode_octet(byte)

    def pack(self, fmt, *args):
        self.write(pack(fmt, *args))

    def unpack(self, fmt):
        size = calcsize(fmt)
        data = self.read(size)
        values = unpack(fmt, data)
        if len(values) == 1:
            return values[0]
        else:
            return values

    def encode(self, field_type, field_value):
        getattr(self, "encode_" + field_type)(field_value)

    def decode(self, field_type):
        return getattr(self, "decode_" + field_type)()

    # bit
    def encode_bit(self, o):
        if o:
            self.outgoing_bits.append(True)
        else:
            self.outgoing_bits.append(False)

    def decode_bit(self):
        if len(self.incoming_bits) == 0:
            bits = self.decode_octet()
            for shift in range(8):
                self.incoming_bits.append(bits >> shift & 1 != 0)
        return self.incoming_bits.pop(0)

    # octet
    def encode_octet(self, o):
        self.pack("!B", o)

    def decode_octet(self):
        return self.unpack("!B")

    # short
    def encode_short(self, o):
        self.pack("!H", o)

    def decode_short(self):
        return self.unpack("!H")

    # long
    def encode_long(self, o):
        self.pack("!L", o)

    def decode_long(self):
        return self.unpack("!L")

    # longlong
    def encode_longlong(self, o):
        self.pack("!Q", o)

    def decode_longlong(self):
        return self.unpack("!Q")

    def enc_str(self, fmt, s):
        size = len(s)
        self.pack(fmt, size)

        # Convert everything to binary representation (aka python3 string, and python2 unicode)
        if six.PY3 and not isinstance(s, bytes):
            s = s.encode()
        elif six.PY2 and isinstance(s, six.text_type):
            s = s.encode()

        self.write(s)

    def enc_bytes(self, fmt, s):
        size = len(s)
        self.pack(fmt, size)
        self.write(s)

    def dec_str(self, fmt):
        size = self.unpack(fmt)
        data = self.read(size)

        # Oppertunistic binary decode
        try:
            data = data.decode()
        except UnicodeDecodeError:
            pass
        return data

    def dec_bytes(self, fmt):
        size = self.unpack(fmt)
        return self.read(size)

    # shortstr
    def encode_shortstr(self, s):
        self.enc_str("!B", s)

    def decode_shortstr(self):
        return self.dec_str("!B")

    # longstr
    def encode_longstr(self, s):
        if isinstance(s, dict):
            self.encode_table(s)
        else:
            self.enc_str("!L", s)

    def encode_longbytes(self, s):
        if isinstance(s, dict):
            self.encode_table(s)
        else:
            self.enc_bytes("!L", s)

    def decode_longstr(self):
        return self.dec_str("!L")

    def decode_longbytes(self):
        return self.dec_bytes("!L")

    # timestamp
    def encode_timestamp(self, o):
        self.pack("!Q", o)

    def decode_timestamp(self):
        return self.unpack("!Q")

    # table
    def encode_table(self, tbl):
        enc = BytesIO()
        codec = Codec(enc)
        for key, value in tbl.items():
            codec.encode_shortstr(key)
            if isinstance(value, str):
                codec.write(b"S")
                codec.encode_longstr(value)
            else:
                codec.write(b"I")
                codec.encode_long(value)
        s = enc.getvalue()
        self.encode_long(len(s))
        self.write(s)

    def decode_table(self):
        size = self.decode_long()
        start = self.nread
        result = {}
        while self.nread - start < size:
            key = self.decode_shortstr()
            item_type = self.read(1)
            if item_type == b"S":
                value = self.decode_longstr()
            elif item_type == b"I":
                value = self.decode_long()
            elif item_type == b"F":
                value = self.decode_table()
            elif item_type == b"t":
                value = (self.decode_octet() != 0)
            else:
                raise ValueError(repr(item_type))
            result[key] = value
        return result


def test(field_type, value):
    if isinstance(value, (list, tuple)):
        values = value
    else:
        values = [value]
    stream = BytesIO()
    codec = Codec(stream)
    for v in values:
        codec.encode(field_type, v)
    codec.flush()
    enc = stream.getvalue()
    stream.reset()
    dup = []
    for _ in range(len(values)):
        dup.append(codec.decode(field_type))
    if values != dup:
        raise AssertionError("%r --> %r --> %r" % (values, enc, dup))

if __name__ == "__main__":
    def dotest(ftype, fvalue):
        args = (ftype, fvalue)
        test(*args)

    for bit_value in ("1", "0", "110", "011", "11001", "10101", "10011"):
        for i in range(10):
            dotest("bit", map(lambda x: x == "1", bit_value*i))

    for str_value in ({}, {"asdf": "fdsa", "fdsa": 1, "three": 3}, {"one": 1}):
        dotest("table", str_value)

    for ftype in ("octet", "short", "long", "longlong"):
        for byte_value in range(0, 256):
            dotest(ftype, byte_value)

    for ftype in ("shortstr", "longstr"):
        for str_value in ("", "a", "asdf"):
            dotest(ftype, str_value)
