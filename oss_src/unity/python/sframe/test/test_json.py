# -*- coding: utf-8 -*-
'''
Copyright (C) 2016 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from __future__ import print_function

import array
import datetime
import math
import os
import pytz
import sys
import unittest

from . import util
from .. import _json as json

if sys.version_info.major == 3:
    long = int

class image_info:
    def __init__(self, url):
        self.url = url
        if 'png' in url:
            self.format = 'PNG'
        elif 'jpg' in url:
            self.format = 'JPG'
        if 'grey' in url:
            self.channels = 1
        else:
            self.channels = 3

current_file_dir = os.path.dirname(os.path.realpath(__file__))
image_urls = [current_file_dir + x for x in [
    '/images/nested/sample_grey.jpg',
    '/images/nested/sample_grey.png',
    '/images/sample.jpg',
    '/images/sample.png'
]]
image_info = [image_info(u) for u in image_urls]

def _print_hex_bytes(s):
    sep = ":"
    format_str = "{:02x}"
    print(sep.join(format_str.format(ord(c)) for c in s))

_SFrameComparer = util.SFrameComparer()

class JSONTest(unittest.TestCase):
    def _assertEquals(self, x, y):
        from ..data_structures.sarray import SArray
        from ..data_structures.sframe import SFrame
        if type(x) in [long,int]:
            self.assertTrue(type(y) in [long,int])
        else:
            self.assertEquals(type(x), type(y))
        if isinstance(x, SArray):
            _SFrameComparer._assert_sarray_equal(x, y)
        elif isinstance(x, SFrame):
            _SFrameComparer._assert_sframe_equal(x, y)
        else:
            self.assertEquals(x, y)

    def _run_test_case(self, value):
        # test that JSON serialization is invertible with respect to both
        # value and type.
        if isinstance(value, str):
            print("Input string is:")
            _print_hex_bytes(value)
        j = json.dumps(value)
        print("Serialized json is:")
        #print(j)
        print("as hex:")
        _print_hex_bytes(j)
        self._assertEquals(json.loads(j), value)

    @unittest.skipIf(sys.platform == 'win32', "Windows long issue")
    def test_int(self):
        [self._run_test_case(value) for value in [
            0,
            1,
            -2147483650,
            -2147483649, # boundary of accurate representation in JS 64-bit float
            2147483648, # boundary of accurate representation in JS 64-bit float
            2147483649,
        ]]

    def test_float(self):
        [self._run_test_case(value) for value in [
            -1.1,
            -1.0,
            0.0,
            1.0,
            1.1,
            float('-inf'),
            float('inf'),
        ]]
        self.assertTrue(
            math.isnan(
                json.loads(
                    json.dumps(float('nan')))))

    def test_string_to_json(self):
        [self._run_test_case(value) for value in [
            "hello",
            "a'b",
            "a\"b",
            "ɖɞɫɷ",
        ]]

    @unittest.skipIf(sys.version_info.major != 2, """
Python 3 SFrame doesn't support non-utf-8 strings with flexible_type.
These test cases don't apply there.
""")
    def test_non_utf8_to_json(self):
        [self._run_test_case(value) for value in [
            # some unicode strings from http://www.alanwood.net/unicode/unicode_samples.html
            u'ɖɞɫɷ'.encode('utf-8'),
            u'ɖɞɫɷ'.encode('utf-16'),
            u'ɖɞɫɷ'.encode('utf-32'),
            'a\x00b', # with null byte
        ]]

    def test_vec_to_json(self):
        [self._run_test_case(value) for value in [
            array.array('d'),
            array.array('d', [1.5]),
            array.array('d', [2.1,2.5,3.1]),
        ]]

    def test_list_to_json(self):
        # TODO -- we can't test lists of numbers, due to
        # Python<->flexible_type not being reversible for lists of numbers.
        # if `list` of `int` goes into C++, the flexible_type representation
        # becomes flex_vec (vector<flex_float>). This is a lossy representation.
        # known issue, can't resolve here.
        [self._run_test_case(value) for value in [
            [],
            ["hello", "world"],
            ["hello", 3, None],
            [3.14159, None],
            [{}, {'x': 1, 'y': 2}],
        ]]

    def test_dict_to_json(self):
        [self._run_test_case(value) for value in [
            {},
            {
                "x": 1,
                "y": 2
            },
        ]]

    def test_date_time_to_json(self):
        d = datetime.datetime(year=2016, month=3, day=5)
        [self._run_test_case(value) for value in [
            d,
            pytz.utc.localize(d),
            pytz.timezone('US/Arizona').localize(d),
        ]]

    def test_image_to_json(self):
        from .. import Image
        [self._run_test_case(value) for value in [
            Image(path=item.url, format=item.format) for item in image_info
        ]]

    def test_sarray_to_json(self):
        from ..data_structures.sarray import SArray
        from ..data_structures.sframe import SFrame
        from .. import Image
        d = datetime.datetime(year=2016, month=3, day=5)
        [self._run_test_case(value) for value in [
            SArray(),
            SArray([1,2,3]),
            SArray([1.0,2.0,3.0]),
            SArray([None, 3, None]),
            SArray(["hello", "world"]),
            SArray(array.array('d', [2.1,2.5,3.1])),
            SArray([
                ["hello", None, "world"],
                ["hello", 3, None],
                [3.14159, None],
            ]),
            SArray([
                {
                    "x": 1,
                    "y": 2
                }, {
                    "x": 5,
                    "z": 3
                },
            ]),
            SArray([
                d,
                pytz.utc.localize(d),
                pytz.timezone('US/Arizona').localize(d),
            ]),
            SArray([
                Image(path=item.url, format=item.format) for item in image_info
            ]),
        ]]

    @unittest.skipIf(sys.version_info.major != 2, """
Python 3 SFrame doesn't support non-utf-8 strings with flexible_type.
These test cases don't apply there.
""")
    def test_non_utf8_sarray_to_json(self):
        from ..data_structures.sarray import SArray
        from ..data_structures.sframe import SFrame
        [self._run_test_case(value) for value in [
            SArray([
                u'ɖɞɫɷ'.encode('utf-8'),
                u'ɖɞɫɷ'.encode('utf-16'),
                u'ɖɞɫɷ'.encode('utf-32'),
            ]),
        ]]

    def test_sframe_to_json(self):
        from ..data_structures.sframe import SFrame
        [self._run_test_case(value) for value in [
            SFrame(),
            #sframe.SFrame({'foo': [1,2,3,4], 'bar': [None, "Hello", None, "World"]}),
        ]]
