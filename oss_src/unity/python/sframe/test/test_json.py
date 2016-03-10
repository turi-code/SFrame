# -*- coding: utf-8 -*-
'''
Copyright (C) 2016 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

import array
import datetime
import math
import os
import pytz
import sframe
import unittest

from . import util

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
    print ":".join("{:02x}".format(ord(c)) for c in s)

_SFrameComparer = util.SFrameComparer()

class JSONTest(unittest.TestCase):
    def _assertEquals(self, x, y):
        import sframe
        self.assertEquals(type(x), type(y))
        if isinstance(x, sframe.SArray):
            _SFrameComparer._assert_sarray_equal(x, y)
        elif isinstance(x, sframe.SFrame):
            _SFrameComparer._assert_sframe_equal(x, y)
        else:
            self.assertEquals(x, y)

    def _run_test_case(self, value):
        # test that JSON serialization is invertible with respect to both
        # value and type.
        if isinstance(value, str):
            print "Input string is:"
            _print_hex_bytes(value)
        json = sframe.json.dumps(value)
        print "Serialized json is:"
        print json
        print "as hex:"
        _print_hex_bytes(json)
        self._assertEquals(sframe.json.loads(json), value)

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
                sframe.json.loads(
                    sframe.json.dumps(float('nan')))))

    def test_string_to_json(self):
        # TODO znation - test non-ascii, non-utf-8 charsets. test null byte inside string.
        [self._run_test_case(value) for value in [
            "hello",
            "a'b",
            "a\"b",
            # some unicode strings from http://www.alanwood.net/unicode/unicode_samples.html
            'ɖɞɫɷ',
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
        [self._run_test_case(value) for value in [
            sframe.Image(path=item.url, format=item.format) for item in image_info
        ]]

    def test_sarray_to_json(self):
        d = datetime.datetime(year=2016, month=3, day=5)
        [self._run_test_case(value) for value in [
            sframe.SArray(),
            sframe.SArray([1,2,3]),
            sframe.SArray([1.0,2.0,3.0]),
            sframe.SArray([None, 3, None]),
            sframe.SArray(["hello", "world"]),
            sframe.SArray([
                u'ɖɞɫɷ'.encode('utf-8'),
                u'ɖɞɫɷ'.encode('utf-16'),
                u'ɖɞɫɷ'.encode('utf-32'),
            ]),
            sframe.SArray(array.array('d', [2.1,2.5,3.1])),
            sframe.SArray([
                ["hello", None, "world"],
                ["hello", 3, None],
                [3.14159, None],
            ]),
            sframe.SArray([
                {
                    "x": 1,
                    "y": 2
                }, {
                    "x": 5,
                    "z": 3
                },
            ]),
            sframe.SArray([
                d,
                pytz.utc.localize(d),
                pytz.timezone('US/Arizona').localize(d),
            ]),
            sframe.SArray([
                sframe.Image(path=item.url, format=item.format) for item in image_info
            ]),
        ]]

    def test_sframe_to_json(self):
        [self._run_test_case(value) for value in [
            sframe.SFrame(),
            #sframe.SFrame({'foo': [1,2,3,4], 'bar': [None, "Hello", None, "World"]}),
        ]]
