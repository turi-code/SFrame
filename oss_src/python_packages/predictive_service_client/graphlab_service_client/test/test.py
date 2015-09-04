'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
import os
from ConfigParser import ConfigParser
import tempfile
from unittest import TestCase
import httpretty
from  predictive_client import PredictiveServiceClient
import json
import re
import urllib

API_KEY = '123'

class PredictiveServiceClientTest(TestCase):
    def setUp(self):
        self.configfile = self._create_temp_config()

    def tearDown(self):
        os.remove(self.configfile)

    def _create_temp_config(self):
        fd, filename = tempfile.mkstemp()
        config = ConfigParser()
        section_name = 'Service Info'
        config.add_section(section_name)
        config.set(section_name, "endpoint", 'http://abc.com')
        config.set(section_name, 'api key', API_KEY)

        with open(filename, 'w') as f:
            config.write(f)
        return filename

    def _register_fake_endpoints(self):
        def query_callback(request, uri, headers):
            headers['content_type'] = 'text/json'

            print uri

            body = json.loads(request.body)
            if ('api key' not in body) or (body['api key'] != API_KEY):
                raise AssertionError('API Key Error')

            predict_response = {'type': 'QuerySuccessful', 'response':{'something':1}}
            wrong_method_response = {'type':'QueryFailed', 'error': 'unknown query method'}
            unknonuri_response = {'type':'UnknownURI'}

            paths = re.match("http://abc.com/data/(.*)", uri)
            print paths.groups()
            model_name = paths.groups()[0]
            print "model_name: %s" % model_name

            if model_name == 'a' or model_name == urllib.quote('name with space'):
                request_data = json.loads(request.body)
                method = request_data['data']['method']
                if method != 'predict' and method != 'recommend':
                    return (200, headers, json.dumps(wrong_method_response))
                else:
                    return (200, headers, json.dumps(predict_response))
            else:
                return (404, headers, json.dumps(unknonuri_response))

        def feedback_callback(request, uri, headers):
            headers['content_type'] = 'text/json'

            print request
            print uri

            body = json.loads(request.body)
            if ('api key' not in body) or (body['api key'] != API_KEY):
                raise AssertionError('API Key Error')

            feedback_response = {'success': 'true'}
            return (200, headers, json.dumps(feedback_response))


        httpretty.register_uri(httpretty.GET, "http://abc.com",
                   body='I am here',
                   status=200)

        httpretty.register_uri(httpretty.POST, "http://abc.com/control/list_objects",
                           body='{"a":1, "name with space": 2}',
                           status=200,
                           content_type='text/json')


        httpretty.register_uri(httpretty.POST, re.compile("http://abc.com/data/(\w+)"),
                           body=query_callback)

        httpretty.register_uri(httpretty.POST, re.compile("http://abc.com/feedback"),
                           body=feedback_callback)

    @httpretty.activate
    def test_read_config(self):
        self._register_fake_endpoints()

        t = PredictiveServiceClient(config_file =self.configfile)
        self.assertEquals(t.endpoint, 'http://abc.com')
        self.assertEquals(t.api_key, API_KEY)


    @httpretty.activate
    def test_query(self):
        self._register_fake_endpoints()

        t = PredictiveServiceClient(config_file = self.configfile)

        data = {"dataset":{"user_id":175343, "movie_id":1011}}
        result = t.query('a', method='predict', data= data})
        result = t.query('a', method='recommend', data= data})
        result = t.query('name with space', method='recommend', data= data})

        # unknown model
        with self.assertRaises(RuntimeError):
            t.query('nonexist', data)

        # wrong data type
        with self.assertRaises(TypeError):
            t.query('a', 'str')

        # wrong method
        with self.assertRaises(RuntimeError):
            t.query('a', {"method":"wrong method", "data":data})

    @httpretty.activate
    def test_feedback(self):
        self._register_fake_endpoints()

        t = PredictiveServiceClient(config_file =self.configfile)

        t.feedback('some', {'a':1})
        t.feedback('some more', {'a':1})
