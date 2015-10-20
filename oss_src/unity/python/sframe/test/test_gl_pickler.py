import boto
import moto
import unittest
import os
import uuid
import shutil

import pickle
from sframe.util import cloudpickle

import sframe as gl
from sframe import _gl_pickle as gl_pickle
from sframe.util import _assert_sframe_equal as assert_sframe_equal


class GLPicklingTest(unittest.TestCase):

    def setUp(self):
        self.filename = str(uuid.uuid4())
        self.dir_mode = False

    def tearDown(self):
        if os.path.isdir(self.filename):
            shutil.rmtree(self.filename)
        elif os.path.exists(self.filename):
            os.remove(self.filename)

    def test_pickling_simple_types(self):

        obj_list = [
            1, "hello", 5.0,
            (1, 2), ("i", "love", "cricket"),
            [1, 2, "hello"], [1.3, (1,2), "foo"], ["bar", {"foo": "bar"}],
            {"cricket": "best-sport", "test": [1,2,3]}, {"foo": 1.3},
        ]
        for obj in obj_list:
            pickler = gl_pickle.GLPickler(self.filename)
            pickler.dump(obj)
            pickler.close()
            obj_ret = gl_pickle.GLUnpickler(self.filename).load()
            assert obj == obj_ret, "Failed pickling in %s (Got back %s)" % (obj, obj_ret)

    def test_pickling_sarray_types(self):

        sarray_list = [
            gl.SArray([1,2,3]),
            gl.SArray([1.0,2.0,3.5]),
            gl.SArray(["foo", "bar"]),
        ]
        for obj in sarray_list:
            pickler = gl_pickle.GLPickler(self.filename)
            pickler.dump(obj)
            pickler.close()
            obj_ret = gl_pickle.GLUnpickler(self.filename).load()
            assert list(obj) ==  list(obj_ret), \
                       "Failed pickling in %s (Got back %s)" % (obj, obj_ret)

    def test_pickling_sframe_types(self):

        sarray_list = [
            gl.SFrame([1,2,3]),
            gl.SFrame([1.0,2.0,3.5]),
            gl.SFrame(["foo", "bar"]),
        ]
        for obj in sarray_list:
            pickler = gl_pickle.GLPickler(self.filename)
            pickler.dump(obj)
            pickler.close()
            obj_ret = gl_pickle.GLUnpickler(self.filename).load()
            assert_sframe_equal(obj, obj_ret)

    def test_pickling_sgraph_types(self):

        sg_test_1 = gl.SGraph().add_vertices([
                                gl.Vertex(0, {'fluffy': 1}),
                                gl.Vertex(1, {'fluffy': 1, 'woof': 1}),
                                gl.Vertex(2, {})])

        sg_test_2 = gl.SGraph()
        sg_test_2 = sg_test_2.add_vertices([
                            gl.Vertex(x) for x in [0, 1, 2]])
        sg_test_2 = sg_test_2.add_edges([
                        gl.Edge(0, 1, attr={'relationship': 'dislikes'}),
                        gl.Edge(1, 2, attr={'relationship': 'likes'}),
                        gl.Edge(1, 0, attr={'relationship': 'likes'})])

        sarray_list = [
            sg_test_1,
            sg_test_2
        ]
        for obj in sarray_list:
            pickler = gl_pickle.GLPickler(self.filename)
            pickler.dump(obj)
            pickler.close()
            obj_ret = gl_pickle.GLUnpickler(self.filename).load()
            assert_sframe_equal(obj.get_vertices(), obj_ret.get_vertices())
            assert_sframe_equal(obj.get_edges(), obj_ret.get_edges())

    def test_combination_gl_python_types(self):

        sg_test_1 = gl.SGraph().add_vertices([
                                gl.Vertex(1, {'fluffy': 1}),
                                gl.Vertex(2, {'fluffy': 1, 'woof': 1}),
                                gl.Vertex(3, {})])
        sarray_test_1 = gl.SArray([1,2,3])
        sframe_test_1 = gl.SFrame([1,2,3])

        obj_list = [
            [sg_test_1, sframe_test_1, sarray_test_1],
            {0:sg_test_1, 1:sframe_test_1, 2:sarray_test_1}
        ]

        for obj in obj_list:
            pickler = gl_pickle.GLPickler(self.filename)
            pickler.dump(obj)
            pickler.close()
            obj_ret = gl_pickle.GLUnpickler(self.filename).load()
            assert_sframe_equal(obj[0].get_vertices(), obj_ret[0].get_vertices())
            assert_sframe_equal(obj[0].get_edges(), obj_ret[0].get_edges())
            assert_sframe_equal(obj[1], obj_ret[1])
            assert list(obj[2]) ==  list(obj_ret[2])

    def test_pickle_compatibility(self):
        obj_list = [
            1, "hello", 5.0,
            (1, 2), ("i", "love", "cricket"),
            [1, 2, "hello"], [1.3, (1,2), "foo"], ["bar", {"foo": "bar"}],
            {"cricket": "best-sport", "test": [1,2,3]}, {"foo": 1.3},
        ]
        for obj in obj_list:
            file = open(self.filename, 'wb')
            pickler = pickle.Pickler(file)
            pickler.dump(obj)
            file.close()
            obj_ret = gl_pickle.GLUnpickler(self.filename).load()
            assert obj == obj_ret, \
                "Failed pickling in %s (Got back %s)" % (obj, obj_ret)

    def test_cloud_pickle_compatibility(self):
        obj_list = [
            1, "hello", 5.0,
            (1, 2), ("i", "love", "cricket"),
            [1, 2, "hello"], [1.3, (1,2), "foo"], ["bar", {"foo": "bar"}],
            {"cricket": "best-sport", "test": [1,2,3]}, {"foo": 1.3},
        ]
        for obj in obj_list:
            file = open(self.filename, 'wb')
            pickler = cloudpickle.CloudPickler(file)
            pickler.dump(obj)
            file.close()
            obj_ret = gl_pickle.GLUnpickler(self.filename).load()
            assert obj == obj_ret, \
                "Failed pickling in %s (Got back %s)" % (obj, obj_ret)

    def test_relative_path(self):
        # Arrange
        sf1 = gl.SFrame(range(10))
        relative_path = 'tmp/%s' % self.filename

        # Act
        pickler = gl_pickle.GLPickler(relative_path)
        pickler.dump(sf1)
        pickler.close()
        sf2 = gl_pickle.GLUnpickler(relative_path).load()

        # Assert
        assert_sframe_equal(sf1, sf2)

        # Clean up
        shutil.rmtree(relative_path)

    @moto.mock_s3
    def test_save_to_s3(self):
        # Arrange
        os.environ['GRAPHLAB_UNIT_TEST'] = 'PredictiveService'
        BUCKET_NAME = 'foo'
        boto.connect_s3().create_bucket(BUCKET_NAME)
        sf1 = gl.SFrame(range(10))
        S3_PATH = "s3://%s/foobar" % BUCKET_NAME

        # Act
        pickler = gl_pickle.GLPickler(S3_PATH)
        pickler.dump(sf1)
        pickler.close()
        sf2 = gl_pickle.GLUnpickler(S3_PATH).load()

        # Assert
        assert_sframe_equal(sf1, sf2)

        del os.environ['GRAPHLAB_UNIT_TEST']

    def test_backward_compatibility(self):

        # Arrange
        file_name = 's3://gl-internal-datasets/models/1.3/gl-pickle.gl'
        obj = {'foo': gl.SFrame([1,2,3]),
               'bar': gl.SFrame(),
               'foo-bar': ['foo-and-bar', gl.SFrame([1])]}


        # Act
        unpickler = gl_pickle.GLUnpickler(file_name)
        obj_ret = unpickler.load()

        # Assert
        assert_sframe_equal(obj['foo'], obj_ret['foo'])
        assert_sframe_equal(obj['bar'], obj_ret['bar'])
        assert_sframe_equal(obj['foo-bar'][1], obj_ret['foo-bar'][1])
        self.assertEqual(obj['foo-bar'][0], obj_ret['foo-bar'][0])

    def test_save_over_previous(self):

        sarray_list = [
            gl.SFrame([1,2,3]),
            gl.SFrame([1.0,2.0,3.5]),
            gl.SFrame(["foo", "bar"]),
        ]
        for obj in sarray_list:
            pickler = gl_pickle.GLPickler(self.filename)
            pickler.dump(obj)
            pickler.close()

            obj_ret = gl_pickle.GLUnpickler(self.filename).load()
            assert_sframe_equal(obj, obj_ret)

            pickler = gl_pickle.GLPickler(self.filename)
            pickler.dump(obj)
            pickler.close()


