import os
import unittest
import tempfile
from ..util import file_util as fu

class FileUtilTests(unittest.TestCase):
    def setUp(self):
        self.local_path = 'tmp/a/b/c'
        self.s3_path = 's3://a/b/c'
        self.http_path = 'http://a.b.c/d'
        self._get_env()

    def _get_env(self):
        self.run_s3_test = ('FILE_UTIL_TEST_S3_BUCKET' in os.environ) and \
                            'AWS_ACCESS_KEY_ID' in os.environ and \
                            'AWS_SECRET_ACCESS_KEY' in os.environ

        if self.run_s3_test:
            self.s3_test_path = os.environ['FILE_UTIL_TEST_S3_BUCKET']
        else:
            self.s3_test_path = None

    def test_get_protocol(self):
        self.assertEqual(fu.get_protocol(self.local_path), '')
        self.assertEqual(fu.get_protocol(self.s3_path), 's3')
        self.assertEqual(fu.get_protocol(self.http_path), 'http')

    def test_s3_load_save(self):
        # skip tests if not setup appropriately
        if self.s3_test_path is None:
            return

        # non exist local file
        with self.assertRaises(RuntimeError):
            fu.upload_to_s3('~/tmp/__abc_non_exist', self.s3_test_path, {})

        # s3 path not correct
        with self.assertRaises(RuntimeError):
            fu.download_from_s3('abc', '~/tmp/some_new_file')

        # local path not correct
        with self.assertRaises(RuntimeError):
            fu.download_from_s3('s3:/a/b/', 'http:/a.b.c/d')

        # invalid s3 path
        with self.assertRaises(RuntimeError):
            with tempfile.NamedTemporaryFile(delete=True) as f:
                f.close()
                fu.upload_to_s3(f.name, 'abc', {})

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write('abc')
            f.close()

            fu.upload_to_s3(f.name, self.s3_test_path)

            os.remove(f.name)
            self.assertFalse(os.path.exists(f.name))

            fu.download_from_s3(self.s3_test_path, f.name)
            self.assertTrue(os.path.exists(f.name))

            with open(f.name, 'r') as f1:
                s = f1.read()
                self.assertEqual(s, 'abc')

    def test_is_local_path(self):
        self.assertTrue(fu.is_local_path(self.local_path))
        self.assertFalse(fu.is_local_path(self.s3_path))
        self.assertFalse(fu.is_local_path(self.http_path))

    def test_is_s3_path(self):
        self.assertFalse(fu.is_s3_path(self.local_path))
        self.assertTrue(fu.is_s3_path(self.s3_path))
        self.assertFalse(fu.is_s3_path(self.http_path))

    def test_expand_full_path(self):
        if not 'HOME' in os.environ:
            raise RuntimeError('warning: cannot find $HOME key in environment')
        else:
            home = os.environ['HOME']
            self.assertTrue(fu.expand_full_path('~/tmp'), os.path.join(home, 'tmp'))
            self.assertTrue(fu.expand_full_path('tmp'), os.path.join(os.getcwd(), 'tmp'))

    def test_parse_s3_path(self):
        s3_path = 's3://a/b/c'
        (bucket, path) = fu.parse_s3_path(s3_path)
        self.assertEqual(bucket, 'a')
        self.assertEqual(path, 'b/c')

        s3_path = 'S3://a/b/c/'
        (bucket, path) = fu.parse_s3_path(s3_path)
        self.assertEqual(bucket, 'a')
        self.assertEqual(path, 'b/c')

    def test_cert_directories(self):
        import sframe as sf
        import certifi
        self.assertEqual(sf.get_runtime_config()['GRAPHLAB_FILEIO_ALTERNATIVE_SSL_CERT_FILE'], certifi.where())
        self.assertEqual(sf.get_runtime_config()['GRAPHLAB_FILEIO_ALTERNATIVE_SSL_CERT_DIR'], "")
