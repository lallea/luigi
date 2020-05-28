from __future__ import print_function

import collections
import os
import unittest

from avro.datafile import DataFileReader
from avro.io import DatumReader

from luigi import LocalTarget
from luigi.contrib.avro import AvroFormat


class AvroFormatTest(unittest.TestCase):
    PATH_PREFIX = '/tmp/luigi_avro_test'

    def setUp(self):
        self.path = self.PATH_PREFIX + '-' + str(self.id())
        if os.path.exists(self.path):
            os.remove(self.path)

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def assertDataEqual(self, expected, actual, msg=None):
        """Backported from python 3, simplified"""
        first = sorted([tuple(sorted(r.items())) for r in expected])
        second = sorted([tuple(sorted(r.items())) for r in actual])

        self.assertEqual(first, second)

    def test_write_no_schema(self):
        t = LocalTarget(self.path, AvroFormat())
        rec1 = {'a': '1', 'b': 2}
        rec2 = {'a': '11', 'b': 12}

        with t.open('w') as f:
            f.write(rec1)
            f.write(rec2)

        with open(self.path, mode='rb') as avro_file:
            with DataFileReader(avro_file, DatumReader()) as a:
                actual = list(a)
                self.assertDataEqual([rec1, rec2], actual)
