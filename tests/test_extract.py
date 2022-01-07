"""Check that data can be extracted from structured data files.

The `load_cdc_data` function should load a collection of `CDCTrackingObject`s from a
CSV file.

To run these tests from the project root, run:

    $ python3 -m unittest --verbose tests.test_extract

"""
import collections.abc
import datetime
import pathlib
import math
import unittest

from extract import load_cdc_data
from models import CDCTrackingObject
from helpers import datetime_to_str
from collections import defaultdict

TESTS_ROOT = (pathlib.Path(__file__).parent).resolve()
TEST_COVID_FILE = TESTS_ROOT / 'test-covid-2021-states-covid-history.csv'

class TestLoadCdcObjs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cdc_objs = load_cdc_data(TEST_COVID_FILE)
        cls.cdc_data_by_state = defaultdict(list)
        cls.cdc_data_by_date = defaultdict(list)
        for d in cls.cdc_objs:
           cls.cdc_data_by_date[datetime_to_str(d.date)].append(d)
           cls.cdc_data_by_state[d.state].append(d)

    @classmethod
    def get_first_cdc_obj_or_none(cls):
        try:
            return next(iter(cls.cdc_objs))
        except StopIteration:
            return None

    def test_cdc_objs_are_collection(self):
        self.assertIsInstance(self.cdc_objs, collections.abc.Collection)

    def test_cdc_data_contain_cdc_tracking_objects(self):
        cdc_obj = self.get_first_cdc_obj_or_none()
        self.assertIsNotNone(cdc_obj)
        self.assertIsInstance(cdc_obj, CDCTrackingObject)

    def test_cdc_data_contain_all_elements(self):
        self.assertEqual(len(self.cdc_objs), 3696)

    def test_cdc_2021_02_01_data(self):
        data_2021_02_01 = self.cdc_data_by_date['2021-02-01']
        self.assertEqual(len(data_2021_02_01), 56)

    def test_cdc_2021_02_01_CA_data(self):
        data_2021_02_01 = self.cdc_data_by_date['2021-02-01']
        for d in data_2021_02_01:
            if d.state == 'CA':
               cal = d
        self.assertEqual(cal.death, 40908)

    def test_cdc_2021_03_01_WI_data(self):
        data_2021_03_01 = self.cdc_data_by_date['2021-03-01']
        for d in data_2021_03_01:
            if d.state == 'WI':
               wisc = d
        self.assertEqual(wisc.death, 7014)


if __name__ == '__main__':
    unittest.main()
