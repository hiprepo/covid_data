"""Check that `query`ing an `CDCTrackingDatabase` accurately produces cdc real data.

There are a plethora of ways to combine the arguments to `create_filters`, which
correspond to different command-line options. This modules tests the options in
isolation, in pairs, and in more complicated combinations. Althought the tests
are not entirely exhaustive, any implementation that passes all of these tests
is most likely up to snuff.

To run these tests from the project root, run::

    $ python3 -m unittest --verbose tests.test_query

"""
import datetime
import pathlib
import unittest

from database import CDCTrackingDatabase
from extract import load_cdc_data
from filters import create_filters
from collections import defaultdict
from helpers import cd_to_datetime, datetime_to_str


TESTS_ROOT = (pathlib.Path(__file__).parent).resolve()
TEST_COVID_FILE = TESTS_ROOT / 'test-covid-2021-states-covid-history.csv'


class TestQuery(unittest.TestCase):
    # Set longMessage to True to enable lengthy diffs between set comparisons.
    longMessage = False

    @classmethod
    def setUpClass(cls):
        cls.cdc_objs = load_cdc_data(TEST_COVID_FILE)
        cls.db = CDCTrackingDatabase(cls.cdc_objs)
        cls.cdc_data_by_state = defaultdict(list)
        cls.cdc_data_by_date = defaultdict(list)
        for d in cls.cdc_objs:
           cls.cdc_data_by_date[datetime_to_str(d.date)].append(d)
           cls.cdc_data_by_state[d.state].append(d)

    def test_query_all(self):
        expected = set(self.cdc_objs)
        self.assertGreater(len(expected), 0)

        filters = create_filters()
        received = set(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    ###############################################
    # Single filters and pairs of related filters #
    ###############################################

    def test_query_data_on_feb_1(self):
        date = datetime.date(2021, 2, 1)

        expected = self.cdc_data_by_date['2021-02-01']
        self.assertGreater(len(expected), 0)

        filters = create_filters(date=date)
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_after_feb_1_2021(self):
        start_date = cd_to_datetime('2021-02-01').date()
        s_date = datetime.datetime(2021, 2, 1)

        expected = []
        for d in self.cdc_objs:
           if d.date >= s_date:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(start_date=start_date)
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_before_mar_3_2021(self):
        end_date = cd_to_datetime('2021-03-03').date()
        e_date = datetime.datetime(2021, 3, 3)

        expected = []
        for d in self.cdc_objs:
           if d.date <= e_date:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(end_date=end_date)
        received = list(self.db.query(filters))
    
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_conflicting_date_bounds(self):
        start_date = datetime.date(2020, 10, 1)
        end_date = datetime.date(2020, 4, 1)

        expected = list()

        filters = create_filters(start_date=start_date, end_date=end_date)
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_bounds_and_a_specific_date(self):
        start_date = datetime.date(2021, 2, 1)
        date = datetime.date(2021, 2, 14)
        end_date = datetime.date(2021, 3, 1)
        specific_date = datetime.datetime(2021, 2, 14)

        expected = []
        for d in self.cdc_objs:
           if d.date == specific_date:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(date=date, start_date=start_date, end_date=end_date)
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_min_hospitalized(self):
        hospitalized_min = 100

        expected = []
        for d in self.cdc_objs:
           if d.hospitalizedCurrently >= hospitalized_min:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(hospitalized_min=hospitalized_min)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_max_hospitalized(self):
        hospitalized_max = 2000

        expected = []
        for d in self.cdc_objs:
           if d.hospitalizedCurrently <= hospitalized_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(hospitalized_max=hospitalized_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_max_hospitalized_and_min_hospitalized(self):
        hospitalized_max = 2000
        hospitalized_min = 100

        expected = []
        for d in self.cdc_objs:
           if hospitalized_min <= d.hospitalizedCurrently <= hospitalized_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(hospitalized_min=hospitalized_min, hospitalized_max=hospitalized_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_max_hospitalized_and_min_hospitalized_conflicting(self):
        hospitalized_max = 100
        hospitalized_min = 4000

        expected = list()

        filters = create_filters(hospitalized_min=hospitalized_min, hospitalized_max=hospitalized_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")


    def test_query_data_min_icu(self):
        icu_min = 200

        expected = []
        for d in self.cdc_objs:
           if d.inIcuCurrently >= icu_min:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(icu_min=icu_min)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_max_icu(self):
        icu_max = 4000

        expected = []
        for d in self.cdc_objs:
           if d.inIcuCurrently <= icu_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(icu_max=icu_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_max_icu_and_min_icu(self):
        icu_min = 100
        icu_max = 3000

        expected = []
        for d in self.cdc_objs:
           if icu_min <= d.inIcuCurrently <= icu_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(icu_min=icu_min, icu_max=icu_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_max_icu_and_min_icu_conflicting(self):
        icu_max = 100
        icu_min = 4000

        expected = list()

        filters = create_filters(icu_min=icu_min, icu_max=icu_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_min_onvent(self):
        onvent_min = 500

        expected = []
        for d in self.cdc_objs:
           if d.onVentilatorCurrently >= onvent_min:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(onvent_min=onvent_min)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_max_onvent(self):
        onvent_max = 5000

        expected = []
        for d in self.cdc_objs:
           if d.onVentilatorCurrently <= onvent_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(onvent_max=onvent_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_max_onvent_and_min_onvent(self):
        onvent_max = 4000
        onvent_min = 1000

        expected = []
        for d in self.cdc_objs:
           if onvent_min <= d.onVentilatorCurrently <= onvent_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(onvent_min=onvent_min, onvent_max=onvent_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_max_onvent_and_min_onvent_conflicting(self):
        onvent_max = 50
        onvent_min = 1000

        expected = list()

        filters = create_filters(onvent_min=onvent_min, onvent_max=onvent_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_max_death(self):
        death_max = 2000

        expected = []
        for d in self.cdc_objs:
           if d.death <= death_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(death_max=death_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_min_death(self):
        death_min = 10

        expected = []
        for d in self.cdc_objs:
           if d.death >= death_min:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(death_min=death_min)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_max_death_and_min_death(self):
        death_max = 2000
        death_min = 10

        expected = []
        for d in self.cdc_objs:
           if death_min <= d.death <= death_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(death_min=death_min, death_max=death_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_with_max_death_and_min_death_conflicting(self):
        death_max = 10
        death_min = 20

        expected = list()

        filters = create_filters(death_min=death_min, death_max=death_max)
        received = list(self.db.query(filters))

        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_state_CA(self):
        state = 'CA'

        expected = self.cdc_data_by_state[state]
        self.assertGreater(len(expected), 0)

        filters = create_filters(state=state)
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_state_invalid(self):
        state = 'KK'

        expected = self.cdc_data_by_state[state]
        self.assertEqual(len(expected), 0)

        filters = create_filters(state=state)
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")


    ###########################
    # Combinations of filters #
    ###########################

    def test_query_data_on_2021_march_2_with_max_hospitalized(self):
        date = datetime.date(2021, 3, 2)
        hospitalized_max = 2000

        data_03_02_2021 = self.cdc_data_by_date['2021-03-02']
        expected = []
        for d in data_03_02_2021:
           if d.hospitalizedCurrently <= hospitalized_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(date=date, hospitalized_max=hospitalized_max)
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_on_2021_march_2_with_min_hospitalized(self):
        date = datetime.date(2021, 3, 2)
        hospitalized_min = 100

        data_03_02_2021 = self.cdc_data_by_date['2021-03-02']
        expected = []
        for d in data_03_02_2021:
           if d.hospitalizedCurrently >= hospitalized_min:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(date=date, hospitalized_min=hospitalized_min)
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_in_march_with_min_icu_and_max_icu(self):
        start_date = datetime.date(2021, 1, 1)
        end_date = datetime.date(2021, 3, 1)
        icu_max = 1000
        icu_min = 200
      
        s_date = datetime.datetime(2021, 1, 1)
        e_date = datetime.datetime(2021, 3, 1)

        expected = []
        for d in self.cdc_objs:
           if s_date <= d.date <= e_date and icu_min <= d.inIcuCurrently <= icu_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(
            start_date=start_date, end_date=end_date,
            icu_min=icu_min, icu_max=icu_max,
        )
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_in_january_with_hospitalized_bounds_and_max_icu(self):
        start_date = datetime.date(2021, 1, 1)
        end_date = datetime.date(2021, 1, 31)
        s_date = datetime.datetime(2021, 1, 1)
        e_date = datetime.datetime(2021, 1, 31)
        hospitalized_max = 4000
        hospitalized_min = 100
        icu_max = 200

        expected = []
        for d in self.cdc_objs:
           if s_date <= d.date <= e_date and hospitalized_min <= d.hospitalizedCurrently <= hospitalized_max and d.inIcuCurrently <= icu_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(
            start_date=start_date, end_date=end_date,
            hospitalized_min=hospitalized_min, hospitalized_max=hospitalized_max,
            icu_max=icu_max
        )
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_in_march_with_hospitalized_and_onvent_bounds(self):
        start_date = datetime.date(2021, 3, 1)
        end_date = datetime.date(2021, 3, 31)
        s_date = datetime.datetime(2021, 3, 1)
        e_date = datetime.datetime(2021, 3, 31)
        hospitalized_max = 4000
        hospitalized_min = 100
        onvent_max = 2000
        onvent_min = 10

        expected = []
        for d in self.cdc_objs:
           if s_date <= d.date <= e_date and hospitalized_min <= d.hospitalizedCurrently <= hospitalized_max and \
              onvent_min <= d.onVentilatorCurrently <= onvent_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(
            start_date=start_date, end_date=end_date,
            hospitalized_min=hospitalized_min, hospitalized_max=hospitalized_max,
            onvent_min=onvent_min, onvent_max=onvent_max
        )
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_in_winter_with_hospitalized_and_icu_bounds_and_max_death(self):
        start_date = datetime.date(2021, 1, 1)
        end_date = datetime.date(2021, 3, 1)
        s_date = datetime.datetime(2021, 1, 1)
        e_date = datetime.datetime(2021, 3, 1)
        hospitalized_max = 3000
        hospitalized_min = 500
        icu_max = 20000
        icu_min = 100
        death_max = 3000

        expected = []
        for d in self.cdc_objs:
           if s_date <= d.date <= e_date and hospitalized_min <= d.hospitalizedCurrently <= hospitalized_max and \
              icu_min <= d.inIcuCurrently <= icu_max and \
              d.death <= death_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(
            start_date=start_date, end_date=end_date,
            hospitalized_min=hospitalized_min, hospitalized_max=hospitalized_max,
            icu_min=icu_min, icu_max=icu_max,
            death_max=death_max
        )
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_in_winter_with_all_bounds(self):
        start_date = datetime.date(2021, 1, 1)
        end_date = datetime.date(2021, 3, 1)
        s_date = datetime.datetime(2021, 1, 1)
        e_date = datetime.datetime(2021, 3, 1)
        hospitalized_max = 5000
        hospitalized_min = 1000
        icu_max = 2500
        icu_min = 500
        onvent_max = 5000
        onvent_min = 100
        death_max = 10000
        death_min = 500

        expected = []
        for d in self.cdc_objs:
           if s_date <= d.date <= e_date and hospitalized_min <= d.hospitalizedCurrently <= hospitalized_max and \
              icu_min <= d.inIcuCurrently <= icu_max and \
              onvent_min <= d.onVentilatorCurrently <= onvent_max and \
              death_min <= d.death <= death_max:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(
            start_date=start_date, end_date=end_date,
            hospitalized_min=hospitalized_min, hospitalized_max=hospitalized_max,
            icu_min=icu_min, icu_max=icu_max,
            onvent_min=onvent_min, onvent_max=onvent_max,
            death_min=death_min, death_max=death_max
        )
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

    def test_query_data_in_winter_with_all_bounds_in_OH(self):
        start_date = datetime.date(2021, 1, 1)
        end_date = datetime.date(2021, 3, 1)
        s_date = datetime.datetime(2021, 1, 1)
        e_date = datetime.datetime(2021, 3, 1)
        hospitalized_max = 5000
        hospitalized_min = 1000
        icu_max = 2500
        icu_min = 500
        onvent_max = 5000
        onvent_min = 100
        death_max = 10000
        death_min = 500
        state = 'OH'

        expected = []
        for d in self.cdc_objs:
           if s_date <= d.date <= e_date and hospitalized_min <= d.hospitalizedCurrently <= hospitalized_max and \
              icu_min <= d.inIcuCurrently <= icu_max and \
              onvent_min <= d.onVentilatorCurrently <= onvent_max and \
              death_min <= d.death <= death_max and \
              d.state == state:
              expected.append(d)
        self.assertGreater(len(expected), 0)

        filters = create_filters(
            start_date=start_date, end_date=end_date,
            hospitalized_min=hospitalized_min, hospitalized_max=hospitalized_max,
            icu_min=icu_min, icu_max=icu_max,
            onvent_min=onvent_min, onvent_max=onvent_max,
            death_min=death_min, death_max=death_max,
            state=state
        )
        received = list(self.db.query(filters))
        self.assertEqual(expected, received, msg="Computed results do not match expected results.")

if __name__ == '__main__':
    unittest.main()
