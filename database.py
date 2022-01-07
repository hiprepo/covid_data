from collections import defaultdict
from helpers import datetime_to_str

class CDCTrackingDatabase:
    """A database of Covid tracking data.

    A `CDCTrackingDatabase` contains a collection of cdc tracking data from
    2020-01-13 to 2021-03-07 of all US states.
    It additionally maintains a few auxiliary data structures to
    help fetch tracking data by date and by US state abbreviation.
    """
    def __init__(self, cdcObjs):
        self._cdcObjs = cdcObjs
        self._data_by_date = defaultdict(list)
        self._data_by_state = defaultdict(list)

        for _, d in enumerate(self._cdcObjs):
            self._data_by_date[datetime_to_str(d.date)].append(d)
            self._data_by_state[d.state].append(d)

    def get_tracking_data_by_date(self, date):
        return self._data_by_date[date]

    def get_tracking_data_by_state(self, state):
        return self._data_by_state[state]

    def query(self, filters=()):
        """
        :param filters: A collection of filters capturing user-specified criteria.
        :return: A stream of matching COVID tracking data objects.
        """
        if filters:
            for obj in self._cdcObjs:
                if all(map(lambda f: f(obj), filters)):
                    yield obj
        else:
            # return all the COVID tracking data objects if they are no filters
            for obj in self._cdcObjs:
                yield obj

