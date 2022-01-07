from helpers import cd_to_datetime, datetime_to_str
import datetime

class CDCTrackingObject:
    """A CDCTrackingObject
    A CDCTrackingObject corresponds to a row of the CSV data with a subset of columns of interest.
    """

    def __init__(self, **info):
        """Create a new `CDCTrackingObject`.
        :param info: A dictionary of excess keyword arguments supplied to the constructor.
        """
        self.date = cd_to_datetime(info.get("date"))
        self.state = info.get("state")
        self.death = info.get("death")
        self.deathIncrease = info.get("deathIncrease")
        self.hospitalizedCumulative = info.get("hospitalizedCumulative")
        self.hospitalizedCurrently = info.get("hospitalizedCurrently")
        self.inIcuCurrently = info.get("inIcuCurrently")
        self.negative = info.get("negative")
        self.onVentilatorCumulative = info.get("onVentilatorCumulative")
        self.onVentilatorCurrently = info.get("onVentilatorCurrently")
        self.positive = info.get("positive")
        self.totalTestResultsIncrease = info.get("totalTestResultsIncrease")
        self.totalTestsAntibody = info.get("totalTestsAntibody")
        self.totalTestsAntigen = info.get("totalTestsAntigen")
        self.totalTestsViral = info.get("totalTestsViral")

    @property
    def date_str(self):
        """Return a formatted representation of date.

        The `datetime_to_str` method converts a `datetime` object to a
        formatted string that can be used in human-readable representations and
        in serialization to CSV and JSON files.
        """
        if self.date:
            return datetime_to_str(self.date)
        return "an unknown time"

    def __str__(self):
        return '\n'.join((f"On {self.date_str}, state {self.state} has {self.death} deaths. The total number of people currently hospitalized is {self.hospitalizedCurrently}.",
                         f"Total number of people who currently hostpitalized on {self.date_str} is {self.hospitalizedCurrently}.",
                         f"Total number of people who currently on ventilator on {self.date_str} is {self.onVentilatorCurrently}."))

    def __repr__(self):
        """Return `repr(self)`, a computer-readable string representation of this object."""
        return (f"CDCTrackingObject(date={self.date_str!r}, state={self.state!r}, "
                f"death={self.death:.3f}, deathIncrease={self.deathIncrease!r})")
