"""Provide filters for querying CDC tracking data and limit the generated results.

The `create_filters` function produces a collection of objects that is used by
the `query` method to generate a stream of `CDCTracking` objects that match
all of the desired criteria. The arguments to `create_filters` are provided by
the main module and originate from the user's command-line options.

This function returns a collection of instances of subclasses
of `AttributeFilter` - a 1-argument callable constructed from a comparator, a
reference value, and a class method `get` that subclasses can override to
fetch an attribute of interest.

The `limit` function simply limits the maximum number of values produced by an
iterator.

"""
import operator


class UnsupportedCriterionError(NotImplementedError):
    """A filter criterion is unsupported."""


class AttributeFilter:
    """A general superclass for filters on comparable attributes.

    An `AttributeFilter` represents the search criteria pattern comparing some
    attribute of a cdc tracking data object to a reference value. It
    essentially functions as a callable predicate for whether a `CDCTracking`
    object satisfies the encoded criterion.

    It is constructed with a comparator operator and a reference value, and
    calling the filter (with __call__) executes `get(covid_data) OP value` (in
    infix notation).

    Concrete subclasses can override the `get` classmethod to provide custom
    behavior to fetch a desired attribute from the given `covid data object`.
    """
    
    def __init__(self, op, value):
        """Construct a new `AttributeFilter` from an binary predicate and a reference value.

        The reference value will be supplied as the second (right-hand side)
        argument to the operator function. For example, an `AttributeFilter`
        with `op=operator.le` and `value=10` will, when called on a cdc tracking data object,
        evaluate `some_attribute <= 10`.

        :param op: A 2-argument predicate comparator (such as `operator.le`).
        :param value: The reference value to compare against.
        """
        self.op = op
        self.value = value

    def __call__(self, covid_data):
        """Invoke `self(covid_data)`."""
        return self.op(self.get(covid_data), self.value)

    @classmethod
    def get(cls, covid_data):
        """Get an attribute of interest from a covid data object.

        Concrete subclasses must override this method to get an attribute of
        interest from the supplied `covid data object`.

        :param covid_data: A `covid data object` on which to evaluate this filter.
        :return: The value of an attribute of interest, comparable to `self.value` via `self.op`.
        """
        raise UnsupportedCriterionError

    def __repr__(self):
        """Repr method used to compare filter attribute."""
        return f"{self.__class__.__name__}(op=operator.{self.op.__name__}, value={self.value})"
   
class DateFilter(AttributeFilter):
    """Subclass of AttributeFilter to filter covid data object objects by date."""

    @classmethod
    def get(cls, covid_data):
        """Return covid_data.date converted to datetime.datetime object for the date filter.
        
        Args:
            covid_data: A covid data object object.
        Returns:
            [datetime.datetime]: Converted time to datetime object.
            
        """
        return covid_data.date.date()

class StateFilter(AttributeFilter):
    """Subclass of AttributeFilter to filter covid data objects by State."""

    @classmethod
    def get(cls, covid_data):
        """Return the State abbreviation of the covid data  object for the State filter.
        
        Args:
            covid_data : A covid data object.
        Returns:
            [str]: Returns the State where this covid data is collected.
            
        """
        return covid_data.state
    
class HospitalizedFilter(AttributeFilter):
    """Subclass of AttributeFilter to filter covid data objects by number of hospitalizedCurrently."""

    @classmethod
    def get(cls, covid_data):
        """Return covid_data.hospitalizedCurrently for the Hospitalized filter.
        
        Args:
            covid_data : A covid data object.
        Returns:
            [int]: Returns the number of people hospitalized.
            
        """
        return covid_data.hospitalizedCurrently
    
class IcuFilter(AttributeFilter):
    """Subclass of AttributeFilter to filter covid data objects by number of inIcuCurrently."""

    @classmethod
    def get(cls, covid_data):
        """Return covid_data.inIcuCurrently for the Icu filter.
        
        Args:
            covid_data : A covid data object.
        Returns:
            [int]: Returns the number of people in ICU
            
        """
        return covid_data.inIcuCurrently

class OnVentFilter(AttributeFilter):
    """Subclass to filter covid data objects by number of onVentilatorCurrently."""

    @classmethod
    def get(cls, covid_data):
        """Return covid_data.onVentilatorCurrently for the Icu filter.
        
        Args:
            covid_data : A covid data object.
        Returns:
            [int]: Returns the number of people on ventilator
            
        """
        return covid_data.onVentilatorCurrently

class DeathFilter(AttributeFilter):
    """Subclass to filter covid data objects by number of deaths."""

    @classmethod
    def get(cls, covid_data):
        """Return the covid_data.death for the death filter.
        
        Args:
            covid_data : A covid data object.
        Returns:
            [int]: Returns the number of deaths of a covid data object.
            
        """
        return covid_data.death

def create_filters(date=None, start_date=None, end_date=None,
                   state=None,
                   hospitalized_min=None, hospitalized_max=None,
                   icu_min=None, icu_max=None,
                   onvent_min=None, onvent_max=None,
                   death_min=None, death_max=None):
    """Create a collection of filters from user-specified criteria.

    :param date: A `date` on which a matching covid data object occurs.
    :param start_date: A `date` on or after which a matching `covid data object`.
    :param end_date: A `date` on or before which a matching `covid data object`.
    :param state: A `state` on or before which a matching `covid data object`.
    :param hospitalized_min: A minimum number of people hospitalized for a matching `covid data object`.
    :param hospitalized_max: A maximum number of people hospitalized for a matching `covid data object`.
    :param icu_min: A minimum number of people in ICU for a matching `covid data object`.
    :param icu_max: A maximum number of people in ICU for a matching `covid data object`.
    :param onvent_min: A minimum number of people on ventilator of a matching `covid data object`.
    :param onvent_max: A maximum number of people on ventilator of a matching `covid data object`.
    :param death_min: A minimum number of deaths of a matching `covid data object`.
    :param death_max: A maximum number of deaths of a matching `covid data object`.
    :return: A collection of filters for use with `query`.
    """
    filters = []
    
    if date:
        filters.append(DateFilter(operator.eq, date))
    if start_date:
        filters.append(DateFilter(operator.ge, start_date))
    if end_date:
        filters.append(DateFilter(operator.le, end_date))
    if state:
        filters.append(StateFilter(operator.eq, state))

    if hospitalized_min:
        filters.append(HospitalizedFilter(operator.ge, hospitalized_min))
    if hospitalized_max:
        filters.append(HospitalizedFilter(operator.le, hospitalized_max))

    if icu_min:
        filters.append(IcuFilter(operator.ge, icu_min))
    if icu_max:
        filters.append(IcuFilter(operator.le, icu_max))

    if onvent_min:
        filters.append(OnVentFilter(operator.ge, onvent_min))
    if onvent_max:
        filters.append(OnVentFilter(operator.le, onvent_max))

    if death_min:
        filters.append(DeathFilter(operator.ge, death_min))
    if death_max:
        filters.append(DeathFilter(operator.le, death_max))

    return filters


def limit(iterator, n=None):
    """Produce a limited stream of values from an iterator.

    If `n` is 0 or None, don't limit the iterator at all.

    :param iterator: An iterator of values.
    :param n: The maximum number of values to produce.
    :yield: The first (at most) `n` values from the iterator.
    """
    if n == 0 or n is None:
        return iterator
    return [x for i, x in enumerate(iterator) if i < n]
