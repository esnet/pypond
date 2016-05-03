"""
Implementation of Pond Collection class.
"""

from pyrsistent import freeze

from .bases import BoundedIn
from .exceptions import CollectionException, CollectionWarning
from .util import unique_id, is_pvector


class Collection(BoundedIn):  # pylint: disable=too-many-public-methods
    """
    A collection is a list of Events. You can construct one out of either
    another collection, or a list of Events. You can addEvent() to a collection
    and a new collection will be returned.

    Basic operations on the list of events are also possible. You
    can iterate over the collection with a for..of loop, get the size()
    of the collection and access a specific element with at().
    """
    def __init__(self, instance_or_list, copy_events=True):
        """
        Initialize from copy, lists, etc.

        instance_or_list can be:
            * a Collection object (copy ctor)
            * a python list
            * a pyrsistent.pvector
        """
        super(Collection, self).__init__()

        self._id = unique_id('collection-')
        self._event_list = None
        self._type = None

        if isinstance(instance_or_list, Collection):
            other = instance_or_list
            if copy_events:
                # pylint: disable=protected-access
                self._event_list = other._event_list
                self._type = other._type
            else:
                self._event_list = freeze(list())

        elif isinstance(instance_or_list, list):
            events = list()
            for i in instance_or_list:
                self._check(i)
                events.append(i)
            self._event_list = freeze(events)

        elif is_pvector(instance_or_list):
            self._event_list = instance_or_list

        else:
            msg = 'Arg was not a Collection, list or pvector - '
            msg += 'initializing event list to empty pvector'
            self._warn(msg, CollectionWarning)

            self._event_list = freeze(list())

    def to_json(self):
        """
        Returns the collection as json object.

        This is actually like json.loads(s) - produces the
        actual vanilla data structure."""
        raise NotImplementedError

    def to_string(self):
        """
        Retruns the collection as a string, useful for serialization.

        In JS land, this is synonymous with __str__ or __unicode__
        """
        raise NotImplementedError

    def type(self):
        """Event object type."""
        raise NotImplementedError

    def size(self):
        """Number of items in collection."""
        return len(self._event_list)

    def size_valid(self):
        """
        Returns the number of valid items in this collection.

        Uses the fieldName and optionally a function passed in
        to look up values in all events. It then counts the number
        that are considered valid, i.e. are not NaN, undefined or null.
        """
        raise NotImplementedError

    def at(self, pos):  # pylint: disable=invalid-name
        """Returns an item in the collection by its position."""
        raise NotImplementedError

    def at_time(self, time):
        """Return an item by time."""
        raise NotImplementedError

    def first(self):
        """First item."""
        raise NotImplementedError

    def last(self):
        """Last item."""
        raise NotImplementedError

    def bisect(self, t, b):  # pylint: disable=invalid-name
        """
        Finds the index that is just less than the time t supplied.
        In other words every event at the returned index or less
        has a time before the supplied t, and every sample after the
        index has a time later than the supplied t.

        Optionally supply a begin index to start searching from.
        """
        raise NotImplementedError

    def events(self):
        """
        Generator to allow for..of loops over series.events()
        """
        raise NotImplementedError

    def event_list(self):
        """Get internet list of events."""
        raise NotImplementedError

    # Series range

    def range(self):
        """
        From the range of times, or Indexes within the TimeSeries, return
        the extents of the TimeSeries as a TimeRange.
        @return {TimeRange} The extents of the TimeSeries
        """
        raise NotImplementedError

    # event list mutation

    def add_event(self):
        """
        Add even and return a new object.
        """
        raise NotImplementedError

    def slice(self, begin, end):
        """
        Perform a slice of events within the Collection, returns a new
        Collection representing a portion of this TimeSeries from begin up to
        but not including end.
        """
        raise NotImplementedError

    def filter(self, func):
        """Generate a filtered event list."""
        raise NotImplementedError

    def map(self, func):
        """Map function."""
        raise NotImplementedError

    def clean(self, field_spec):
        """
        Returns a new Collection by testing the fieldSpec
        values for being valid (not NaN, null or undefined).
        The resulting Collection will be clean for that fieldSpec.
        """
        raise NotImplementedError

    # sum/min/max etc

    def count(self, field_spec='value'):
        """Get count - calls self.size_valid(field_spec)"""
        raise NotImplementedError

    def sum(self, field_spec='value'):
        """Get sum"""
        raise NotImplementedError

    def max(self, field_spec='value'):
        """Get max"""
        raise NotImplementedError

    def min(self, field_spec='value'):
        """Get min"""
        raise NotImplementedError

    def avg(self, field_spec='value'):
        """Get avg"""
        raise NotImplementedError

    def mean(self, field_spec='value'):
        """Get mean"""
        raise NotImplementedError

    def median(self, field_spec='value'):
        """Get median"""
        raise NotImplementedError

    def stdev(self, field_spec='value'):
        """Get std dev"""
        raise NotImplementedError

    def __str__(self):
        """call to_string()"""
        raise NotImplementedError
