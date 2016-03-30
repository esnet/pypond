"""
Implementation of Pond TimeRange classes.

http://software.es.net/pond/#/timerange
"""
import datetime

from pyrsistent import freeze

from .util import is_pvector, dt_from_ms, dt_is_aware
from .exceptions import TimeRangeException, NAIVE_MESSAGE


class TimeRangeBase(object):
    """Base for TimeRange"""

    @staticmethod
    def sanitize_list_input(list_type):
        """
        Validate input when a pvector, list or tuple is passed in
        as a constructor arg.
        """
        # two elements
        if len(list_type) != 2:
            raise TimeRangeException('list/tuple/vector input must have two elements.')
        # datetime or int
        if not isinstance(list_type[0], (int, datetime.datetime)) or not \
                isinstance(list_type[1], (int, datetime.datetime)):
            raise TimeRangeException('list/tuple/vector elements must be ints or datetime objects.')
        # make sure both elements are the same type
        try:
            list_type[0] > list_type[1]
        except TypeError:
            raise TimeRangeException('both list/tuple/vector elements must be the same type.')

        # if we already have datetimes, we're good to go
        if isinstance(list_type[0], datetime.datetime):
            return freeze(list(list_type))
        else:
            # we must have ints then - convert
            return freeze([dt_from_ms(list_type[0]), dt_from_ms(list_type[1])])

    @staticmethod
    def validate_range(range_obj):
        """
        Make sure the datetimes are aware and that that the end is not
        chronologically before the begin.
        """
        if not dt_is_aware(range_obj[0]) or not dt_is_aware(range_obj[1]):
            raise TimeRangeException(NAIVE_MESSAGE)

        if range_obj[0] > range_obj[1]:
            msg = 'Invalid range - end {e} is earlier in time than begin {b}'.format(
                e=range_obj[1], b=range_obj[0])
            raise TimeRangeException(msg)


class TimeRange(TimeRangeBase):  # pylint: disable=too-many-public-methods
    """
    Builds a new TimeRange which may be of several different formats:
    - Another TimeRange (copy constructor)
    - A python tuple, list or pyrsistent.PVector object containing two
      python datetime objects or ms timestamps.
    - Two arguments, begin and end, each of which may be a datetime object,
      or a ms timestamp.
    """
    def __init__(self, instance_or_begin, end=None):
        """
        Construct the object using the aforementioned arg combinations.
        """

        if isinstance(instance_or_begin, TimeRange):
            # copy constructor
            self._range = instance_or_begin._range  # pylint: disable=protected-access
        elif isinstance(instance_or_begin, list) \
                or isinstance(instance_or_begin, tuple) \
                or is_pvector(instance_or_begin):
            # a list, vector or tuple - check input first
            self._range = self.sanitize_list_input(instance_or_begin)
        else:
            # two args epoch ms or datetime
            if isinstance(instance_or_begin, int) and \
                    isinstance(end, int):
                self._range = freeze([dt_from_ms(instance_or_begin), dt_from_ms(end)])
            elif isinstance(instance_or_begin, datetime.datetime) and \
                    isinstance(end, datetime.datetime):
                self._range = freeze([instance_or_begin, end])
            else:
                msg = 'both args must be datetime objects or int ms since epoch'
                raise TimeRangeException(msg)

        # Make sure that end is not earlier in time etc
        self.validate_range(self._range)

    def range(self):
        """
        Returns the internal range, which is an Immutable List containing
        begin and end keys
        """
        raise NotImplementedError

    def to_json(self):
        """
        Returns the TimeRange as JSON, which will be a Javascript array
        of two ms timestamps.
        """
        raise NotImplementedError

    def to_string(self):
        """Returns the TimeRange as a string, useful for serialization."""
        raise NotImplementedError

    def to_local_string(self):
        """Returns the TimeRange as a string expressed in local time."""
        raise NotImplementedError

    def to_utc_string(self):
        """Returns the TimeRange as a string expressed in UTC time."""
        raise NotImplementedError

    def humanize(self):
        """
        Returns a human friendly version of the TimeRange, e.g.
        "Aug 1, 2014 05:19:59 am to Aug 1, 2014 07:41:06 am"
        """
        raise NotImplementedError

    def relative_string(self):
        """
        Returns a human friendly version of the TimeRange, e.g.
        e.g. "a few seconds ago to a month ago"
        """
        raise NotImplementedError

    def begin(self):
        """Returns the begin time of the TimeRange."""
        raise NotImplementedError

    def end(self):
        """Returns the end time of the TimeRange."""
        raise NotImplementedError

    def set_begin(self, dtime):
        """
        Sets a new begin time on the TimeRange. The result will be
        a new TimeRange.
        """
        raise NotImplementedError

    def set_end(self, dtime):
        """
        Sets a new end time on the TimeRange. The result will be a new TimeRange.
        """
        raise NotImplementedError

    def equals(self, other):
        """
        Returns if the two TimeRanges can be considered equal,
        in that they have the same times.
        """
        raise NotImplementedError

    def contains(self, other):
        """Returns true if other is completely inside this."""
        raise NotImplementedError

    def within(self, other):
        """
        Returns true if this TimeRange is completely within the supplied
        other TimeRange.
        """
        raise NotImplementedError

    def overlaps(self, other):
        """Returns true if the passed in other TimeRange overlaps this time Range."""
        raise NotImplementedError

    def disjoint(self, other):
        """
        Returns true if the passed in other Range in no way
        overlaps this time Range.
        """
        raise NotImplementedError

    def extents(self, other):
        """
        Returns a new Timerange which covers the extents of this and
        other combined.
        """
        raise NotImplementedError

    def intersection(self, other):
        """
        Returns a new TimeRange which represents the intersection
        (overlapping) part of this and other.
        """
        raise NotImplementedError

    def duration(self):
        """Return epoch milliseconds."""
        raise NotImplementedError

    def humanize_duration(self):
        """Humanize the duration."""
        raise NotImplementedError

    # Static class methods to create canned TimeRanges

    """
    These methods are currently bugged in the JS. Should be like:

        const endTime = moment();
        const beginTime = beginTime.clone().subtract(24, "hours");
        return new TimeRange(beginTime, endTime);
    """

    @staticmethod
    def last_day():
        """time range spanning last 24 hours"""
        raise NotImplementedError

    @staticmethod
    def last_seven_days():
        """time range spanning last 7 days"""
        raise NotImplementedError

    @staticmethod
    def last_thirty_days():
        """time range spanning last 30 days"""
        raise NotImplementedError

    @staticmethod
    def last_ninety_days():
        """time range spanning last 90 days"""
        raise NotImplementedError
