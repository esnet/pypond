#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Implementation of Pond TimeRange classes.

http://software.es.net/pond/#/timerange
"""
import datetime
import json

from pyrsistent import freeze

from .bases import PypondBase
from .exceptions import TimeRangeException, NAIVE_MESSAGE
from .util import (
    aware_utcnow,
    dt_from_ms,
    dt_is_aware,
    format_dt,
    humanize_dt,
    humanize_dt_ago,
    humanize_duration,
    is_pvector,
    monthdelta,
    ms_from_dt,
    sanitize_dt,
)


class TimeRangeBase(PypondBase):
    """Base for TimeRange"""

    @staticmethod
    def awareness_check(dtime):
        """
        Check input to make sure datetimes are aware. Presumes an interable
        contaning datetimes, but will fail over via duck typing.

        :param dtime: Iterable containing datetime.datetime objects. Will also
            handle a single datetime.datetime object as well in case.
        :type dtime: list
        :raises: TimeRangeException
        """
        try:
            for i in dtime:
                if not dt_is_aware(i):
                    raise TimeRangeException(NAIVE_MESSAGE)
        except TypeError:
            if not dt_is_aware(dtime):
                raise TimeRangeException(NAIVE_MESSAGE)

    @staticmethod
    def sanitize_list_input(list_type):
        """
        Validate input when a pvector, list or tuple is passed in
        as a constructor arg.

        :param list_type: A list, tuple or pvector passed to the constructor.
        :type list_type: list, tuple or pvector
        :returns: pyrsistent.pvector -- containing the start and end time.
        :raises: TimeRangeException
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

        # datetimes - check and sanitize
        if isinstance(list_type[0], datetime.datetime):
            TimeRangeBase.awareness_check(list_type)
            return freeze([sanitize_dt(list_type[0]), sanitize_dt(list_type[1])])
        else:
            # we must have ints then - convert
            return freeze([dt_from_ms(list_type[0]), dt_from_ms(list_type[1])])

    @staticmethod
    def validate_range(range_obj):
        """
        Make sure that the end time is not chronologically before the begin.

        :raises: TimeRangeException
        """
        if range_obj[0] > range_obj[1]:
            msg = 'Invalid range - end {e} is earlier in time than begin {b}'.format(
                e=range_obj[1], b=range_obj[0])
            raise TimeRangeException(msg)


class TimeRange(TimeRangeBase):  # pylint: disable=too-many-public-methods
    """
    Builds a new TimeRange. First arg may be of several different formats:

    - Another TimeRange (copy constructor)
    - A python tuple, list or pyrsistent.PVector object containing two
      python datetime objects or ms timestamps.
    - Two arguments, begin and end, each of which may be a datetime object,
      or a ms timestamp.

    :param instance_or_begin: See above for variations.
    :type instance_or_begin: TimeRange, iterable, int or datetime.datetime.
    :param end: Optional arg for the end of the time range.
    :type end: int or datetime.datetime
    :raises: TimeRangeException
    """
    def __init__(self, instance_or_begin, end=None):
        """
        Construct the object using the aforementioned arg combinations.
        """
        super(TimeRange, self).__init__()

        if isinstance(instance_or_begin, TimeRange):
            # copy constructor
            self._range = instance_or_begin._range  # pylint: disable=protected-access
        elif isinstance(instance_or_begin, list) \
                or isinstance(instance_or_begin, tuple) \
                or is_pvector(instance_or_begin):
            # a list, vector or tuple - check input first
            self._range = self.sanitize_list_input(instance_or_begin)
        else:
            # two args - epoch ms or datetime
            if isinstance(instance_or_begin, int) and \
                    isinstance(end, int):
                self._range = freeze([dt_from_ms(instance_or_begin), dt_from_ms(end)])
            elif isinstance(instance_or_begin, datetime.datetime) and \
                    isinstance(end, datetime.datetime):
                self.awareness_check([instance_or_begin, end])
                self._range = freeze([sanitize_dt(instance_or_begin), sanitize_dt(end)])
            else:
                msg = 'both args must be datetime objects or int ms since epoch'
                raise TimeRangeException(msg)

        # Make sure that end is not earlier in time etc
        self.validate_range(self._range)

    def range(self):
        """
        Returns the internal range, which is an Immutable List containing
        begin and end values.

        :returns: pyrsisten.pvector
        """
        return self._range

    def to_json(self):
        """
        Returns the TimeRange as a python list of two ms timestamps.

        :returns: list
        """
        return [ms_from_dt(self.begin()), ms_from_dt(self.end())]

    def to_string(self):
        """Returns the TimeRange as a string, useful for serialization.

        :returns: str -- string representation of the range.
        """
        return json.dumps(self.to_json())

    def to_local_string(self):
        """Returns the TimeRange as a string expressed in local time.

        :returns: str
        """
        return '[{b}, {e}]'.format(
            b=format_dt(self.begin(), localize=True),
            e=format_dt(self.end(), localize=True))

    def to_utc_string(self):
        """Returns the TimeRange as a string expressed in UTC time.

        :returns: str
        """
        return '[{b}, {e}]'.format(b=format_dt(self.begin()),
                                   e=format_dt(self.end()))

    def humanize(self):
        """
        Returns a human friendly version of the TimeRange, e.g.
        "Aug 1, 2014 05:19:59 am to Aug 1, 2014 07:41:06 am"

        This displays in local time, so don't freak out.

        :returns: str
        """
        return '{b} to {e}'.format(b=humanize_dt(self.begin()), e=humanize_dt(self.end()))

    def relative_string(self):
        """
        Returns a human friendly version of the TimeRange, e.g.
        e.g. "a few seconds ago to a month ago"

        :returns: str
        """
        return '{b} to {e}'.format(b=humanize_dt_ago(self.begin()),
                                   e=humanize_dt_ago(self.end()))

    def begin(self):
        """Returns the begin time of the TimeRange.

        :returns: datetime.datetime
        """
        return self._range[0]

    def end(self):
        """Returns the end time of the TimeRange.

        :returns: datetime.datetime
        """
        return self._range[1]

    def set_begin(self, dtime):
        """
        Sets a new begin time on the TimeRange. The result will be a new TimeRange.

        :param dtime: New time range boundry.
        :type dtime: datetime.datetime
        :returns: TimeRange -- new time range object with new bounds.
        :raises: TimeRangeException
        """
        if not isinstance(dtime, datetime.datetime):
            msg = 'arg must be a datetime object.'
            raise TimeRangeException(msg)

        self.awareness_check(dtime)

        return TimeRange(self._range.set(0, dtime))

    def set_end(self, dtime):
        """
        Sets a new end time on the TimeRange. The result will be a new TimeRange.

        :param dtime: New time range boundry.
        :type dtime: datetime.datetime
        :returns: TimeRange -- new time range object with new bounds.
        :raises: TimeRangeException
        """
        if not isinstance(dtime, datetime.datetime):
            msg = 'arg must be a datetime object.'
            raise TimeRangeException(msg)

        self.awareness_check(dtime)

        return TimeRange(self._range.set(1, dtime))

    def equals(self, other):
        """
        Returns if the two TimeRanges can be considered equal,
        in that they have the same times.

        :param other: Another time range object to compare to this instance.
        :type other: TimeRange
        :returns: bool
        """
        return bool(self.begin() == other.begin() and self.end() == other.end())

    def contains(self, other):
        """Returns true if other is completely inside this."""
        if isinstance(other, datetime.datetime):
            return bool(self.begin() <= other and self.end() >= other)
        elif isinstance(other, TimeRange):
            return bool(self.begin() <= other.begin() and
                        self.end() >= other.end())

        return False

    def within(self, other):
        """
        Returns true if this TimeRange is completely within the supplied
        other TimeRange.

        :param other: Another time range object to compare to this instance.
        :type other: TimeRange
        :returns: bool
        """
        return bool(self.begin() >= other.begin() and self.end() <= other.end())

    def overlaps(self, other):
        """Returns true if the passed in other TimeRange overlaps this time Range.

        :param other: Another time range object to compare to this instance.
        :type other: TimeRange
        :returns: bool
        """
        return bool(
            (self.contains(other.begin()) and not self.contains(other.end())) or
            (self.contains(other.end()) and not self.contains(other.begin()))
        )

    def disjoint(self, other):
        """
        Returns true if the passed in other Range in no way
        overlaps this time Range.

        :param other: Another time range object to compare to this instance.
        :type other: TimeRange
        :returns: bool
        """
        return bool((self.end() < other.begin()) or (self.begin() > other.end()))

    def extents(self, other):
        """
        Returns a new Timerange which covers the extents of this and
        other combined.

        :param other: Another time range object to compare to this instance.
        :type other: TimeRange
        :returns: TimeRange -- new time range instance.
        """
        beg = self.begin() if self.begin() < other.begin() else other.begin()
        end = self.end() if self.end() > other.end() else other.end()

        return TimeRange(beg, end)

    def intersection(self, other):
        """
        Returns a new TimeRange which represents the intersection
        (overlapping) part of this and other.

        :param other: Another time range object to compare to this instance.
        :type other: TimeRange
        :returns: TimeRange -- new time range instance.
        """
        if self.disjoint(other):
            return None

        beg = self.begin() if self.begin() > other.begin() else other.begin()
        end = self.end() if self.end() < other.end() else other.end()

        return TimeRange(beg, end)

    def duration(self):
        """Return epoch milliseconds.

        :returns: int -- Duration in ms.
        """
        return ms_from_dt(self.end()) - ms_from_dt(self.begin())

    def humanize_duration(self):
        """Humanize the duration.

        :returns: str -- Humanized duration string.
        """
        return humanize_duration(self.end() - self.begin())

    def __str__(self):
        """string repr method.

        :returns: str"""
        return self.to_string()

    # Static class methods to create canned TimeRanges

    @staticmethod
    def last_day():
        """Generate a time range spanning last 24 hours

        :returns: TimeRange
        """
        end = aware_utcnow()
        beg = end - datetime.timedelta(hours=24)
        return TimeRange(beg, end)

    @staticmethod
    def last_seven_days():
        """Generate a time range spanning last 7 days

        :returns: TimeRange
        """
        end = aware_utcnow()
        beg = end - datetime.timedelta(days=7)
        return TimeRange(beg, end)

    @staticmethod
    def last_thirty_days():
        """Generate a time range spanning last 30 days

        :returns: TimeRange
        """
        end = aware_utcnow()
        beg = end - datetime.timedelta(days=30)
        return TimeRange(beg, end)

    @staticmethod
    def last_month():
        """Generate a time range spanning last month.

        :returns: TimeRange
        """
        end = aware_utcnow()
        beg = monthdelta(end, -1)
        return TimeRange(beg, end)

    @staticmethod
    def last_ninety_days():
        """Generate a time range spanning last 90 days

        :returns: TimeRange
        """
        end = aware_utcnow()
        beg = end - datetime.timedelta(days=90)
        return TimeRange(beg, end)
