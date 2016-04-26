"""
Implementation of Pond Index class.

http://software.es.net/pond/#/index
"""

import re

from pypond.exceptions import IndexException
from pypond.range import TimeRange
from pypond.util import dt_from_ms, localtime_from_ms


class IndexBase(object):
    """Base / static util methods for Index class."""

    units = dict(
        s=dict(label='seconds', length=1),
        m=dict(label='minutes', length=60),
        h=dict(label='hours', length=3600),
        d=dict(label='days', length=86400),
    )

    @staticmethod
    def range_from_index_string(s, is_utc=True):  # pylint: disable=invalid-name
        """
        Generate the time range from the idx string.
        """
        parts = s.split('-')
        num_parts = len(parts)

        begin_time = None
        end_time = None

        if num_parts == 3:
            pass
        elif num_parts == 2:

            range_re = re.match('([0-9]+)([smhd])', s)

            if range_re:

                try:
                    pos = int(parts[1])
                    num = int(range_re.group(1))
                except ValueError:
                    msg = 'unable to parse valid integers from {s}'.format(s=s)
                    msg += 'tried elements {pos} and {num}'.format(
                        pos=parts[1], num=range_re.group(1))
                    raise IndexException(msg)

                unit = range_re.group(2)
                length = num * IndexBase.units[unit].get('length') * 1000

                begin_time = dt_from_ms(pos * length) if is_utc else \
                    localtime_from_ms(pos * length)

                end_time = dt_from_ms((pos + 1) * length) if is_utc else \
                    localtime_from_ms((pos + 1) * length)

            else:
                print 'month'

        elif num_parts == 1:
            pass

        if begin_time and end_time:
            return TimeRange(begin_time, end_time)
        else:
            return None


class Index(IndexBase):
    """
    An index that represents as a string a range of time. That range may either
    be in UTC or local time. UTC is the default.

    The actual derived timerange can be found using asRange(). This will return
    a TimeRange instance.

    The original string representation can be found with toString(). A nice
    version for date based indexes (e.g. 2015-03) can be generated with
    toNiceString(format) (e.g. March, 2015).
    """

    def __init__(self, s, utc=True):
        """Create the Index."""

        self._utc = utc
        self._string = s
        self._timerange = self.range_from_index_string(self._string, self._utc)

    def to_json(self):
        """
        Returns the Index as JSON, which will just be its string
        representation

        This is actually like json.loads(s) - produces the
        actual data structure."""
        raise NotImplementedError

    def to_string(self):
        """
        Simply returns the Index as its string

        In JS land, this is synonymous with __str__ or __unicode__
        """
        raise NotImplementedError

    def to_nice_string(self, fmt):
        """
        for the calendar range style Indexes, this lets you return
        that calendar range as a human readable format, e.g. "June, 2014".
        The format specified is a Moment.format.
        """
        raise NotImplementedError

    def as_string(self):
        """Alias for to_string()"""
        raise NotImplementedError

    def as_timerange(self):
        """Returns the Index as a TimeRange"""
        return self._timerange

    def begin(self):
        """Returns start date of the index."""
        raise NotImplementedError

    def end(self):
        """Returns end data of the index."""
        raise NotImplementedError

    def __str__(self):
        """call to_string()"""
        raise NotImplementedError

    # Static class methods

    @staticmethod
    def get_index_string(win, date):
        """ TBA """
        raise NotImplementedError

    @staticmethod
    def get_bucket(win, date, key):
        """ TBA """
        raise NotImplementedError

    @staticmethod
    def get_index_string_list(win, timerange):
        """ TBA """
        raise NotImplementedError

    @staticmethod
    def get_bucket_list(win, timerange, key):
        """ TBA """
        raise NotImplementedError
