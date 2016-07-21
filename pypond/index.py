#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Implementation of Pond Index class.

http://software.es.net/pond/#/index
"""

import copy
import datetime
import re

from .bases import PypondBase
from .exceptions import IndexException, IndexWarning
from .range import TimeRange
from .util import (
    aware_dt_from_args,
    dt_from_ms,
    localtime_from_ms,
    localtime_info_from_utc,
    monthdelta,
    ms_from_dt,
    sanitize_dt,
)


UNITS = dict(
    s=dict(label='seconds', length=1),
    m=dict(label='minutes', length=60),
    h=dict(label='hours', length=3600),
    d=dict(label='days', length=86400),
)


class Index(PypondBase):
    """
    An index that represents as a string a range of time. That range may either
    be in UTC or local time. UTC is the default.

    The actual derived timerange can be found using asRange(). This will return
    a TimeRange instance.

    The original string representation can be found with toString(). A nice
    version for date based indexes (e.g. 2015-03) can be generated with
    toNiceString(format) (e.g. March, 2015).

    The index string arg will may be of two forms:

    - 2015-07-14  (day)
    - 2015-07     (month)
    - 2015        (year)


    or:

    - 1d-278      (range, in n x days, hours, minutes or seconds)

    Parameters
    ----------
    s : str
        The index string in one of the aforementioned formats.
    utc : bool, optional
        Index interpreted as UTC or localtime. Please don't set this to false
        since non-UTC times are the devil.

    Raises
    ------
    IndexException
        Raised if arg s could not be translated into a valid timerange/index.
    """

    def __init__(self, s, utc=True):
        """Create the Index.
        """
        super(Index, self).__init__()

        self._utc = utc
        self._string = s
        # keep track of what kind of index it is to simplify other things.
        self._index_type = None

        self._timerange = self.range_from_index_string(self._string, self._utc)

        if self._index_type is None:
            raise IndexException('could not determine timerange/index type from {arg}'.format(
                arg=s))

    def to_json(self):
        """
        Returns the Index as JSON, which will just be its string
        representation

        This is actually like json.loads(s) - produces the
        actual data structure.

        Returns
        -------
        str
            The index string as previously outlined.
        """
        return self._string

    def to_string(self):
        """
        Simply returns the Index as its string

        In JS land, this is synonymous with __str__ or __unicode__

        Returns
        -------
        str
            The index string as previously outlined.
        """
        return self._string

    def to_nice_string(self, fmt=None):
        """
        for the calendar range style Indexes, this lets you return
        that calendar range as a human readable format, e.g. "June, 2014".
        The format specified is a Moment.format.

        Originally implemented at Util.niceIndexString in the JS source,
        this is just a greatly simplified version using self._index_type.

        Parameters
        ----------
        fmt : str, optional
            User can pass in a valid strftime() format string.

        Returns
        -------
        str
            FThe index text string as a formatted (strftime()) time.
        """

        if fmt is not None and self._index_type in ('day', 'month', 'year'):
            return self.begin().strftime(fmt)

        if self._index_type == 'day':
            return self.begin().strftime('%B %-d %Y')
        elif self._index_type == 'index':
            return self._string
        elif self._index_type == 'month':
            return self.begin().strftime('%B')
        elif self._index_type == 'year':
            return self.begin().strftime('%Y')

    def as_string(self):
        """Alias for to_string()

        Returns
        -------
        str
            The index string as previously outlined.
        """
        return self.to_string()

    def as_timerange(self):
        """Returns the Index as a TimeRange

        Returns
        -------
        TimeRange
            The underlying time range object.
        """
        return self._timerange

    def begin(self):
        """Returns start date of the index.

        Returns
        -------
        datetime.datetime
            Start date of the index.
        """
        return self.as_timerange().begin()

    def end(self):
        """Returns end date of the index.

        Returns
        -------
        datetime.datetime
            End date of the index.
        """
        return self.as_timerange().end()

    def __str__(self):
        """call to_string()

        Returns
        -------
        str
            String representation of the object.
        """
        return self.to_string()

    @property
    def utc(self):
        """accessor for internal utc boolean."""
        return self._utc

    # utility methods

    def _local_idx_warning(self, local=False):
        """blanket warning to avoid if statements and make pylint happy."""
        if local:
            msg = 'year/month/day indexes will being coerced to UTC from localtime'
            self._warn(msg, IndexWarning)

    def range_from_index_string(self, idx_str, is_utc=True):  # pylint: disable=too-many-locals, too-many-statements
        """
        Generate the time range from the idx string.

        The index string arg will may be of two forms:

        - 2015-07-14  (day)
        - 2015-07     (month)
        - 2015        (year)

        or:

        - 1d-278      (range, in n x days, hours, minutes or seconds)

        and return a TimeRange for that time. The TimeRange may be considered to be
        local time or UTC time, depending on the utc flag passed in.

        This was in src/util.js in the original project, but the only thing using
        the code in that util.js was the Index class, and it makes more sense
        having this as a class method and setting self._index_type makes further
        regex analysis of the index unnecessary.

        Parameters
        ----------
        idx_str : str
            The index string in one of the aformentioned formats
        is_utc : bool, optional
            Index interpreted as utc or localtime. Please don't use localtime.

        Returns
        -------
        TimeRange
            A time range made from the interpreted index string.

        Raises
        ------
        IndexException
            Raised when the string format is determined to be invalid.
        """
        parts = idx_str.split('-')
        num_parts = len(parts)

        begin_time = None
        end_time = None

        local = False if is_utc else True

        if num_parts == 3:
            # 2015-07-14  (day)
            self._index_type = 'day'
            try:
                year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])
            except ValueError:
                msg = 'unable to parse integer year/month/day from {arg}'.format(arg=parts)
                raise IndexException(msg)

            self._local_idx_warning(local)

            dtargs = dict(year=year, month=month, day=day)

            begin_time = aware_dt_from_args(dtargs, localize=local)

            end_time = (begin_time + datetime.timedelta(days=1)) - datetime.timedelta(seconds=1)

        elif num_parts == 2:

            range_re = re.match('([0-9]+)([smhd])', idx_str)

            if range_re:
                # 1d-278      (range, in n x days, hours, minutes or seconds)
                self._index_type = 'index'

                try:
                    pos = int(parts[1])  # 1d-278 : 278
                    num = int(range_re.group(1))  # 1d-278 : 1
                except ValueError:
                    msg = 'unable to parse valid integers from {s}'.format(s=idx_str)
                    msg += ' tried elements {pos} and {num}'.format(
                        pos=parts[1], num=range_re.group(1))
                    raise IndexException(msg)

                unit = range_re.group(2)  # 1d-278 : d
                # num day/hr/etc units * seconds in that unit * 1000
                length = num * UNITS[unit].get('length') * 1000

                # pos * length = ms since epoch
                begin_time = dt_from_ms(pos * length) if is_utc else \
                    localtime_from_ms(pos * length)

                # (pos + 1) * length is one hour/day/minute/etc later
                end_time = dt_from_ms((pos + 1) * length) if is_utc else \
                    localtime_from_ms((pos + 1) * length)

            else:
                # 2015-07     (month)
                self._index_type = 'month'
                try:
                    year = int(parts[0])
                    month = int(parts[1])
                except ValueError:
                    msg = 'unable to parse integer year/month from {arg}'.format(arg=parts)
                    raise IndexException(msg)

                self._local_idx_warning(local)

                dtargs = dict(year=year, month=month, day=1)

                begin_time = aware_dt_from_args(dtargs, localize=local)

                end_time = monthdelta(begin_time, 1) - datetime.timedelta(seconds=1)

        elif num_parts == 1:
            # 2015        (year)
            self._index_type = 'year'
            try:
                year = int(parts[0])
            except ValueError:
                msg = 'unable to parse integer year from {arg}'.format(arg=parts[0])
                raise IndexException(msg)

            self._local_idx_warning(local)

            dtargs = dict(year=year, month=1, day=1)

            begin_time = aware_dt_from_args(dtargs, localize=local)

            end_time = begin_time.replace(year=year + 1) - datetime.timedelta(seconds=1)

        if begin_time and end_time:
            return TimeRange(begin_time, end_time)
        else:
            return None

    # Static class methods
    # The two window_* methods were in util.js in the pond source but
    # they were only being called from this code, so here they are.

    @staticmethod
    def window_duration(win):
        """duration in ms given a window duration string.

        previously: Generator.getLengthFromSize.

        Parameters
        ----------
        win : str
            An index string in the previously mentioned 1d-278 style format.

        Returns
        -------
        int
            Duration of the index/range in ms.
        """
        range_re = re.match('([0-9]+)([smhd])', win)

        if range_re:
            # normally would try/except, but the regex ensures it'll be a number
            num = int(range_re.group(1))

            unit = range_re.group(2)

            return num * UNITS[unit].get('length') * 1000

        else:
            return None

    @staticmethod
    def window_position_from_date(win, dtime):
        """window position from datetime object. Called by get_index_string_list().

        previously: Generator.getBucketPosFromDate

        Parameters
        ----------
        win : str
            Prefix if the index string.
        dtime : datetime.datetime
            Datetime to calculate suffix from.

        Returns
        -------
        int
            The suffix for the index string.
        """
        duration = Index.window_duration(win)
        ddms = ms_from_dt(sanitize_dt(dtime))
        return int(ddms / duration)

    @staticmethod
    def get_index_string(win, dtime):
        """Return the index string given an index prefix and a datetime
        object. Example usage follows.

        ::

            dtime = aware_dt_from_args(
                dict(year=2015, month=3, day=14, hour=7, minute=32, second=22))

            idx_str = Index.get_index_string('5m', dtime)

            self.assertEquals(idx_str, '5m-4754394')

        previously: Generator.bucketIndex

        Parameters
        ----------
        win : str
            Prefix of the index string.
        dtime : datetime.datetime
            Datetime to generate index string from.

        Returns
        -------
        str
            The index string.
        """
        pos = Index.window_position_from_date(win, dtime)
        return '{win}-{pos}'.format(win=win, pos=pos)

    @staticmethod
    def get_index_string_list(win, timerange):
        """Given the time range, return a list of strings of index values
        every <prefix> tick. Example usage follows (from test suite).

        ::

            dtime_1 = aware_dt_from_args(
            dict(year=2015, month=3, day=14, hour=7, minute=30, second=0))

            dtime_2 = aware_dt_from_args(
                dict(year=2015, month=3, day=14, hour=8, minute=29, second=59))

            idx_list = Index.get_index_string_list('5m', TimeRange(dtime_1, dtime_2))

            self.assertEquals(len(idx_list), 12)
            self.assertEquals(idx_list[0], '5m-4754394')
            self.assertEquals(idx_list[-1], '5m-4754405')

        previously: Generator.bucketIndexList

        Parameters
        ----------
        win : str
            Prefix of the index string.
        timerange : TimeRange
            Time range object to generate index string from

        Returns
        -------
        list
            A list of strings of index values at every "tick" in the range
            specified.
        """
        pos1 = Index.window_position_from_date(win, timerange.begin())
        pos2 = Index.window_position_from_date(win, timerange.end())

        idx_list = list()

        if pos1 <= pos2:
            pos = copy.copy(pos1)
            while pos <= pos2:
                idx_list.append('{win}-{pos}'.format(win=win, pos=pos))
                pos += 1

        return idx_list

    @staticmethod
    def get_daily_index_string(date, utc=True):
        """Generate an index string with day granularity.

        Parameters
        ----------
        date : datetime.datetime
            An aware UTC datetime object
        utc : bool, optional
            Render the index in local time this is used for display purposes
            to render charts in a localized way.

        Returns
        -------
        string
            The formatted index string.
        """
        year = date.year if utc else localtime_info_from_utc(date).get('year')
        month = date.strftime('%m') if utc else localtime_info_from_utc(date).get('month')
        day = date.strftime('%d') if utc else localtime_info_from_utc(date).get('day')
        return '{y}-{m}-{d}'.format(y=year, m=month, d=day)

    @staticmethod
    def get_monthly_index_string(date, utc=True):
        """Generate an index string with month granularity.

        Parameters
        ----------
        date : datetime.datetime
            An aware UTC datetime object
        utc : bool, optional
            Render the index in local time this is used for display purposes
            to render charts in a localized way.

        Returns
        -------
        string
            The formatted index string.
        """
        year = date.year if utc else localtime_info_from_utc(date).get('year')
        month = date.strftime('%m') if utc else localtime_info_from_utc(date).get('month')
        return '{y}-{m}'.format(y=year, m=month)

    @staticmethod
    def get_yearly_index_string(date, utc=True):
        """Generate an index string with year granularity.

        Parameters
        ----------
        date : datetime.datetime
            An aware UTC datetime object
        utc : bool, optional
            Render the index in local time this is used for display purposes
            to render charts in a localized way.

        Returns
        -------
        string
            The formatted index string.
        """
        year = date.year if utc else localtime_info_from_utc(date).get('year')
        return '{y}'.format(y=year)
