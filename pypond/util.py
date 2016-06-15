#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Various utilities for the pypond code.  Primarily functions to take
care of consistent handling and conversion of time values as we are
trying to traffic in aware datetime objects in UTC time.

Additionally some boolean test functions and assorted other utility functions.
"""

import datetime
import json
import math
import types
import uuid
import warnings

import humanize
import pytz
import tzlocal

from pyrsistent import PMap, PVector

from pypond.exceptions import UtilityException, UtilityWarning

# datetime conversion and utils

EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)
LOCAL_TZ = tzlocal.get_localzone()
HUMAN_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'


def dt_is_aware(dtime):
    """see if a datetime object is aware

    Parameters
    ----------
    dtime : datetime.datetime
        A datetime object

    Returns
    -------
    bool
        Returns True if the dtime is aware/non-naive.
    """
    if dtime.tzinfo is not None and dtime.tzinfo.utcoffset(dtime) is not None:
        return True

    return False


def aware_utcnow():
    """return an aware utcnow() datetime rounded to milliseconds.

    Returns
    -------
    datetime.datetime
        New datetime object
    """
    return to_milliseconds(datetime.datetime.utcnow().replace(tzinfo=pytz.UTC))


def dt_from_ms(msec):
    """generate a datetime object from epoch milliseconds

    Parameters
    ----------
    msec : int
        epoch milliseconds

    Returns
    -------
    datetime.datetime
        New datetime object from ms
    """
    return EPOCH + datetime.timedelta(milliseconds=msec)


def localtime_from_ms(msec):
    """generate an aware localtime datetime object from ms

    Parameters
    ----------
    msec : int
        epoch milliseconds

    Returns
    -------
    datetime.datetime
        New datetime object
    """
    return datetime.datetime.fromtimestamp(msec / 1000.0, LOCAL_TZ)


def aware_dt_from_args(dtargs, localize=False):
    """
    generate an aware datetime object using datetime.datetime kwargs.

    can generate a localized version as well, but please don't.

    Parameters
    ----------
    dtargs : dict
        Dict containing the args you pass to datetime.datetime.
    localize : bool, optional
        Will create a new object in localtime, but just don't do it.

    Returns
    -------
    datetime.datetime
        New datetime object

    Raises
    ------
    UtilityException
        Raised if the args are wrong type.
    """

    if not isinstance(dtargs, dict):
        raise UtilityException('dtargs must be a dict')

    if localize:
        return LOCAL_TZ.localize(datetime.datetime(**dtargs))
    else:
        return datetime.datetime(**dtargs).replace(tzinfo=pytz.UTC)


# The awareness check on these functions is a dev bulletproofing maneuver.
# Introduction of naive datetime objects should be stopped by a check
# in an individual class (ie: Event, etc) and the class specific
# exception should be raised. This is to prevent that being overlooked.

def to_milliseconds(dtime):
    """Check to see if a datetime object has granularity smaller
    than millisecond (ie: microseconds) and massage back to ms if so.

    Doing this round-trip seems kludgy and inefficient, but doing this:

    return dtime.replace(millisecond=round(dt.millisecond, -3))

    produced inconsistent results because of the rounding and I'm not
    going to start treating numbers like strings.

    Parameters
    ----------
    dtime : datetime.datetime
        A datetime object.

    Returns
    -------
    datetime.datetime
        New datetime object rounded down to milliseconds from microseconds.
    """
    if dtime.microsecond % 1000 != 0:
        msec = ms_from_dt(dtime)
        return dt_from_ms(msec)
    else:
        return dtime


def _check_dt(dtime, utc_check=True):
    """
    Make sure that the datetime objects passed to the utility functions
    are aware and UTC. Allow disabling the UTC check for the sanitize_dt
    function.

    Parameters
    ----------
    dtime : datetime.datetime
        A datetime object
    utc_check : bool, optional
        Can be set to False to not do the utc_check

    Raises
    ------
    UtilityException
        Raised if dtime fails check.
    """
    if not dt_is_aware(dtime):
        msg = 'Received a naive datetime object - check class input.'
        raise UtilityException(msg)

    if utc_check:
        if dtime.tzinfo is not pytz.UTC:
            msg = 'Got non utc tz {t} - use pypond.util.sanitize_dt()'.format(t=dtime.tzinfo)
            raise UtilityException(msg)


def ms_from_dt(dtime):
    """Turn a datetime object into ms since epoch.

    Parameters
    ----------
    dtime : datetime.datetime
        A datetime object

    Returns
    -------
    int
        epoch milliseconds
    """
    _check_dt(dtime)

    # diff = dtime - EPOCH
    # millis = diff.days * 24 * 60 * 60 * 1000
    # millis += diff.seconds * 1000
    # millis += diff.microseconds / 1000

    return int((dtime - EPOCH).total_seconds() * 1000)


def sanitize_dt(dtime, testing=False):
    """
    Make sure the datetime object is in UTC/etc. Also round incoming
    datetime objects to milliseconds.

    Allow disabling warnings when testing. Warning primarily exists
    to herd users into not passing in non-UTC tz datetime objects.

    Parameters
    ----------
    dtime : datetime.datetime
        A datetime object
    testing : bool, optional
        Suppress warnings when testing.

    Returns
    -------
    datetime.datetime
        New datetime object rounded to ms from microseconds.
    """
    _check_dt(dtime, utc_check=False)

    if dtime.tzinfo is not pytz.UTC:
        if not testing:
            msg = 'Got datetime with non utc tz {t}'.format(t=dtime.tzinfo)
            msg += ' - coercing to UTC {dt}'.format(dt=dtime.astimezone(pytz.UTC))
            msg += ' - consider using datetime with UTC or ms since epoch instead'
            warnings.warn(msg, UtilityWarning, stacklevel=2)
        return to_milliseconds(dtime.astimezone(pytz.UTC))
    else:
        # create new object just to do it.
        return to_milliseconds(dtime + datetime.timedelta(seconds=0))


def monthdelta(date, delta):
    """because we wish datetime.timedelta had a month kwarg.

    Courtesy of: http://stackoverflow.com/a/3425124/3916180

    Parameters
    ----------
    date : datetime.date
        Date object
    delta : int
        Month delta

    Returns
    -------
    datetime.date
        New Date object with delta offset.
    """
    month, year = (date.month + delta) % 12, date.year + ((date.month) + delta - 1) // 12
    if not month:
        month = 12
    day = min(date.day, [31, 29 if year % 4 == 0 and not year % 400 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])  # pylint: disable=line-too-long
    return date.replace(day=day, month=month, year=year)


def format_dt(dtime, localize=False):
    """Format for human readable output.

    Parameters
    ----------
    dtime : datetime.datetime
        A datetime object
    localize : bool, optional
        Display as local time.

    Returns
    -------
    str
        Formatted date string.
    """
    _check_dt(dtime)

    # base_format = '%c %Z %z'
    base_format = HUMAN_FORMAT

    if not localize:
        return dtime.strftime(base_format)
    else:
        return dtime.astimezone(LOCAL_TZ).strftime(base_format)


def humanize_dt(dtime):
    """format time format display for humanize maneuvers.

    Parameters
    ----------
    dtime : datetime.datetime
        A datetime object

    Returns
    -------
    str
        Datetime formatted as a string.
    """
    return dtime.astimezone(LOCAL_TZ).strftime(HUMAN_FORMAT)


def humanize_dt_ago(dtime):
    """format to "23 minutes ago" style format.

    Parameters
    ----------
    dtime : datetime.datetime
        A datetime object

    Returns
    -------
    str
        Humanized string.
    """
    # and here we went through all the trouble to make everything
    # UTC and offset-aware. Le sigh. The underlying lib uses datetime.now()
    # as the comparison reference, so we need naive localtime.
    return humanize.naturaltime(dtime.astimezone(LOCAL_TZ).replace(tzinfo=None))


def humanize_duration(delta):
    """format for a single duration value - takes datatime.timedelta as arg

    Parameters
    ----------
    delta : datetime.timedelta
        A time delta

    Returns
    -------
    str
        Humanize delta to duration.
    """
    return humanize.naturaldelta(delta)

# various utility functions


def unique_id(prefix=''):
    """generate a uuid with a prefix - for debugging

    Parameters
    ----------
    prefix : str, optional
        Prefix for uuid.

    Returns
    -------
    str
        Prefixed uuid.
    """
    return prefix + str(uuid.uuid4())


class ObjectEncoder(json.JSONEncoder):
    """
    Class to allow arbitrary python objects to be json encoded with
    json.dumps()/etc by defining a .to_json() method on your object.

    We need this for encoding lists of custom Event (etc) objects.

    Usage: json.dumps(your_cool_object, cls=ObjectEncoder)
    """

    def default(self, obj):  # pylint: disable=method-hidden
        if hasattr(obj, "to_json"):
            return self.default(obj.to_json())

        return obj

# test types


def is_pmap(pmap):
    """Check this here so people don't mistake pmap and PMap.

    Parameters
    ----------
    pmap : obj
        An object

    Returns
    -------
    bool
        Returns True if it is a pyrsistent.pmap
    """
    return isinstance(pmap, PMap)


def is_pvector(pvector):
    """Check this here so people don't mistake PVector and pvector.

    Parameters
    ----------
    pvector : obj
        An object

    Returns
    -------
    bool
        Returns True if it is a pyrsistent.pvector
    """
    return isinstance(pvector, PVector)


def is_nan(val):
    """Test if a value is NaN

    Parameters
    ----------
    val : obj
        A value

    Returns
    -------
    bool
        Is it NaN?
    """
    try:
        float(val)
        if math.isnan(float(val)):
            return True
    except (ValueError, TypeError):
        pass

    return False


def is_function(func):
    """Test if a value is a function.

    Parameters
    ----------
    func : obj
        A value

    Returns
    -------
    bool
        Is the object a python function?
    """
    return isinstance(func, types.FunctionType)
