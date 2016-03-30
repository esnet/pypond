"""
Various utilities for the pypond code.
"""

import datetime
import math
import types
import warnings

import humanize
import pytz
import tzlocal

from pyrsistent import PMap, PVector

from pypond.exceptions import UtilityException, UtilityWarning

# datetime conversion and utils

EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)
LOCAL_TZ = tzlocal.get_localzone()
HUMAN_FORMAT = '%b %-d, %Y %I:%M:%S %p'


def dt_is_aware(dtime):
    """see if a datetime object is aware"""
    if dtime.tzinfo is not None and dtime.tzinfo.utcoffset(dtime) is not None:
        return True

    return False


def aware_utcnow():
    """return an aware utcnow() datetime."""
    return datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)


def dt_from_ms(msec):
    """generate a datetime object from epoch milliseconds"""
    return EPOCH + datetime.timedelta(milliseconds=msec)


# The awareness check on these functions is a dev bulletproofing maneuver.
# Introduction of naive datetime objects should be stopped by a check
# in an individual class (ie: Event, etc) and the class specific
# exception should be raised. This is to prevent that being overlooked.

def _check_dt(dtime, utc_check=True):
    """
    Make sure that the datetime objects passed to the utility functions
    are aware and UTC. Allow disabling the UTC check for the sanitize_dt
    function.
    """
    if not dt_is_aware(dtime):
        msg = 'Received a naive datetime object - check class input.'
        raise UtilityException(msg)

    if utc_check:
        if dtime.tzinfo is not pytz.UTC:
            msg = 'Got non utc tz {t} - use pypond.util.sanitize_dt()'.format(t=dtime.tzinfo)
            raise UtilityException(msg)


def ms_from_dt(dtime):
    """Turn a datetime object into ms since epoch."""
    _check_dt(dtime)

    return int((dtime - EPOCH).total_seconds() * 1000)


def sanitize_dt(dtime, testing=False):
    """
    Make sure the datetime object is in UTC/etc.

    Allow disabling warnings when testing. Warning primarily exists
    to herd users into not passing in non-UTC tz datetime objects.
    """
    _check_dt(dtime, utc_check=False)

    if dtime.tzinfo is not pytz.UTC:
        if not testing:
            msg = 'Got datetime with non utc tz {t}'.format(t=dtime.tzinfo)
            msg += ' - coercing to UTC {dt}'.format(dt=dtime.astimezone(pytz.UTC))
            msg += ' - consider using datetime with UTC or ms since epoch instead'
            warnings.warn(msg, UtilityWarning, stacklevel=2)
        return dtime.astimezone(pytz.UTC)
    else:
        # create new object just to do it.
        return dtime + datetime.timedelta(seconds=0)


def format_dt(dtime, localize=False):
    """Format for human readable output."""
    _check_dt(dtime)

    base_format = '%c %Z %z'

    if not localize:
        return dtime.strftime(base_format)
    else:
        return dtime.astimezone(LOCAL_TZ).strftime(base_format)


def humanize_dt(dtime):
    """format time format display for humanize maneuvers."""
    return dtime.astimezone(LOCAL_TZ).strftime(HUMAN_FORMAT)


def humanize_dt_ago(dtime):
    """format to "23 minutes ago" style format."""
    # and here we went through all the trouble to make everything
    # UTC and offset-aware. Le sigh. The underlying lib uses datetime.now()
    # as the comparison reference, so we need naive localtime.
    return humanize.naturaltime(dtime.astimezone(LOCAL_TZ).replace(tzinfo=None))


def humanize_duration(delta):
    """format for a single duration value - takes datatime.timedelta as arg"""
    return humanize.naturaldelta(delta)

# test types


def is_pmap(pmap):
    """Check this here so people don't mistake pmap and PMap."""
    return isinstance(pmap, PMap)


def is_pvector(pvector):
    """Check this here so people don't mistake PVector and pvector."""
    return isinstance(pvector, PVector)


def is_nan(val):
    """Test if a value is NaN"""
    try:
        float(val)
        if math.isnan(float(val)):
            return True
    except (ValueError, TypeError):
        pass

    return False


def is_function(func):
    """Test if a value is a function."""
    return isinstance(func, types.FunctionType)
