"""
Various utilities for the pypond code.
"""

import datetime
import warnings

import pytz

from pyrsistent import PMap

from pypond.exceptions import UtilityException, UtilityWarning

# datetime conversion and utils

EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)


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


def dt_from_dt(dtime):
    """generate a new datetime object from an existing one"""
    _check_dt(dtime)

    return dtime + datetime.timedelta(seconds=0)


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
        return dtime

# test types


def is_pmap(pmap):
    """Check this here so people don't mistake pmap and PMap."""
    return isinstance(pmap, PMap)
