"""
Various utilities for the pypond code
"""

import datetime

from pyrsistent import PMap

# date conversion


def dt_from_ms(msec):
    """generate a datetime object from epoch milliseconds"""
    return datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=msec)


def dt_from_dt(dtime):
    """generate a new datetime object from an existing one"""
    return dtime + datetime.timedelta(seconds=0)


def dt_from_d(date):
    """Generate a datetime from a date object"""
    return datetime.datetime(year=date.year, month=date.month, day=date.day)

# test types


def is_pmap(pmap):
    """Check this here so people don't mistake pmap and PMap."""
    return isinstance(pmap, PMap)
