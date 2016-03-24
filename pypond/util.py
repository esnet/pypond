"""
Various utilities for the pypond code
"""

import datetime

from pyrsistent import PMap

# date conversion

EPOCH = datetime.datetime.utcfromtimestamp(0)


def ms_from_dt(dtime):
    """Turn a datetime object into ms since epoch."""
    return int((dtime - EPOCH).total_seconds() * 1000)


def dt_from_ms(msec):
    """generate a datetime object from epoch milliseconds"""
    return EPOCH + datetime.timedelta(milliseconds=msec)


def dt_from_dt(dtime):
    """generate a new datetime object from an existing one"""
    return dtime + datetime.timedelta(seconds=0)

# test types


def is_pmap(pmap):
    """Check this here so people don't mistake pmap and PMap."""
    return isinstance(pmap, PMap)
