#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Functions to act as reducers/aggregators, etc.
"""

import inspect

from functools import reduce
from math import sqrt

from .exceptions import FilterException


class Filters(object):
    """Filter functions to pass to aggregation function factory
    methods.

    These all control how the underlying aggregators handle missing/invalid
    values.  Can pass things through (the default to all agg functions),
    ignore any bad values, transform any bad values to zero, or make the
    entire aggregation fail if there are any bad values.
    """
    @staticmethod
    def keep_missing(events):
        """no-op - default"""
        return events

    @staticmethod
    def ignore_missing(events):
        """Pull out the bad values resulting in a shorter array."""
        good = list()

        for i in events:
            if i is not None:
                good.append(i)

        return good

    @staticmethod
    def zero_missing(events):
        """Make bad values 0 - array will be the same length."""
        filled = list()

        for i in events:
            if i is None:
                filled.append(0)
            else:
                filled.append(i)

        return filled

    @staticmethod
    def propogate_missing(events):
        """It's all bad if there are missing values - return None if so."""
        for i in events:
            if i is None:
                return None

        return events

FILTER_NAMES = [x[0] for x in inspect.getmembers(Filters, predicate=inspect.isfunction)]


def f_check(flt):
    """Set the default filter for aggregation operations when no
    filter is specified. When one is, make sure that it is a
    valid filter.
    """

    # default case when no filter is specified to a higher
    # level aggregation method.
    if flt is None:
        return Filters.keep_missing

    # are we legit?
    if not callable(flt) or flt.__name__ not in FILTER_NAMES:
        msg = 'Invalid filter from pypond.functions.Filters got: {0} {1} {2}'.format(
            flt.__name__, type(flt), callable(flt))
        msg += '  filter names: {0}end list'.format(FILTER_NAMES)
        raise FilterException(msg)

    # we are legit
    return flt


class Functions(object):
    """
    Utility class to contain the functions.

    The inner() function is the one that does the actual processing and
    it returned by calling the outer named function.  Previously one would
    pass Functions.sum to an aggregation or reducer method::

        timeseries.aggregate(Functions.sum, 'in')

    Now it is a factory to return the acutal function::

        timeseries.aggregate(Functions.sum(), 'in')

    The static methods in the Filters class can be passed to the outer
    factory method to control how bad values are handled::

        timeseries.aggregate(Functions.sum(Filters.zero_missing), 'in')
    """

    # pylint: disable=missing-docstring
    # skipping coverage on all the propogate_missing logic because
    # those don't need specific tests.

    @staticmethod
    def keep(flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            result = Functions.first()(vals)

            for i in vals:
                if i is not None and i != result:
                    return None  # pragma: no cover

            return result

        return inner

    @staticmethod
    def sum(flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            return reduce(lambda x, y: x + y, vals, 0)

        return inner

    @staticmethod
    def avg(flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            return float(Functions.sum()(vals)) / len(vals)

        return inner

    @staticmethod
    def max(flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            return max(vals)

        return inner

    @staticmethod
    def min(flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            return min(vals)

        return inner

    @staticmethod
    def count(flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            return len(vals)

        return inner

    @staticmethod
    def first(flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            try:
                return vals[0]
            except IndexError:
                return None

        return inner

    @staticmethod
    def last(flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            try:
                return vals[-1]
            except IndexError:
                return None

        return inner

    @staticmethod
    def stddev(flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            avg = Functions.avg()(vals)
            variance = [(e - avg)**2 for e in vals]
            return sqrt(Functions.avg()(variance))

        return inner

    @staticmethod
    def median(flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            sort = sorted(vals)
            half = len(sort) // 2

            if not len(sort) % 2:

                return (sort[half - 1] + sort[half]) / 2.0
            return sort[half]

        return inner

    @staticmethod
    def difference(flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            return max(vals) - min(vals)

        return inner
