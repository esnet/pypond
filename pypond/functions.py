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

from functools import reduce
from math import sqrt, floor
from operator import truediv

from .exceptions import FilterException, FunctionException
from .util import is_valid


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
            if is_valid(i):
                good.append(i)

        return good

    @staticmethod
    def zero_missing(events):
        """Make bad values 0 - array will be the same length."""
        filled = list()

        for i in events:
            if not is_valid(i):
                filled.append(0)
            else:
                filled.append(i)

        return filled

    @staticmethod
    def propogate_missing(events):
        """It's all bad if there are missing values - return None if so."""
        for i in events:
            if not is_valid(i):
                return None

        return events

    @staticmethod
    def none_if_empty(events):
        """Return none if the event list is empty. Could be used to override
        the default behavior of Functions.avg(), etc"""
        if len(events) == 0:
            return None
        else:
            return events


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
    if not callable(flt) or not hasattr(Filters, flt.__name__):
        msg = 'Invalid filter from pypond.functions.Filters got: {0} {1}'.format(
            flt.__name__, type(flt))
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

            if len(vals) is 0:
                return 0

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
    def percentile(perc, method='linear', flt=Filters.keep_missing):

        def inner(values):

            vals = flt(values)

            if vals is None:
                return None  # pragma: no cover

            ret = None

            sort_values = sorted(values)
            size = len(sort_values)

            if perc < 0 or perc > 100:
                msg = 'percentile must be between 0 and 100'
                raise FunctionException(msg)

            i = truediv(perc, 100)
            index = int(floor((size - 1) * i))

            if size == 1 or perc == 0:
                return sort_values[0]

            if perc == 100:
                return sort_values[size - 1]

            if index < size - 1:
                fraction = (size - 1) * i - index
                # pylint: disable=invalid-name
                v0 = sort_values[index]
                v1 = sort_values[index + 1]

                if method == 'lower' or fraction == 0:
                    ret = v0
                elif method == 'linear':
                    ret = v0 + (v1 - v0) * fraction
                elif method == 'higher':
                    ret = v1
                elif method == 'nearest':
                    ret = v0 if fraction < .5 else v1
                elif method == 'midpoint':
                    ret = (v0 + v1) / 2

            return ret

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
