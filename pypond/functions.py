"""
Functions to act as reducers, etc
"""

from math import sqrt


class Functions(object):
    """
    Utility class to contain the functions.
    """
    # pylint: disable=missing-docstring
    @staticmethod
    def sum(values):
        return reduce(lambda x, y: x + y, values, 0)

    @staticmethod
    def avg(values):
        return float(Functions.sum(values)) / len(values)

    @staticmethod
    def max(values):
        return max(values)

    @staticmethod
    def min(values):
        return min(values)

    @staticmethod
    def count(values):
        return len(values)

    @staticmethod
    def first(values):
        try:
            return values[0]
        except IndexError:
            return None

    @staticmethod
    def last(values):
        try:
            return values[-1]
        except IndexError:
            return None

    @staticmethod
    def stddev(values):
        avg = Functions.avg(values)
        variance = [(e - avg)**2 for e in values]
        return sqrt(Functions.avg(variance))

    @staticmethod
    def median(values):
        sort = sorted(values)
        half = len(sort) // 2

        if not len(sort) % 2:
            return (sort[half - 1] + sort[half]) / 2.0
        return sort[half]

    @staticmethod
    def difference(values):
        return max(values) - min(values)
