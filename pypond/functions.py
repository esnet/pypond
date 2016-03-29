"""
Functions to act as reducers, etc
"""


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
    def difference(values):
        return max(values) - min(values)
