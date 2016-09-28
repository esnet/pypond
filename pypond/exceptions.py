#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Custom exception and warning classes.
"""


class EventException(Exception):
    """Custom Event exception"""

    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class EventWarning(Warning):
    """Custom Event warning"""
    pass


class TimeRangeException(Exception):
    """Custom TimeRange exception"""

    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class TimeRangeWarning(Warning):
    """Custom TimeRange warning"""
    pass


class IndexException(Exception):
    """Custom Index exception"""

    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class IndexWarning(Warning):
    """Custom Index warning"""
    pass


class UtilityException(Exception):
    """Custom Utility exception"""

    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class UtilityWarning(Warning):
    """Custom Utility warning"""
    pass


class PipelineException(Exception):
    """Custom Pipeline exception"""

    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class PipelineWarning(Warning):
    """Custom Pipeline warning"""
    pass


class PipelineIOException(Exception):
    """Custom PipelineIO exception"""

    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class PipelineIOWarning(Warning):
    """Custom PipelineIO warning"""
    pass


class CollectionException(Exception):
    """Custom Collection exception"""

    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class CollectionWarning(Warning):
    """Custom Collection warning"""
    pass


class TimeSeriesException(Exception):
    """Custom TimeSeries exception"""

    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class TimeSeriesWarning(Warning):
    """Custom TimeSeries warning"""
    pass


class ProcessorException(Exception):
    """Custom Processor exception"""

    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class ProcessorWarning(Warning):
    """Custom Processor warning"""
    pass


class FilterException(Exception):
    """Custom Filter exception"""

    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class FilterWarning(Warning):
    """Custom Filter warning"""
    pass


class FunctionException(Exception):
    """Custom Function exception"""

    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):  # pragma: no cover
        return repr(self.value)


class FunctionWarning(Warning):
    """Custom Function warning"""
    pass

NAIVE_MESSAGE = 'non-naive (aware) datetime objects required'
