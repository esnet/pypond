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


NAIVE_MESSAGE = 'non-naive (aware) datetime objects required'
