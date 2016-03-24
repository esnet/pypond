"""
Custom exception and warning classes.
"""


class EventException(Exception):
    """Custom Event exception"""
    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):
        return repr(self.value)


class EventWarning(Warning):
    """Custom Event warning"""
    pass


class UtilityException(Exception):
    """Custom Utility exception"""
    def __init__(self, value):
        # pylint: disable=super-init-not-called
        self.value = value

    def __str__(self):
        return repr(self.value)


class UtilityWarning(Warning):
    """Custom Utility warning"""
    pass


NAIVE_MESSAGE = 'non-naive (aware) datetime objects required'
