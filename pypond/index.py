"""
Implementation of Pond Index class.

http://software.es.net/pond/#/index
"""


class Index(object):
    """
    An index that represents as a string a range of time. That range may either
    be in UTC or local time. UTC is the default.

    The actual derived timerange can be found using asRange(). This will return
    a TimeRange instance.

    The original string representation can be found with toString(). A nice
    version for date based indexes (e.g. 2015-03) can be generated with
    toNiceString(format) (e.g. March, 2015).
    """

    def __init__(self, s, utc):
        """Create the Index."""
        raise NotImplementedError

    def to_json(self):
        """
        Returns the Index as JSON, which will just be its string
        representation

        This is actually like json.loads(s) - produces the
        actual data structure."""
        raise NotImplementedError

    def to_string(self):
        """
        Simply returns the Index as its string

        In JS land, this is synonymous with __str__ or __unicode__
        """
        raise NotImplementedError

    def to_nice_string(self, fmt):
        """
        for the calendar range style Indexes, this lets you return
        that calendar range as a human readable format, e.g. "June, 2014".
        The format specified is a Moment.format.
        """
        raise NotImplementedError

    def as_string(self):
        """Alias for to_string()"""
        raise NotImplementedError

    def as_timerange(self):
        """Returns the Index as a TimeRange"""
        raise NotImplementedError

    def begin(self):
        """Returns start date of the index."""
        raise NotImplementedError

    def end(self):
        """Returns end data of the index."""
        raise NotImplementedError

    def __str__(self):
        """call to_string()"""
        raise NotImplementedError

    # Static class methods

    @staticmethod
    def get_index_string(win, date):
        """ TBA """
        raise NotImplementedError

    @staticmethod
    def get_bucket(win, date, key):
        """ TBA """
        raise NotImplementedError

    @staticmethod
    def get_index_string_list(win, timerange):
        """ TBA """
        raise NotImplementedError

    @staticmethod
    def get_bucket_list(win, timerange, key):
        """ TBA """
        raise NotImplementedError
