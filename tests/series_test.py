"""
Tests for the TimeSeries class

Also including tests for Collection class since they are tightly bound.
"""

import unittest
import warnings

from pypond.event import Event
from pypond.collection import Collection
from pypond.exceptions import CollectionWarning

EVENT_LIST = [
    Event(1429673400000, {'in': 1, 'out': 2}),
    Event(1429673460000, {'in': 3, 'out': 4}),
    Event(1429673520000, {'in': 5, 'out': 6}),
]


class SeriesBase(unittest.TestCase):
    """
    base for the tests.
    """
    def setUp(self):
        """setup."""
        self._canned_collection = Collection(EVENT_LIST)


class TestCollectionCreation(SeriesBase):
    """
    Tests for the collection class.
    """
    def test_create_collection(self):
        """test collection creation."""

        # event list
        col_1 = Collection(EVENT_LIST)
        self.assertEquals(col_1.size(), 3)

        # copy ctor
        col_2 = Collection(col_1)
        self.assertEquals(col_2.size(), 3)

        # copy ctor - no event copy
        col_3 = Collection(col_2, copy_events=False)
        self.assertEquals(col_3.size(), 0)

        # pass in an immutable - use a pre- _check()'ed one
        col_4 = Collection(col_1._event_list)  # pylint: disable=protected-access
        self.assertEquals(col_4.size(), 3)

    def test_bad_args(self):
        """pass in bad values"""
        with warnings.catch_warnings(record=True) as wrn:
            bad_col = Collection(dict())
            self.assertEquals(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, CollectionWarning))
            self.assertEquals(bad_col.size(), 0)


class TestTimeSeriesCreation(SeriesBase):
    """
    Test variations of TimeSeries object creation.
    """
    def test_wire(self):
        """TimeSeries created with our wire format"""
        pass

    def test_event_list(self):
        """TimeSeries created with a list of events"""
        pass

if __name__ == '__main__':
    unittest.main()
