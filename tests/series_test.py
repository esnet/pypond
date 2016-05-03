"""
Tests for the TimeSeries class

Also including tests for Collection class since they are tightly bound.
"""

import datetime
import json
import unittest
import warnings

# import event before collection or circular import issue will happen.
from pypond.event import Event
from pypond.collection import Collection
from pypond.exceptions import CollectionWarning
from pypond.util import is_pvector

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
        """test collection creation and methods related to internal payload."""

        # event list
        col_1 = Collection(EVENT_LIST)
        self.assertEquals(col_1.size(), 3)
        self.assertEquals(col_1.type(), Event)
        self.assertEquals(col_1.size_valid('in'), 3)

        # copy ctor
        col_2 = Collection(col_1)
        self.assertEquals(col_2.size(), 3)
        self.assertEquals(col_2.type(), Event)
        self.assertEquals(col_2.size_valid('in'), 3)

        # copy ctor - no event copy
        col_3 = Collection(col_2, copy_events=False)
        self.assertEquals(col_3.size(), 0)
        self.assertIsNone(col_3.type())
        self.assertEquals(col_3.size_valid('in'), 0)

        # pass in an immutable - use a pre- _check()'ed one
        col_4 = Collection(col_1._event_list)  # pylint: disable=protected-access
        self.assertEquals(col_4.size(), 3)
        # _check() is not run when immutable comes in.
        self.assertIsNone(col_4.type())
        self.assertEquals(col_4.size_valid('in'), 3)

    def test_accessor_methods(self):
        """test various access methods. Mostly for coverage."""

        col = self._canned_collection

        # basic accessors
        self.assertEquals(col.to_json(), EVENT_LIST)

        # test at() - corollary to array index
        self.assertTrue(Event.same(col.at(2), EVENT_LIST[2]))

        # test at_time()
        # get timestamp of second event and add some time to it
        ref_dtime = EVENT_LIST[1].timestamp() + datetime.timedelta(seconds=3)
        self.assertTrue(Event.same(col.at_time(ref_dtime), EVENT_LIST[1]))

        # at_first() and at_last()
        self.assertTrue(Event.same(col.at_first(), EVENT_LIST[0]))
        self.assertTrue(Event.same(col.at_last(), EVENT_LIST[2]))

        # get the raw event list
        self.assertTrue(is_pvector(col.event_list()))
        self.assertTrue(isinstance(col.event_list_as_list(), list))

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
