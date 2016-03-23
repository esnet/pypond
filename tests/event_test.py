"""
Tests for the Event class
"""
import datetime
import unittest

from pypond.event import Event


class TestEventCreation(unittest.TestCase):
    """
    Test variations of Event object creation.
    """
    def _create_regular(self, args):  # pylint: disable=no-self-use
        return Event(*args)

    def test_regular_deep_data(self):
        """create regular event w/deep data"""
        print 'deep'

    def test_indexed_index_data(self):
        """create an IndexedEvent using a string index and data"""
        print 'IndexedEvent'

    def test_regular_with_dt_data_key(self):
        """create a regular Event with a datetime, dict and key"""
        ts = datetime.datetime.utcnow()
        data = {'a': 3, 'b': 6}
        key = 'cpu_usage'
        args = [ts, data, key]
        event = self._create_regular(args)
        self.assertEqual(event.key(), key)


if __name__ == '__main__':
    unittest.main()
