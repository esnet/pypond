"""
Tests for the Event class
"""

import unittest


class TestEventCreation(unittest.TestCase):
    """
    Test variations of Event object creation.
    """
    def test_regular_deep_data(self):
        """create regular event w/deep data"""
        print 'deep'

    def test_indexed_index_data(self):
        """create an IndexedEvent using a string index and data"""
        print 'IndexedEvent'

if __name__ == '__main__':
    unittest.main()
