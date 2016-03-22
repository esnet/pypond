"""
Tests for the TimeRange class
"""

import unittest


class TestTimeRangeCreation(unittest.TestCase):
    """
    Test variations of TimeRange object creation.
    """
    def test_new_begin_end(self):
        """create a new range with a begin and end time."""
        print 'begin/end'

    def test_new_epoch(self):
        """create a new range with two UNIX epoch times in an array"""
        print 'epoch'

if __name__ == '__main__':
    unittest.main()
