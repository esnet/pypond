"""
Tests for the TimeSeries class
"""

import unittest


class TestTimeSeriesCreation(unittest.TestCase):
    """
    Test variations of TimeSeries object creation.
    """
    def test_wire(self):
        """TimeSeries created with our wire format"""
        print 'wire'

    def test_event_list(self):
        """TimeSeries created with a list of events"""
        print 'event_list'

if __name__ == '__main__':
    unittest.main()
