"""
Tests for ordering the contents of a TimeSeries/Collection.

Those test modules are already out of hand to make a new one.
"""

import unittest

from pypond.series import TimeSeries

# each time series is in order but originally could trigger an
# Collection.is_chronological() == False after they were reduced
# by TimeSeries.timeseries_list_reduce().

TS1 = TimeSeries({
    "utc": True,
    "name": "base",
    "columns": ["index", "in", "out"],
    "points": [
        ["5m-4855968", 0.0, 0.0],
        ["5m-4855969", 0.0, 0.0],
        ["5m-4855970", 0.0, 0.0],
        ["5m-4855971", 0.0, 0.0],
        ["5m-4855972", 0.0, 0.0],
        ["5m-4855973", 0.0, 0.0],
        ["5m-4855974", 0.0, 0.0],
        ["5m-4855975", 0.0, 0.0],
        ["5m-4855976", 0.0, 0.0],
        ["5m-4855977", 0.0, 0.0],
    ]
})

TS2 = TimeSeries({
    "utc": True,
    "name": "all",
    "columns": ["index", "in", "out"],
    "points": [
        ["5m-4855968", 1.0, 1.0],
        ["5m-4855969", 1.0, 1.0],
        ["5m-4855970", 1.0, 1.0],
        ["5m-4855971", 1.0, 1.0],
        ["5m-4855972", 1.0, 1.0],
        ["5m-4855973", 1.0, 1.0],
        ["5m-4855974", 1.0, 1.0],
        ["5m-4855975", 1.0, 1.0],
        ["5m-4855976", 1.0, 1.0],
        ["5m-4855977", 1.0, 1.0],
    ]
})

TS3 = TimeSeries({
    "utc": True,
    "name": "first half",
    "columns": ["index", "in", "out"],
    "points": [
        ["5m-4855968", 1.0, 1.0],
        ["5m-4855969", 1.0, 1.0],
        ["5m-4855970", 1.0, 1.0],
        ["5m-4855971", 1.0, 1.0],
        ["5m-4855972", 1.0, 1.0],
    ]
})


TS4 = TimeSeries({
    "utc": True,
    "name": "middle",
    "columns": ["index", "in", "out"],
    "points": [
        ["5m-4855971", 1.0, 1.0],
        ["5m-4855972", 1.0, 1.0],
        ["5m-4855973", 1.0, 1.0],
        ["5m-4855974", 1.0, 1.0],
    ]
})

TS5 = TimeSeries({
    "utc": True,
    "name": "last half",
    "columns": ["index", "in", "out"],
    "points": [
        ["5m-4855973", 1.0, 1.0],
        ["5m-4855974", 1.0, 1.0],
        ["5m-4855975", 1.0, 1.0],
        ["5m-4855976", 1.0, 1.0],
        ["5m-4855977", 1.0, 1.0],
    ]
})


class TestOrdering(unittest.TestCase):
    """
    Test various ordering maneuvers.
    """

    def test_series_reduce(self):
        """Test reduce re-ordering out of order start times."""

        # Mix the ordering up so they're out of chronological order
        summ = TimeSeries.timeseries_list_sum(
            {"name": "summ"}, [TS4, TS2, TS3, TS1, TS5], ["in", "out"])

        self.assertEqual(summ.size(), 10)
        self.assertEqual(summ.at(0).get('in'), 2)
        self.assertEqual(summ.at(1).get('in'), 2)
        self.assertEqual(summ.at(2).get('in'), 2)
        self.assertEqual(summ.at(3).get('in'), 3)
        self.assertEqual(summ.at(4).get('in'), 3)
        self.assertEqual(summ.at(5).get('in'), 3)
        self.assertEqual(summ.at(6).get('in'), 3)
        self.assertEqual(summ.at(7).get('in'), 2)
        self.assertEqual(summ.at(8).get('in'), 2)
        self.assertEqual(summ.at(9).get('in'), 2)


if __name__ == '__main__':
    unittest.main()
