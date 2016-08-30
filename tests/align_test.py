"""
Tests for the align and rate processors.
"""

import copy
import unittest
import warnings

from pypond.exceptions import ProcessorException, ProcessorWarning
from pypond.series import TimeSeries
from pypond.processor import Align, Rate

SIMPLE_GAP_DATA = dict(
    name="traffic",
    columns=["time", "value"],
    points=[
        [1471824030000, .75],  # Mon, 22 Aug 2016 00:00:30 GMT
        [1471824105000, 2],  # Mon, 22 Aug 2016 00:01:45 GMT
        [1471824210000, 1],  # Mon, 22 Aug 2016 00:03:30 GMT
        [1471824390000, 1],  # Mon, 22 Aug 2016 00:06:30 GMT
        [1471824510000, 3],  # Mon, 22 Aug 2016 00:08:30 GMT
        # final point in same window, does nothing, for coverage
        [1471824525000, 5],  # Mon, 22 Aug 2016 00:08:45 GMT
    ]
)

# already aligned totally synthetic rates to make sure
# the underlying math is at the right order of magnitude.
RATE = dict(
    name='traffic',
    columns=['time', 'in'],
    points=[
        [0, 1],
        [30000, 3],
        [60000, 10],
        [90000, 40],
        [120000, 70],
        [150000, 130],
        [180000, 190],
        [210000, 220],
        [240000, 300],
        [270000, 390],
        [300000, 510],
    ]
)


class AlignTest(unittest.TestCase):
    """
    tests for the align processor
    """

    def setUp(self):
        """setup for all tests."""
        self._simple_ts = TimeSeries(SIMPLE_GAP_DATA)

    def test_basic_linear_align(self):
        """test basic align"""

        aligned = self._simple_ts.align(window='1m')

        self.assertEqual(aligned.size(), 8)
        self.assertEqual(aligned.at(0).get(), 1.25)
        self.assertEqual(aligned.at(1).get(), 1.8571428571428572)
        self.assertEqual(aligned.at(2).get(), 1.2857142857142856)
        self.assertEqual(aligned.at(3).get(), 1.0)
        self.assertEqual(aligned.at(4).get(), 1.0)
        self.assertEqual(aligned.at(5).get(), 1.0)
        self.assertEqual(aligned.at(6).get(), 1.5)
        self.assertEqual(aligned.at(7).get(), 2.5)

    def test_basic_hold_align(self):
        """test basic hold align."""

        aligned = self._simple_ts.align(window='1m', method='hold')

        self.assertEqual(aligned.size(), 8)
        self.assertEqual(aligned.at(0).get(), .75)
        self.assertEqual(aligned.at(1).get(), 2)
        self.assertEqual(aligned.at(2).get(), 2)
        self.assertEqual(aligned.at(3).get(), 1)
        self.assertEqual(aligned.at(4).get(), 1)
        self.assertEqual(aligned.at(5).get(), 1)
        self.assertEqual(aligned.at(6).get(), 1)
        self.assertEqual(aligned.at(7).get(), 1)

    def test_align_limit(self):
        """test basic hold align."""

        aligned = self._simple_ts.align(window='1m', method='hold', limit=2)

        self.assertEqual(aligned.size(), 8)
        self.assertEqual(aligned.at(0).get(), .75)
        self.assertEqual(aligned.at(1).get(), 2)
        self.assertEqual(aligned.at(2).get(), 2)
        self.assertEqual(aligned.at(3).get(), None)  # over limit, fill with None
        self.assertEqual(aligned.at(4).get(), None)  # over limit, fill with None
        self.assertEqual(aligned.at(5).get(), None)  # over limit, fill with None
        self.assertEqual(aligned.at(6).get(), 1)
        self.assertEqual(aligned.at(7).get(), 1)

        aligned = self._simple_ts.align(field_spec='value', window='1m', method='linear', limit=2)

        self.assertEqual(aligned.size(), 8)
        self.assertEqual(aligned.at(0).get(), 1.25)
        self.assertEqual(aligned.at(1).get(), 1.8571428571428572)
        self.assertEqual(aligned.at(2).get(), 1.2857142857142856)
        self.assertEqual(aligned.at(3).get(), None)  # over limit, fill with None
        self.assertEqual(aligned.at(4).get(), None)  # over limit, fill with None
        self.assertEqual(aligned.at(5).get(), None)  # over limit, fill with None
        self.assertEqual(aligned.at(6).get(), 1.5)
        self.assertEqual(aligned.at(7).get(), 2.5)

    def test_invalid_point(self):
        """make sure non-numeric values are handled properly."""

        bad_point = copy.deepcopy(SIMPLE_GAP_DATA)
        bad_point.get('points')[-2][1] = 'non_numeric_value'
        ts = TimeSeries(bad_point)

        with warnings.catch_warnings(record=True) as wrn:
            aligned = ts.align(window='1m')
            self.assertEqual(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, ProcessorWarning))

        self.assertEqual(aligned.size(), 8)
        self.assertEqual(aligned.at(0).get(), 1.25)
        self.assertEqual(aligned.at(1).get(), 1.8571428571428572)
        self.assertEqual(aligned.at(2).get(), 1.2857142857142856)
        self.assertEqual(aligned.at(3).get(), 1.0)
        self.assertEqual(aligned.at(4).get(), 1.0)
        self.assertEqual(aligned.at(5).get(), 1.0)
        self.assertEqual(aligned.at(6).get(), None)  # bad value
        self.assertEqual(aligned.at(7).get(), None)  # bad value

        with warnings.catch_warnings(record=True) as wrn:
            a_diff = aligned.rate()
            self.assertEqual(len(wrn), 1)
            self.assertTrue(issubclass(wrn[0].category, ProcessorWarning))

        self.assertEqual(a_diff.at(5).get(), None)  # bad value
        self.assertEqual(a_diff.at(6).get(), None)  # bad value

    def test_rate_mag(self):
        """test the rate processor order of mag."""

        ts = TimeSeries(RATE)
        rate = ts.rate(field_spec='in')

        # one less than source
        self.assertEqual(rate.size(), len(RATE.get('points')) - 1)
        self.assertEqual(rate.at(2).get('in_rate'), 1)
        self.assertEqual(rate.at(3).get('in_rate'), 1)
        self.assertEqual(rate.at(4).get('in_rate'), 2)
        self.assertEqual(rate.at(8).get('in_rate'), 3)
        self.assertEqual(rate.at(9).get('in_rate'), 4)

    def test_rate_bins(self):
        """replicate basic esmond rates."""

        #  |           100 |              |              |              |   200       |   v
        #  |           |   |              |              |              |   |         |
        # 60          89  90            120            150            180 181       210   t ->
        #  |               |              |              |              |             |
        #  |<- ? --------->|<- 1.08/s --->|<- 1.08/s --->|<- 1.08/s --->|<- ? ------->|   result

        raw_rates = dict(
            name="traffic",
            columns=["time", "value"],
            points=[
                [89000, 100],
                [181000, 200]
            ]
        )

        ts = TimeSeries(raw_rates)
        rates = ts.align(window='30s').rate()

        self.assertEqual(rates.size(), 3)
        self.assertEqual(rates.at(0).get('value_rate'), 1.0869565217391313)
        self.assertEqual(rates.at(1).get('value_rate'), 1.0869565217391293)
        self.assertEqual(rates.at(2).get('value_rate'), 1.0869565217391313)

    def test_negative_derivatives(self):
        """Test behavior on counter resets."""

        raw_rates = dict(
            name="traffic",
            columns=["time", "value"],
            points=[
                [89000, 100],
                [181000, 50]
            ]
        )

        ts = TimeSeries(raw_rates)
        rates = ts.align(window='30s').rate()

        # lower counter will produce negative derivatives
        self.assertEqual(rates.size(), 3)
        self.assertEqual(rates.at(0).get('value_rate'), -0.5434782608695656)
        self.assertEqual(rates.at(1).get('value_rate'), -0.5434782608695646)
        self.assertEqual(rates.at(2).get('value_rate'), -0.5434782608695653)

        rates = ts.align(window='30s').rate(allow_negative=False)

        self.assertEqual(rates.size(), 3)
        self.assertEqual(rates.at(0).get('value_rate'), None)
        self.assertEqual(rates.at(1).get('value_rate'), None)
        self.assertEqual(rates.at(2).get('value_rate'), None)

    def test_bad_args(self):
        """error states for coverage."""

        # various bad values
        with self.assertRaises(ProcessorException):
            Align(dict())

        with self.assertRaises(ProcessorException):
            Rate(dict())

        with self.assertRaises(ProcessorException):
            self._simple_ts.align(method='bogus')

        with self.assertRaises(ProcessorException):
            self._simple_ts.align(limit='bogus')

        # non event types
        ticket_range = dict(
            name="outages",
            columns=["timerange", "title", "esnet_ticket"],
            points=[
                [[1429673400000, 1429707600000], "BOOM", "ESNET-20080101-001"],
                [[1429673400000, 1429707600000], "BAM!", "ESNET-20080101-002"],
            ],
        )

        ts = TimeSeries(ticket_range)
        with self.assertRaises(ProcessorException):
            ts.align()

        with self.assertRaises(ProcessorException):
            ts.rate()

if __name__ == '__main__':
    unittest.main()
