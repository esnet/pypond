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

# A set of raw counters that was fed to an esmond instance
# then calculated into rates.  Used for testing
# TimeSeries.align(window='30s').rate()

RAW = dict(
    name="raw",
    columns=["time", "value"],
    points=[
        [1343956814000, 281577000],
        [1343956845000, 281577600],
        [1343956876000, 281578800],
        [1343956906000, 281578800],
        [1343956936000, 281579160],
        [1343956967000, 281579520],
        [1343956997000, 281579520],
        [1343957028000, 281581080],
        [1343957058000, 281581860],
        [1343957092000, 281582520],
        [1343957119000, 281582940],
        [1343957150000, 281583240],
        [1343957180000, 281583900],
        [1343957211000, 281584020],
        [1343957241000, 281584500],
        [1343957271000, 281584500],
        [1343957302000, 281585100],
        [1343957333000, 281585160],
        [1343957363000, 281585160],
        [1343957394000, 281585760],
        [1343957424000, 281586480],
        [1343957455000, 281587620],
        [1343957485000, 281588160],
        [1343957516000, 281588580],
        [1343957546000, 281588940],
        [1343957576000, 281596440],
        [1343957607000, 281596740],
        [1343957637000, 281596740],
        [1343957668000, 281597400],
        [1343957699000, 281597940],
        [1343957729000, 281598000],
        [1343957760000, 281598600],
        [1343957790000, 281598600],
        [1343957821000, 281598660],
        [1343957851000, 281599260],
        [1343957881000, 281599260],
        [1343957912000, 281600160],
        [1343957943000, 281600520],
        [1343957973000, 281600520],
        [1343958003000, 281601120],
        [1343958034000, 281603580],
        [1343958064000, 281607240],
        [1343958095000, 281607240],
        [1343958125000, 281607240],
        [1343958156000, 281607840],
        [1343958186000, 281608200],
        [1343958216000, 281609700],
        [1343958247000, 281609700],
        [1343958278000, 281610000],
        [1343958308000, 281610720],
        [1343958339000, 281610720],
        [1343958369000, 281611020],
        [1343958400000, 281611920],
        [1343958430000, 281611980],
        [1343958460000, 281611980],
        [1343958491000, 281612580],
        [1343958521000, 281612580],
        [1343958552000, 281613540],
        [1343958582000, 281614200],
        [1343958613000, 281614380],
        [1343958643000, 281615100],
        [1343958674000, 281615760],
        [1343958704000, 281616060],
        [1343958735000, 281617260],
        [1343958765000, 281617260],
        [1343958795000, 281618220],
        [1343958826000, 281618820],
        [1343958856000, 281618820],
        [1343958887000, 281619420],
        [1343958918000, 281620680],
        [1343958948000, 281620680],
        [1343958979000, 281620980],
        [1343959009000, 281621280],
        [1343959040000, 281621640],
        [1343959070000, 281622300],
        [1343959100000, 281622600],
        [1343959131000, 281623200],
        [1343959161000, 281623500],
        [1343959194000, 281624160],
    ]
)

BASE = dict(
    name="base_rates",
    columns=["time", "value"],
    points=[
        # first point commented out so these and the generated
        # rates will be at the same indexes
        # [1343956800000, 20.2666666667],
        [1343956830000, 29.0333333333],
        [1343956860000, 20.6333333333],
        [1343956890000, 5.6],
        [1343956920000, 11.8333333333],
        [1343956950000, 6.56666666667],
        [1343956980000, 21.8],
        [1343957010000, 40.6],
        [1343957040000, 23.3666666667],
        [1343957070000, 18.3666666667],
        [1343957100000, 13.4],
        [1343957130000, 13.8],
        [1343957160000, 15.9666666667],
        [1343957190000, 7.5],
        [1343957220000, 11.2],
        [1343957250000, 5.8],
        [1343957280000, 14.7],
        [1343957310000, 1.5],
        [1343957340000, 4.5],
        [1343957370000, 20.3],
        [1343957400000, 26.5666666667],
        [1343957430000, 33.6333333333],
        [1343957460000, 17.2666666667],
        [1343957490000, 13.3333333333],
        [1343957520000, 43.7333333333],
        [1343957550000, 217.966666667],
        [1343957580000, 8.7],
        [1343957610000, 2.13333333333],
        [1343957640000, 21.0333333333],
        [1343957670000, 16.9],
        [1343957700000, 2.56666666667],
        [1343957730000, 19.3666666667],
        [1343957760000, 0.0],
        [1343957790000, 1.93333333333],
        [1343957820000, 19.4],
        [1343957850000, 0.666666666667],
        [1343957880000, 28.0666666667],
        [1343957910000, 12.7666666667],
        [1343957940000, 1.16666666667],
        [1343957970000, 18.0],
        [1343958000000, 73.4333333333],
        [1343958030000, 116.3],
        [1343958060000, 16.2666666667],
        [1343958090000, 0.0],
        [1343958120000, 16.1333333333],
        [1343958150000, 13.4666666667],
        [1343958180000, 42.4],
        [1343958210000, 10.0],
        [1343958240000, 7.43333333333],
        [1343958270000, 20.1666666667],
        [1343958300000, 6.4],
        [1343958330000, 7.0],
        [1343958360000, 23.3333333333],
        [1343958390000, 11.0],
        [1343958420000, 0.666666666667],
        [1343958450000, 12.9],
        [1343958480000, 7.1],
        [1343958510000, 19.6],
        [1343958540000, 25.6],
        [1343958570000, 12.3],
        [1343958600000, 16.1],
        [1343958630000, 22.4666666667],
        [1343958660000, 15.2666666667],
        [1343958690000, 25.3],
        [1343958720000, 19.3666666667],
        [1343958750000, 16.0],
        [1343958780000, 25.6666666667],
        [1343958810000, 10.3333333333],
        [1343958840000, 9.03333333333],
        [1343958870000, 28.5666666667],
        [1343958900000, 24.4],
        [1343958930000, 3.86666666667],
        [1343958960000, 9.8],
        [1343958990000, 10.6],
        [1343959020000, 15.0666666667],
        [1343959050000, 18.0],
        [1343959080000, 13.1333333333],
        [1343959110000, 16.5333333333],
        [1343959140000, 13.0],
        [1343959170000, 16.0],
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

    def test_rate_bins_long(self):
        """replicate counter to rate conversion with more data."""

        raw_ts = TimeSeries(RAW)
        base_rates = raw_ts.align(window='30s').rate()

        for i in enumerate(base_rates.collection().events()):

            # there are going to be decimal rounding variations but
            # for the purposes of this sanity check, if they are
            # equal to one decimal place is close enough.
            self.assertAlmostEqual(
                i[1].get('value_rate'),
                BASE.get('points')[i[0]][1],
                places=1
            )

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
