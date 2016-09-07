#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Unify the processor classes from the individual modules so one can:

from pypond.processor import Mapper
"""

from .aggregator import Aggregator
from .align import Align
from .base import Processor  # include for isisntance() tests
from .collapser import Collapser
from .converter import Converter
from .filler import Filler
from .filter import Filter
from .mapper import Mapper
from .offset import Offset
from .rate import Rate
from .selector import Selector
from .taker import Taker
