"""
Unify the processor classes from the individual modules so one can:

from pypond.processor import Mapper
"""

from .aggregator import Aggregator
from .base import Processor  # include for isisntance() tests
from .collapser import Collapser
from .converter import Converter
from .filler import Filler
from .filter import Filter
from .mapper import Mapper
from .offset import Offset
from .selector import Selector
from .taker import Taker
