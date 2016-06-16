[![Documentation Status](https://readthedocs.org/projects/pypond/badge/?version=latest)](http://pypond.readthedocs.io/en/latest/?badge=latest) [![Build Status](https://travis-ci.org/esnet/pypond.svg?branch=master)](https://travis-ci.org/esnet/pypond) [![Coverage Status](https://coveralls.io/repos/github/esnet/pypond/badge.svg?branch=master)](https://coveralls.io/github/esnet/pypond?branch=master)

# PyPond - Python Pond timeseries library.

## Overview

PyPond is a Python implementation of the JavaScript [Pond timeseries library](http://software.es.net/pond/). At a very high level, both implementations offer classes and structures to collect, manipulate and transmit timeseries data. Time series transmission is done via a JSON-based wire format.

This implementation is [available on GitHub](https://github.com/esnet/pypond) and the API documentation is available at [software.es.net](http://software.es.net/pypond/) (and alternately on [RTD](http://pypond.readthedocs.io/en/latest/)).

PyPond runs on python 2.7 and 3.3 through 3.6.

## Core Documentation

The [main project site](http://software.es.net/pond/) has extensive documentation on the various structures (Event, TimeRange, TimeSeries, etc) that both implementations use internally. There is no need to duplicate that conceptual documentation here since the python implementation follows the same API and uses the same structures.

The only real difference with pypond is that the method names have been changed to their obvious pythonic corollaries (`obj.toString()` becomes `obj.to_string()`) and any comparison methods named `.is()` in the JavaScript version have been renamed to `.same()` in pypond since `is` is a reserved word in python.

The [tests](https://github.com/esnet/pypond/tree/master/tests) can also be referred to as a fairly complete set of examples as well.
