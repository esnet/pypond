# PyPond - Python Pond timeseries library.

## Overview

PyPond is a Python implementation of the JavaScript [Pond timeseries library](http://software.es.net/pond/). At a very high level, both implementations offer classes and structures to collect, manipulate and transmit timeseries data. Time series transmission is done via a JSON-based wire format.

This implementation is [hosted on GitHub](https://github.com/esnet/pypond) and the API documentation is [hosted on Read the Docs](http://pypond.readthedocs.org/).

## Documentation

The [main project link](http://software.es.net/pond/) has extensive documentation on the various structures (Event, TimeRange, TimeSeries, etc) that both implementations use internally. There is no need to duplicate that documentation here since the python implementation follows the same API and uses the same structures.

The only real difference with pypond is that the method names have been changed to their obvious pythonic corollaries (`obj.toString()` becomes `obj.to_string()`) and any comparison methods named `.is()` in the JavaScript version have been renamed to `.same()` in pypond since `is` is a reserved word.

### Running the tests

Running the unit tests will probably only be of interest to other developers. There is a test module that tests interoperability with the JavaScript library (`pypond/tests/interop_test.py`) that will require some additional setup.

1. It will check to find the `node` executable somewhere in the path. If it is not found, those tests will fail.
2. The [Pond source](https://github.com/esnet/pond) will need to be checked out at the same level "alongside" of the pypond source and then execute `npm install` followed by `npm run build` at the root level of the pond source. (Running `npm run build` should be a formality, but is included just in case the `pond/lib` directory was not properly regenerated from `pond/src`)
3. Execute `pip install -r dev-requirements.txt` and then run `nosetests` from either the source root or test directory (`pypond/` or `pypond/tests/`). The `pip` command will also install pypond in "develop" mode.

That particular test sends the data on a round trip by:

1. generating the wire format using the Python structures
2. sends it to an external script run by `node` as an arg
3. which reconstitutes the wire format as a JS structure
4. then the JS structure is used to generate the wire format again
5. wire format is returned to the calling unit test over stdout
6. a new Python structure is created with the incoming wire format
7. that structure is checked against the original data the first TimeSeries was created from.

All of the other tests are just standard-issue Python unit tests.

The tests can also be referred to as a fairly complete set of examples as well.
