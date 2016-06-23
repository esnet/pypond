#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Implementation of the Pond Pipeline classes.

http://software.es.net/pond/#/pipeline
"""

from .bases import PypondBase


class Runner(PypondBase):  # pylint: disable=too-few-public-methods
    """
    A runner is used to extract the chain of processing operations
    from a Pipeline given an Output. The idea here is to traverse
    back up the Pipeline(s) and build an execution chain.

    When the runner is started, events from the "in" are streamed
    into the execution chain and outputed into the "out".

    Rebuilding in this way enables us to handle connected pipelines:

    ::

                            |--
         in --> pipeline ---.
                            |----pipeline ---| -> out

    The runner breaks this into the following for execution:

    ::

          _input        - the "in" or from() bounded input of
                          the upstream pipeline
          _processChain - the process nodes in the pipelines
                          leading to the out
          _output       - the supplied output destination for
                          the batch process

    NOTE: There's no current way to merge multiple sources, though
          a time series has a TimeSeries.merge() static method for
          this purpose.

    Parameters
    ----------
    pipeline : Pipeline
        The pipeline to run.
    output : PipelineOut
        The output driving this runner
    """

    def __init__(self, pipeline, output):
        """Create a new batch runner"""
        super(Runner, self).__init__()

        self._pipeline = pipeline
        self._output = output

        # We use the pipeline's chain() function to walk the
        # DAG back up the tree to the "in" to:
        # 1) assemble a list of process nodes that feed into
        #    this pipeline, the processChain
        # 2) determine the _input
        #
        # NOTE: we do not currently support merging, so this is
        # a linear chain.

        # Using the list of nodes in the tree that will be involved in
        # our processing we can build an execution chain. This is the
        # chain of processor clones, linked together, for our specific
        # processing pipeline. We run this execution chain later by
        # evoking start().

    def start(self, force=False):
        """Start the runner

        Args:
            force (bool, optional): force Flush at the end of the batch source
            to cause any buffers to emit.
        """
        # Clear any results ready for the run

        # The head is the first process node in the execution chain.
        # To process the source through the execution chain we add
        # each event from the input to the head.

        # The runner indicates that it is finished with the bounded
        # data by sending a flush() call down the chain. If force is
        # set to false (the default) this is never called.
        pass


class Pipeline(PypondBase):
    """
    Build a new Pipeline.

    A pipeline manages a processing chain, for either batch or stream processing
    of collection data.

    The argument may be either:

    - a Pipeline (copy ctor)
    - a pyrsistent.PMap in which case the internal state will be constructed from the map.

    Usually you would initialize a Pipeline using the factory function,
    rather than this object directly.

    Parameters
    ----------
    arg : Pipeline, PMap, None
        See above.

    Returns
    -------
    TYPE
        Description
    """

    def __init__(self, arg):
        """New pipeline."""
        super(Pipeline, self).__init__()

    # Accessors to the current Pipeline state

    def input(self):
        """Originally called in() in JS code."""
        return self._d.get('in')

    def mode(self):
        return self._d.get('mode')

    def first(self):
        return self._d.get('first')

    def last(self):
        return self._d.get('last')

    def get_window_type(self):
        return self._d.get('windowType')

    def get_window_duration(self):
        return self._d.get('windowDuration')

    def get_group_by(self):
        return self._d.get('groupBy')

    def get_emit_on(self):
        return self._d.get('emitOn')

    # Results

    def clear_results(self):
        raise NotImplementedError

    def add_result(self, arg1, arg2):
        raise NotImplementedError

    def results_done(self):
        raise NotImplementedError

    # Pipeline mutations

    def _set_in(self, input):
        raise NotImplementedError

    def _set_first(self, i):
        raise NotImplementedError

    def _set_last(self, i):
        raise NotImplementedError

    def _append(self, processor):
        raise NotImplementedError

    # Pipeline state chained methods

    def window_by(self, win):
        raise NotImplementedError

    def clear_window(self):
        raise NotImplementedError

    def group_by(self, key):
        raise NotImplementedError

    def clear_group_by(self):
        raise NotImplementedError

    def emit_on(self, trigger):
        raise NotImplementedError

    def from_source(self, src):
        """originally named from() in JS code."""
        raise NotImplementedError

    def to_event_list(self):
        raise NotImplementedError

    def to_keyed_collections(self):
        raise NotImplementedError

    def to(self):
        raise NotImplementedError

    def count(self):
        raise NotImplementedError

    def offset_by(self, by, field_spec):
        raise NotImplementedError

    def aggregate(self, fields):
        raise NotImplementedError

    def as_events(self, options):
        raise NotImplementedError

    def map(self, op):  # pylint: disable=invalid-name
        raise NotImplementedError

    def filter(self, op):  # pylint: disable=invalid-name
        raise NotImplementedError

    def select(self, field_spec):
        raise NotImplementedError

    def collapse(self, field_spec, name, reducer, append):
        raise NotImplementedError

    def take(self, limit):
        raise NotImplementedError

    def as_time_range_events(self, options):
        raise NotImplementedError

    def as_indexed_events(self, options):
        raise NotImplementedError

# module functions

def pipeline(args):
    return Pipeline(args)

def is_pipeline(pline):
    return isinstance(pline, Pipeline)
