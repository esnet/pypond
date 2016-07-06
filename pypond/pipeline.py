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

from pyrsistent import freeze

from .bases import PypondBase
from .exceptions import PipelineException
from .pipeline_io import CollectionOut
from .processors import (
    Collapser,
    Filter,
    Mapper,
    Offset,
    Processor,
    Selector,
    Taker,
)
from .series import TimeSeries
from .sources import BoundedIn, UnboundedIn
from .util import is_pmap, Options


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

    def __init__(self, pline, output):
        """Create a new batch runner"""
        super(Runner, self).__init__()

        self._log('Runner.init')

        self._pipeline = pline
        self._output = output
        self._input = None
        self._execution_chain = list()

        # We use the pipeline's chain() function to walk the
        # DAG back up the tree to the "in" to:
        # 1) assemble a list of process nodes that feed into
        #    this pipeline, the processChain
        # 2) determine the _input
        #
        # NOTE: we do not currently support merging, so this is
        # a linear chain.

        process_chain = list()

        if self._pipeline.last() is not None:
            process_chain = self._pipeline.last().chain()
            self._input = process_chain[0].pipeline().input()
        else:
            self._input = self._pipeline.input()

        # Using the list of nodes in the tree that will be involved in
        # our processing we can build an execution chain. This is the
        # chain of processor clones, linked together, for our specific
        # processing pipeline. We run this execution chain later by
        # evoking start().

        self._execution_chain = [self._output]

        prev = self._output

        for i in process_chain:
            if isinstance(i, Processor):
                processor = i.clone()
                if prev is not None:
                    processor.add_observer(prev)
                    self._execution_chain.append(processor)
                    prev = processor

    def start(self, force=False):
        """Start the runner

        Args:
            force (bool, optional): force Flush at the end of the batch source
            to cause any buffers to emit.
        """
        self._log('Runner.start', 'starting')
        # Clear any results ready for the run
        self._pipeline.clear_results()

        # The head is the first process node in the execution chain.
        # To process the source through the execution chain we add
        # each event from the input to the head.

        head = self._execution_chain.pop()
        for i in self._input.events():
            head.add_event(i)

        # The runner indicates that it is finished with the bounded
        # data by sending a flush() call down the chain. If force is
        # set to false (the default) this is never called.

        if force is True:
            self._log('Runner.start', 'flushing')
            head.flush()


def default_callback(*args):  # pylint: disable=unused-argument
    """Default no-op callback for group_by in the Pipeline constructor."""
    return None


class Pipeline(PypondBase):  # pylint: disable=too-many-public-methods
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
    """

    def __init__(self, arg=None):
        """New pipeline."""
        super(Pipeline, self).__init__()

        # sorry pylint, that's just how it goes sometimes
        # pylint: disable=invalid-name, protected-access

        self._log('Pipeline.init')

        if isinstance(arg, Pipeline):
            self._d = arg._d
        elif is_pmap(arg):
            self._d = arg
        else:
            self._d = freeze(
                dict(
                    type=None,
                    input=None,  # renamed from 'in' in the JS source
                    first=None,
                    last=None,
                    group_by=default_callback,
                    window_type='global',
                    window_duration=None,
                    emit_on='eachEvent'
                )
            )

        self._results = list()
        self._results_done = False

    # Accessors to the current Pipeline state

    def input(self):
        """Originally called in() in JS code."""
        return self._d.get('in')

    def mode(self):
        """Get the pipeline mode (ie: batch, stream).

        Returns
        -------
        str
            The mode.
        """
        return self._d.get('mode')

    def first(self):
        """Get the first processor

        Returns
        -------
        Processor
            An pipeline processor.
        """
        return self._d.get('first')

    def last(self):
        """Get the last processor

        Returns
        -------
        Processor
            An pipeline processor.
        """
        return self._d.get('last')

    def get_window_type(self):
        """Get the window type (global, etc).

        Returns
        -------
        str
            The window type.
        """
        return self._d.get('window_type')

    def get_window_duration(self):
        """Get the window duration.

        Returns
        -------
        str
            A formatted window duration.
        """
        return self._d.get('window_duration')

    def get_group_by(self):
        """Get the group by callback.

        Returns
        -------
        function
            Returns the group by function.
        """
        return self._d.get('group_by')

    def get_emit_on(self):
        """Get the emit on (eachEvent, etc).

        Returns
        -------
        str
            Description
        """
        return self._d.get('emit_on')

    # Results

    def clear_results(self):
        """Clear the result state of this Pipeline instance."""
        self._results = None
        self._results_done = False

    def add_result(self, arg1, arg2):
        """Add the incoming result from the processor callback.

        Parameters
        ----------
        arg1 : str
            Collection key string.
        arg2 : Collection or str
            Generally the incoming collection.
        """
        if self._results is None:
            if isinstance(arg1, str) and arg2 is not None:
                self._results = dict()
            else:
                self._results = list()  # pylint: disable=redefined-variable-type

        if isinstance(arg1, str) and arg2 is not None:
            self._results[arg1] = arg2
        else:
            self._results.append(arg1)

        self._results_done = False

    def results_done(self):
        """Set result state as done."""
        self._results_done = True

    #
    # Pipeline mutations
    #

    def _set_in(self, pipe_in):
        """
        Setting the In for the Pipeline returns a new Pipeline.
        """
        self._log('Pipeline._set_in', 'in: {0}'.format(pipe_in))

        mode = None
        source = pipe_in

        if isinstance(pipe_in, TimeSeries):
            mode = 'batch'
            source = pipe_in.collection()
        elif isinstance(pipe_in, BoundedIn):
            mode = 'batch'
        elif isinstance(pipe_in, UnboundedIn):
            mode = 'stream'
        else:
            msg = 'Unknown input type'
            raise PipelineException(msg)

        new_d = self._d.update({'in': source, 'mode': mode})

        return Pipeline(new_d)

    def _set_first(self, i):
        """
        Set the first processing node pointed to, returning
        a new Pipeline. The original pipeline will still point
        to its orginal processing node.
        """
        raise NotImplementedError

    def _set_last(self, i):
        """
        Set the last processing node pointed to, returning
        a new Pipeline. The original pipeline will still point
        to its orginal processing node.
        """
        raise NotImplementedError

    def _append(self, processor):

        self._log('Pipeline._append', 'processor: {0}'.format(processor))

        first = self.first()
        last = self.last()

        if first is None:
            first = processor
        if last is not None:
            last.add_observer(processor)

        last = processor

        new_d = self._d.update({'first': first, 'last': last})

        return Pipeline(new_d)

    # Pipeline state chained methods

    def window_by(self, win):
        """
        Set the window, returning a new Pipeline. The argument here
        is an object with {type, duration}.

        Window *win* may be:

        * A fixed interval: "fixed"
        * A calendar interval: "day," "month" or "year"

        Duration is of the form:

        * "30s" or "1d" etc - supports seconds (s), minutes (m), hours (h),
          days (d)

        Parameters
        ----------
        win : string
            See above.

        Returns
        -------
        Pipeline
            The Pipeline.
        """
        raise NotImplementedError

    def clear_window(self):
        """
        Remove windowing from the Pipeline. This will
        return the pipeline to no window grouping. This is
        useful if you have first done some aggregated by
        some window size and then wish to collect together
        the all resulting events.

        Returns
        -------
        Pipeline
            The Pipeline
        """
        raise NotImplementedError

    def group_by(self, key='value'):
        """
        Sets a new groupBy expression. Returns a new Pipeline.

        Grouping is a state set on the Pipeline. Operations downstream
        of the group specification will use that state. For example, an
        aggregation would occur over any grouping specified.


        Parameters
        ----------
        key : function, list or string
            The key to group by. You can group_by using a function,
            a field_spec (field name or dot.delimited.path) or an array
            of field_specs

        Returns
        -------
        Pipeline
            The Pipeline
        """

        grp = None

        if callable(key):
            grp = key
        elif isinstance(key, (str, list)):
            def get_callback(event):
                """gb a column value."""
                return event.get(key)
            grp = get_callback  # pylint: disable=redefined-variable-type
        else:
            grp = default_callback

        new_d = self._d.update(dict(group_by=grp))

        return Pipeline(new_d)

    def clear_group_by(self):
        """
        Remove the grouping from the pipeline. In other words
        recombine the events.

        Returns
        -------
        Pipeline
            The Pipeline
        """
        raise NotImplementedError

    def emit_on(self, trigger):
        """
        Sets the condition under which an accumulated collection will
        be emitted. If specified before an aggregation this will control
        when the resulting event will be emitted relative to the
        window accumulation. Current options are:

        * to emit on every event, or
        * just when the collection is complete, or
        * when a flush signal is received, either manually calling done(),
          or at the end of a bounded source.

        The strings indicating how to trigger how a Collection should
        be emitted - can be:

        * "eachEvent" - when a new event comes in, all currently maintained
          collections will emit their result.
        * "discard" - when a collection is to be discarded, first it will
          emit. But only then.
        * "flush" - when a flush signal is received.

        The difference will depend on the output you want, how often
        you want to get updated, and if you need to get a partial state.
        There's currently no support for late data or watermarks. If an
        event passes comes in after a collection window, that collection
        is considered finished.


        Parameters
        ----------
        trigger : string
            See above

        Returns
        -------
        Pipeline
            The Pipeline
        """
        new_d = self._d.set('emit_on', trigger)
        return Pipeline(new_d)

    # I/O

    def from_source(self, src):
        """
        Note: originally named from() in JS code.

        The source to get events from. The source needs to be able to
        iterate its events using `for..of` loop for bounded Ins, or
        be able to emit() for unbounded Ins. The actual batch, or stream
        connection occurs when an output is defined with `to()`.

        Pipelines can be chained together since a source may be another
        Pipeline.


        Parameters
        ----------
        src : BoundedIn, UnboundedIn or Pipeline
            The source for the Pipeline, or another Pipeline.

        Returns
        -------
        Pipeline
            The Pipeline.
        """
        self._log('Pipeline.from_source', 'called with: {0}'.format(src))

        if isinstance(src, Pipeline):
            pipeline_in = src.input()
            return self._set_in(pipeline_in)
        elif isinstance(src, (BoundedIn, UnboundedIn, TimeSeries)):
            return self._set_in(src)
        else:
            msg = 'from_source() only takes Pipeline, BoundedIn or UnboundedIn'
            raise PipelineException(msg)

    def to_event_list(self):
        """Directly return the results from the processor rather than
        passing a callback in.

        Returns
        -------
        list or dict
            Returns the _results attribute with events.
        """
        raise NotImplementedError

    def to_keyed_collections(self):
        """Directly return the results from the processor rather than
        passing a callback in.

        Returns
        -------
        list or dict
            Returns the _results attribute from a Pipeline object after processing.
            Will contain Collection objects.
        """
        return self.to(CollectionOut)

    def to(self, out, observer=None, options=Options()):  # pylint: disable=invalid-name
        """
        Sets up the destination sink for the pipeline.

        For a batch mode connection, i.e. one with a Bounded source,
        the output is connected to a clone of the parts of the Pipeline dependencies
        that lead to this output. This is done by a Runner. The source input is
        then iterated over to process all events into the pipeline and though to the Out.

        For stream mode connections, the output is connected and from then on
        any events added to the input will be processed down the pipeline to
        the out.

        ::

            p = Pipeline()

            XXX FLESH OUT EXAMPLE

        NOTE: arg list has been changed from the ordering in the JS source
        to conform to python convention.

        Parameters
        ----------
        out : EventOut, CollectionOut, etc instance
            The output.
        observer : function or instance
            The observer.
        options : Options, optional
            Options.

        Returns
        -------
        Pipeline
            The Pipeline.
        """

        self._log(
            'Pipeline.to',
            'out: {0}, obs: {1}, opt: {2} mode: {3}'.format(
                out, observer, options.to_dict(), self.mode())
        )

        Out = out  # pylint: disable=invalid-name

        if self.input() is None:
            msg = 'Tried to eval pipeline without a In. Missing from() in chain?'
            raise PipelineException(msg)

        out = Out(self, observer, options)

        if self.mode() == 'batch':
            runner = Runner(self, out)
            runner.start(True)
            if self._results_done and observer is None:
                return self._results

        elif self.mode() == 'stream':
            out = Out(self, observer, options)

            if self.first():
                self.input().add_observer(self.first())

            if self.last():
                self.last().add_observer(out)
            else:
                self.input().add_observer(out)

        return self

    def count(self, observer, force=True):
        """
        Outputs the count of events.

        Parameters
        ----------
        observer : function
            The callback function. This will be passed the count, the window_key
            and the group_by_key.
        force : bool, optional
            Flush at the end of processing batch events, output again with possibly
            partial result

        Returns
        -------
        Pipeline
            The Pipeline.
        """
        raise NotImplementedError

    def offset_by(self, offset_by, field_spec='value'):
        """
        Processor to offset a set of fields by a value. Mostly used for
        testing processor and pipeline operations with a simple operation.

        Parameters
        ----------
        offset_by : int, float
            The amout to offset by.
        field_spec : string, list
            The fields

        Returns
        -------
        Pipeline
            The modified Pipeline.
        """

        self._log('Pipeline.offset_by', 'offset: {0}'.format(offset_by))

        offset = Offset(
            self,
            Options(
                by=offset_by,
                field_spec=field_spec,
                prev=self.last() if self.last() else self
            )
        )

        return self._append(offset)

    def aggregate(self, fields):
        """
        Uses the current Pipeline windowing and grouping
        state to build collections of events and aggregate them.

        IndexedEvents will be emitted out of the aggregator based
        on the `emitOn` state of the Pipeline.

        To specify what part of the incoming events should
        be aggregated together you specify a `fields`
        object. This is a map from fieldName to operator.

        ::

            XXX FLESH OUT EXAMPLE


        Parameters
        ----------
        fields : instance
            Fields and operators to be aggregated

        Returns
        -------
        Pipeline
            The Pipeline
        """
        raise NotImplementedError

    def as_events(self, options):
        """
        Converts incoming TimeRangeEvents or IndexedEvents to
        Events. This is helpful since some processors will
        emit TimeRangeEvents or IndexedEvents, which may be
        unsuitable for some applications.

        There are three options:

        1. use the beginning time (options = Options(alignment='lag')
        2. use the center time (options = Options(alignment='center')
        3. use the end time (options = Options(alignment='lead')

        Parameters
        ----------
        options : Options
            The options, see above.

        Returns
        -------
        Pipeline
            The Pipeline.
        """
        raise NotImplementedError

    def _chain_last(self):
        """Get the operative last for the processors

        Returns
        -------
        Pipeline
            Returns either self.last() or self
        """
        return self.last() if self.last() is not None else self

    def map(self, op):  # pylint: disable=invalid-name
        """
        Map the event stream using an operator.


        Parameters
        ----------
        op : function
            A function that returns a new Event.

        Returns
        -------
        Pipeline
            The Pipeline.
        """
        res = Mapper(self, Options(op=op, prev=self._chain_last()))

        return self._append(res)

    def filter(self, op):  # pylint: disable=invalid-name
        """
        Filter the event stream using an operator

        Parameters
        ----------
        op : function
            A function that returns True or False

        Returns
        -------
        Pipeline
            The Pipeline
        """
        flt = Filter(
            self,
            Options(
                op=op,
                prev=self._chain_last(),
            )
        )

        return self._append(flt)

    def select(self, field_spec):
        """
        Select a subset of columns.

        Parameters
        ----------
        field_spec : list, string
            The columns to include in the output.

        Returns
        -------
        Pipeline
            The Pipeline.
        """

        sel = Selector(
            self,
            Options(
                field_spec=field_spec,
                prev=self._chain_last(),
            )
        )

        return self._append(sel)

    def collapse(self, field_spec, name, reducer, append=True):
        """
        Collapse a subset of columns using a reducer function.


        Parameters
        ----------
        field_spec : list, string
            The columns to collapse into the output.
        name : string
            The resulting output column's name.
        reducer : function
            Function to use to do the reduction.
        append : bool
            Add the new column to the existing ones, or replace them.

        Returns
        -------
        Pipeline
            The Pipeline.
        """

        coll = Collapser(
            self,
            Options(
                field_spec=field_spec,
                name=name,
                reducer=reducer,
                append=append,
                prev=self._chain_last(),
            )
        )

        return self._append(coll)

    def take(self, limit):
        """
        Take events up to the supplied limit, per key.

        Parameters
        ----------
        limit : int
            Integer number of events to take.

        Returns
        -------
        Pipeline
            The Pipeline.
        """

        take = Taker(
            self,
            Options(
                limit=limit,
                prev=self._chain_last(),
            )
        )

        return self._append(take)

    def as_time_range_events(self, options):
        """
        Converts incoming Events or IndexedEvents to TimeRangeEvents.

        There are three option for alignment:

        1. time range will be in front of the timestamp - ie:
           options = Options(alignment='front')
        2. time range will be centered on the timestamp - ie:
           options = Options(alignment='center')
        3. time range will be positoned behind the timestamp - ie:
           options = Options(alignment='behind')

        The duration is of the form "1h" for one hour, "30s" for 30 seconds and so on.

        Parameters
        ----------
        options : Options
            Options - see above.

        Returns
        -------
        Pipeline
            The Pipeline
        """
        raise NotImplementedError

    def as_indexed_events(self, options):
        """
        Converts incoming Events to IndexedEvents.

        Note: It isn't possible to convert TimeRangeEvents to IndexedEvents.


        Parameters
        ----------
        options : Options
            Contains the conversion options. In this case, the duration string
            of the Index is expected. Must contain the key 'duration' and the
            duration string is of the form "1h" for one hour, "30s" for 30
            seconds and so on.

        Returns
        -------
        TYPE
            Description

        Raises
        ------
        NotImplementedError
            Description


        """
        raise NotImplementedError

# module functions

# def pipeline(args):
#     return Pipeline(args)


def is_pipeline(pline):
    """Boolean test to see if something is a Pipeline instance."""
    return isinstance(pline, Pipeline)
