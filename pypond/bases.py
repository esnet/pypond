#  Copyright (c) 2016, The Regents of the University of California,
#  through Lawrence Berkeley National Laboratory (subject to receipt
#  of any required approvals from the U.S. Dept. of Energy).
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

"""
Common base classes and mixins.
"""

import logging
import os
import time
import types
import warnings


def setup_log(log_path=None):  # pragma: no cover
    """
    Usage:
    _log('main.start', 'happy simple log event')
    _log('launch', 'more={0}, complex={1} log=event'.format(100, 200))
    """
    # pylint: disable=redefined-variable-type
    logger = logging.getLogger("pypond")
    if not log_path:
        handle = logging.StreamHandler()
    else:
        # it's on you to make sure log_path is valid.
        logfile = '{0}/pypond.log'.format(log_path)
        handle = logging.FileHandler(logfile)
    handle.setFormatter(logging.Formatter('ts=%(asctime)s %(message)s'))
    logger.addHandler(handle)
    logger.setLevel(logging.INFO)
    return logger


log = setup_log()  # pylint: disable=invalid-name


def _log(event, msg):  # pragma: no cover
    log.info('event=%s id=%s %s', event, int(time.time()), msg)


class PypondBase(object):  # pylint: disable=too-few-public-methods
    """
    Universal base class. Used to provide common functionality (logging, etc)
    to all the other classes.
    """
    __slots__ = ('_logger',)

    def __init__(self):
        """ctor"""

        self._logger = _log

    def _log(self, event, msg='', format_args=tuple()):  # pragma: no cover
        """Log events if environment variable PYPOND_LOG is set.

        Parameters
        ----------
        event : str
            The event - ie: 'init.start' and etc.
        msg : str
            The log message
        format_args : tuple
            The args to format. This is to keep objects from being stringified
            in production which is a performance drag.
        """
        if 'PYPOND_LOG' in os.environ:
            self._logger(event, msg.format(*format_args))

    def _warn(self, msg, warn_type):  # pylint: disable=no-self-use
        """Issue a python warning.

        Parameters
        ----------
        msg : str
            The warning message
        warn_type : Exception subclass
            Custom warning from pypond.exceptions.
        """
        warnings.warn(msg, warn_type, stacklevel=2)

    @staticmethod
    def _field_path_to_array(fspec):
        """Split the field spec if it is not already a list.

        Also, allow for deep fields to be passed in as a tuple because
        it will need to be used as a dict key in some of the processor
        Options.

        This is deployed in Event.get() to process anything passed
        to it, but this should also be deployed "upstream" to avoid
        having that split() done over and over in a loop.
        """

        if isinstance(fspec, list) or isinstance(fspec, types.FunctionType):
            # corner case, but Event.map() takes field_spec as a
            # function, so let that pass through just in case.
            return fspec
        elif isinstance(fspec, tuple):
            return list(fspec)
        elif isinstance(fspec, str):
            return fspec.split('.')

        if fspec is None:
            return ['value']


# base classes for pipeline sources, etc


class Observable(PypondBase):
    """
     Base class for objects in the processing chain which
     need other object to listen to them. It provides a basic
     interface to define the relationships and to emit events
     to the interested observers.
    """

    def __init__(self):
        super(Observable, self).__init__()

        self._observers = list()

    def emit(self, event):
        """add event to observers."""
        for i in self._observers:
            i.add_event(event)

    def flush(self):
        """flush observers."""
        self._log('Observable.flush')
        for i in self._observers:
            if hasattr(i, 'flush'):
                self._log('Observable.flush')
                i.flush()

    def add_observer(self, observer):
        """add an observer if it does not already exist."""
        self._log('Observable.add_observer', 'obs: {0}', (observer,))
        should_add = True

        for i in self._observers:
            if i == observer:
                should_add = False

        if should_add:
            self._observers.append(observer)

    def has_observers(self):
        """does the object have observers?"""
        return bool(len(self._observers) > 0)
