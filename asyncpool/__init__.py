#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: __init__.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: none
Description:
    A Pool of threads running functions iterating through a long list
"""

from threading import Thread
from threading import current_thread
from threading import Event
from Queue import Queue
import logging

LOGGER = logging.getLogger(__name__)


class Signal(object):
    """ An signal carrier
    """
    def __init__(self, msg):
        super(Signal, self).__init__()
        self.msg = msg


class NoMoreArg(Signal):
    """ No more argument would available in the future
    """
    def __init__(self):
        super(NoMoreArg, self).__init__('No More Arguments')


class Interrupted(Signal):
    """ Queue got interrupted and may return with None for get()
    """
    def __init__(self):
        super(Interrupted, self).__init__('Got Interrupted')


class BaseWorker(object):
    """ Abstract class for base
    """
    def __init__(self):
        super(BaseWorker, self).__init__()

    def process(self, argpack):
        """ Processing the given argpack
        """
        raise NotImplementedError


class FunctionWorker(BaseWorker):
    """ A worker class for processing
    """
    def __init__(self, func=None):
        super(FunctionWorker, self).__init__()
        self.func = func

    def process(self, argpack):
        """ Working on a pack of args
        """
        if argpack is not None and len(argpack) > 0:
            respack = list()
            for arg in argpack:
                res = self.func(arg)
                respack.append(res)
            return respack


class AsyncPool(object):
    """ Asyncronized pool
    """
    def __init__(self, poolsize, debug=False):
        super(AsyncPool, self).__init__()
        self.poolsize = poolsize
        self.pool = dict()
        self.inputQueue = Queue(maxsize=self.poolsize * 2)
        self.outputQueue = Queue(maxsize=self.poolsize * 10)
        self._worker_class = None
        self._worker_initargs = None
        self._stopflag = Event()
        self.feeder = None
        if debug:
            LOGGER.setLevel(logging.DEBUG)

    def asyncmap(self, args_iter, initargs,
                 worker_class=FunctionWorker, chunksize=100):
        """ Iterating through args in args_iter
        """
        self._worker_class = worker_class
        self._worker_initargs = initargs
        self.feeder = Thread(target=self._argFeeder,
                             name='_argFeeder',
                             args=(args_iter, chunksize))
        for idx in range(self.poolsize):
            n = 'WorkerThread-%d' % (idx, )
            self.pool[n] = Thread(
                target=self._worker_driver,
                args=(worker_class, initargs),
                name=n)

        _ = [p.start() for p in self.pool]
        self.feeder.start()
        return self._resultCollector()

    def _populate(self):
        """ Check whether there are insufficient number of
            workers in the pool. If so, populate the pool
            with new workers.
        """
        for name in self.pool.iterkeys():
            if not self.pool[name].isAlive():
                del self.pool[name]
                self.pool[name] = Thread(
                    target=self._worker_driver,
                    args=(self._worker_class, self._worker_initargs),
                    name=name)

    def _worker_driver(self, worker_class, initargs):
        """ Fetching the args and running the function
        """
        LOGGER.debug('[START] ' + current_thread().name)
        worker = worker_class(*initargs)

        while not self._stopflag.is_set():
            arg = self.inputQueue.get()
            LOGGER.debug(current_thread().name + ' get ' + str(arg))
            if isinstance(arg, NoMoreArg):
                break
            res = worker.process(arg)
            self.outputQueue.put(res)
        self.outputQueue.put(NoMoreArg())
        LOGGER.debug('[FINISH] ' + current_thread().name)

    def _argFeeder(self, args_iter, chunksize):
        """ Feeding the queue either with
            args or list of args
        """
        LOGGER.debug('[START] Feeder')
        if chunksize == 1:
            for arg in args_iter:
                self.inputQueue.put(arg)
        else:
            assert chunksize > 1
            chunk = list()
            for arg in args_iter:
                chunk.append(arg)
                if len(chunk) >= chunksize:
                    LOGGER.debug('Feeder put ' + str(chunk))
                    self.inputQueue.put(chunk)
                    chunk = list()
            if len(chunk) > 0:
                self.inputQueue.put(chunk)
        for _ in range(self.poolsize):
            self.inputQueue.put(NoMoreArg())
        LOGGER.debug('[FINISH] Feeder')

    def _resultCollector(self):
        """ Collecting results
        """
        cnt = self.poolsize
        while cnt > 0:
            res = self.outputQueue.get()
            if isinstance(res, NoMoreArg):
                cnt -= 1
                continue
            yield res

    def _argDumper(self, argDump):
        """ Dump Arg to a file for later use
        """
        LOGGER.debug('[START] ' + current_thread().name)
        while True:
            arg = self.inputQueue.get()
            if isinstance(arg, NoMoreArg):
                # return the signal object to argQueue
                self.inputQueue.put(arg)
                break
            argDump(arg)
        LOGGER.debug('[FINISH] ' + current_thread().name)

    def stop(self, argDump=lambda x: None):
        """ Stop all the pool workers and dump all unprocessed args
            with writeArg(.)
        """
        self._stopflag.set()
        dumper = Thread(target=self._argDumper,
                        args=(argDump, ))
        dumper.start()
        dumper.join()
        self.join()

    def join(self):
        """ Stop the threads
        """
        _ = [p.join() for p in self.pool]
        self.feeder.join()
