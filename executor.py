#!/usr/bin/env python3
# encoding=utf-8

# AUTHOR: heroesm
# LICENCE: MIT

from typing import Coroutine, Callable, Sequence, Mapping

import asyncio
import logging
from collections import namedtuple

log = logging.getLogger(__name__)

Call = namedtuple('Call', ('func', 'args', 'kwargs'))


class Work():

    def __init__(
        self,
        coroutine: Coroutine = None,
        func: Callable = None,
        args: Sequence = None,
        kwargs: Mapping = None,
        loop = None,
    ) -> None:
        assert not (coroutine and func)

        if (asyncio.iscoroutine(coroutine)):
            self.coroutine = coroutine
        elif (asyncio.iscoroutinefunction(func)):
            args = args or []
            kwargs = kwargs or {}
            self.coroutine = func(*args, **kwargs)
        else:
            raise TypeError('coroutine or coroutine function should be passed in')

        self.loop = loop or asyncio.get_event_loop()

        self.task = None  # type: asyncio.Task
        self.done_future = self.loop.create_future()

    def make_task(
        self,
        callback: Callable = None
    ):
        assert self.coroutine and not self.task
        self.task = self.loop.create_task(self.coroutine)

        def done_callback(task):
            assert task is self.task and task.done()
            try:
                result = self.task.result()
            except BaseException as ex:
                self.done_future.set_exception(ex)
            else:
                self.done_future.set_result(result)

            if (callable(callback)):
                callback(self)

        self.task.add_done_callback(done_callback)


class CoroutineExecutor():

    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.queue = asyncio.Queue(loop=self.loop)
        self.workload = set()

    async def _stop_loop(self, is_graceful=True):
        log.debug('stop loop')
        if (is_graceful):
            await self.join()
        self.loop.stop()

    def sync_run(self):
        self.loop.create_task(self._stop_loop())
        self.loop.run_forever()

    def submit(self, func, args=None, kwargs=None):
        # wrap coroutine funciton in coroutine and apply it 
        assert asyncio.iscoroutinefunction(func)
        coroutine = func(*args, **kwargs)
        return self.apply(coroutine)

    def apply(self, coroutine):
        # TODO: always return future or task according to coroutine
        assert asyncio.iscoroutine(coroutine)
        work = Work(coroutine=coroutine, loop=self.loop)
        self.queue.put_nowait(work)
        self.adjust_workload()
        return work.done_future

    def work_done(self, work):
        self.workload.remove(work)
        self.queue.task_done()

    def adjust_workload(self):
        try:
            work = self.queue.get_nowait()
        except asyncio.QueueEmpty as e:
            pass
        else:
            self.workload.add(work)
            work.make_task(callback=self.work_done)

    async def join(self):
        await self.queue.join()
        # if (self.workload):
        #     await asyncio.wait([work.done_future for work in self.workload], loop=self.loop)


async def afun():
    print(1)
    await asyncio.sleep(2)
    print(2)

def test():
    logging.basicConfig(level = logging.DEBUG)
    executor = CoroutineExecutor()
    #loop = asyncio.get_event_loop()
    print(executor.workload)
    executor.apply(afun())
    print(executor.workload)
    executor.sync_run()
    print(executor.workload)
    #print(list(executor.workload)[0])


if __name__ == '__main__':
    test()
