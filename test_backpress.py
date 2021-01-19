import asyncio

import pytest
import rx
from rx import operators as ops
from rx.scheduler.eventloop import AsyncIOScheduler

import rxpy_backpress


@pytest.mark.asyncio
async def test_buffered_aio():
    sched = AsyncIOScheduler(asyncio.get_running_loop())

    events = []
    outs = []

    @rxpy_backpress.wrap_aio
    async def block(x):
        events.append(x)
        await asyncio.sleep(0.5)
        return x

    rx.interval(0.05, scheduler=sched).pipe(
        rxpy_backpress.BackpressBuffered(),
        ops.flat_map(block)
    ).subscribe(on_next=outs.append, scheduler=sched)

    await asyncio.sleep(1.1)

    assert events == [0, 1, 2]
    assert outs == [0, 1]
