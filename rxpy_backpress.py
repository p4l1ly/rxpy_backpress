import asyncio
import sys
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, List, Deque

import rx
from rx.core.typing import Observer


@dataclass
class Backpress:
    observers: List[Observer] = field(init=False, default_factory=list)

    def scheduled_send(self, observer, scheduler, x):
        def x_for_observer_(_scheduler, _state):
            self.send(observer, scheduler, x)
        return x_for_observer_

    def send(self, observer, scheduler, x):
        observer.on_next((x, lambda: self.subscribe(observer, scheduler)))

    def send_or_schedule(self, observer, scheduler, x):
        if scheduler is None:
            self.send(observer, scheduler, x)
        else:
            scheduler.schedule(self.scheduled_send(observer, scheduler, x))

    def on_next(self, x):
        if not self.observers:
            self.handle_no_observers(x)
        else:
            old_observers = self.observers
            self.observers = []

            for observer, scheduler in old_observers:
                self.send_or_schedule(observer, scheduler, x)

    def on_error(self, e):
        if self.observers:
            for observer, _ in self.observers:
                observer.on_error(e)
        else:
            print(e, file=sys.stderr, flush=True)

    def __call__(self, source):
        source.subscribe(on_next=self.on_next, on_error=self.on_error)
        return rx.create(self.subscribe)

    def subscribe(self, observer, scheduler):
        if self.subscribe_hook(observer, scheduler):
            self.observers.append((observer, scheduler))

    def subscribe_hook(self, observer, scheduler):
        return True

    def handle_no_observers(self, x):
        pass


@dataclass
class BackpressBuffered(Backpress):
    buffer: Deque[Any] = field(init=False, default_factory=deque)

    def subscribe_hook(self, observer, scheduler):
        if not self.buffer:
            return True

        x = self.buffer.popleft()
        self.send_or_schedule(observer, scheduler, x)
        return False

    def handle_no_observers(self, x):
        self.buffer.append(x)


@dataclass
class BackpressBufferedDropNew(BackpressBuffered):
    limit: int

    def handle_no_observers(self, x):
        if len(self.buffer) < self.limit:
            self.buffer.append(x)


class BackpressBufferedDropOld(BackpressBufferedDropNew):
    def handle_no_observers(self, x):
        if len(self.buffer) >= self.limit:
            self.buffer.popleft()
        self.buffer.append(x)


@dataclass
class GiveUp:
    flag: bool = False

    def announce(self):
        self.flag = True

    def __bool__(self):
        return self.flag


@dataclass
class BackpressCowardlyBuffered(BackpressBufferedDropNew):
    give_up: GiveUp = field(init=False, default_factory=GiveUp)
    notify_capitulation: Callable[[], None]

    def send(self, observer, scheduler, x):
        super().send(observer, scheduler, (x, self.give_up))

    def handle_no_observers(self, x):
        if len(self.buffer) >= self.limit:
            self.give_up.announce()
            self.notify_capitulation()
            self.buffer.clear()
            print("backpress gave up", file=sys.stderr, flush=True)

        else:
            self.buffer.append(x)


def wrap_aio(coroutine_fn):
    def wrap_aio_(x):
        async def wrap_aio__():
            result = await coroutine_fn(x[0])
            x[1]()
            return result

        return rx.from_future(asyncio.create_task(wrap_aio__()))

    return wrap_aio_
