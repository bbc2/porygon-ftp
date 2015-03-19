import asyncio

class JoinableSemaphore():
    def __init__(self, maxsize=0):
        self.count = 0
        self.limiter = asyncio.Semaphore(maxsize)
        self.empty = asyncio.Lock()

    @asyncio.coroutine
    def acquire(self):
        if self.count == 0: yield from self.empty
        self.count += 1
        yield from self.limiter.acquire()

    @asyncio.coroutine
    def release(self):
        self.count -= 1
        if self.count == 0: self.empty.release()
        self.limiter.release()

    @asyncio.coroutine
    def join(self):
        yield from self.empty
