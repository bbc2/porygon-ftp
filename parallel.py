#!/usr/bin/env python

from multiprocessing import Process, Queue

class Parallel(object):
    def __init__(self, task, gen, process_count=2, buffer_size=64):
        self._task = task
        self.in_queue = Queue(buffer_size)
        self.out_queue = Queue()
        self._task_gen = Process(target=self._put_inputs, args=(gen,))
        self._processes = [Process(target=self._process, args=(id,)) for id in range(process_count)]

    def start(self):
        self._task_gen.start()
        for p in self._processes:
            p.start()

    def results(self):
        sentinel_left = len(self._processes)
        while sentinel_left != 0:
            output = self.out_queue.get()
            if output is None:
                sentinel_left -= 1
            else:
                yield output

    def _put_inputs(self, gen):
        for input in gen():
            self.in_queue.put(input)
        self.in_queue.put(None)

    def _process(self, id):
        while True:
            input = self.in_queue.get()
            if input is None:
                self.in_queue.put(None)
                self.out_queue.put(None)
                break
            try:
                output = self._task(input)
            except Exception as e:
                output = (input, e)
            self.out_queue.put(output)

if __name__ == '__main__':
    import time

    def input_gen():
        return xrange(-5, 5)

    def inverse_100(input):
        print('processed: %s' % input)
        time.sleep(.5)
        return (input, 100. / input)

    inverses = Parallel(inverse_100, input_gen, process_count=4)
    inverses.start()
    for result in inverses.results():
        print('Result (or exception): %s -> %r' % result)
