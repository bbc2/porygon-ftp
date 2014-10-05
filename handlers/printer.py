import os
import os.path
import logging

class Printer():
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.logger = logging.getLogger('Printer({})'.format(ip))
        self.i = 0

    def __enter__(self):
        pass

    def handle(self, files):
        for (path, name) in files:
            if self.i % 500 == 0: self.logger.debug('i=%d', self.i)
            self.i += 1

    def __exit__(self, type, value, tb):
        pass
