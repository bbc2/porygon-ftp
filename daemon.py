#!/usr/bin/env python3

import signal
import asyncio
import logging
import logging.config
import functools
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

from scanner import Scanner
from walker import Walker

logger = logging.getLogger(__name__)

class Daemon():
    def __init__(self, loop, port, user, passwd, network, get_indexer, scan_interval,
                 scan_timeout, max_scans, offline_delay, index_interval, index_timeout,
                 max_index_tasks, max_index_errors):
        self.loop = loop
        self.port = port
        self.user = user
        self.passwd = passwd
        self.network = network
        self.get_indexer = get_indexer
        self.scan_interval = timedelta(seconds=scan_interval)
        self.scan_timeout = scan_timeout
        self.max_scans = max_scans
        self.offline_delay = timedelta(seconds=offline_delay)
        self.index_interval = timedelta(seconds=index_interval)
        self.index_timeout = index_timeout
        self.max_index_errors = max_index_errors
        self.executor = ThreadPoolExecutor(max_workers=max_index_tasks)
        self.scheduled = {} # handle for addresses of servers scheduled for indexation
        self.submitted = {} # future for addresses of servers about to be indexed
        self.busy = set() # addresses of servers being indexed
        self.hosts = {} # host information for recently seen servers
        self.should_stop = False

    @asyncio.coroutine
    def _scan(self):
        scanner = Scanner(loop, port=21, user=self.user, passwd=self.passwd,
                timeout=self.scan_timeout, max_tasks=self.max_scans)
        return (yield from scanner.scan(self.network))

    # Can be run in parallel for different hosts.
    def _index(self, ip):
        logger.info('Start indexation %s', ip)
        walker = Walker(ip, self.port, self.user, self.passwd, self.index_timeout,
                        self.max_index_errors, self.get_indexer(ip, self.port))
        del self.submitted[ip]
        self.busy.add(ip)
        try:
            walker.walk()
        except Exception as exc:
            logger.warn('Exception during indexation of %s: %r', ip, exc)
            return (ip, False)
        else:
            return (ip, True)

    # Called when _index has finished.
    def _indexed(self, future):
        now = datetime.now(timezone.utc)
        (ip, success) = future.result()
        info = self.hosts[ip]

        info['last_indexed'] = now
        info['last_index_success'] = success
        logger.info('Finished indexation of %s', ip)

        self.busy.remove(ip)
        if info['online'] and not self.should_stop:
            delay = self.index_interval.seconds
            self.scheduled[ip] = self.loop.call_later(delay, self._submit, ip)
            logger.debug('Scheduled subsequent indexation of %s in %d seconds', ip, delay)

    def _submit(self, ip):
        del self.scheduled[ip]
        if self.hosts[ip]['online']:
            logger.debug('Submit indexation of %s to tread pool', ip)
            future = self.executor.submit(self._index, ip)
            future.add_done_callback(self._indexed)
            self.submitted[ip] = future

    def _process(self, online_hosts):
        now = datetime.now(timezone.utc)

        # Add new hosts to the self.hosts dict and update existing ones
        for (ip, info) in self.hosts.items(): info['online'] = False
        online_attrs = { 'online': True, 'last_online': now }
        self.hosts.update({ ip: online_attrs for ip in online_hosts })

        # Forget about hosts that have been offline for too much time
        limit = now - self.offline_delay
        old = (ip for (ip, info) in self.hosts.items() if info['last_online'] < limit)
        for ip in old:
            del self.hosts[ip]
            logger.debug('Forgot about %s', ip)

        # Schedule indexation for online hosts that are not about to be indexed.
        for ip in online_hosts:
            info = self.hosts[ip]
            if ip not in self.scheduled and ip not in self.submitted \
                    and ip not in self.busy:
                try:
                    delta = (now - info['last_indexed'] + self.index_interval)
                    delay = delta.seconds
                except KeyError:
                    delay = 0
                self.scheduled[ip] = self.loop.call_later(delay, self._submit, ip)
                logger.debug('Scheduled indexation of %s in %d seconds', ip, delay)

    @asyncio.coroutine
    def _sleep(self, delta):
        if self.should_stop: return
        self.sleep = asyncio.Future()
        try:
            yield from asyncio.wait_for(self.sleep, delta.seconds)
        except asyncio.CancelledError:
            logger.info('Sleep interrupted')
        except asyncio.TimeoutError:
            pass

    @asyncio.coroutine
    def run(self):
        try:
            while not self.should_stop:
                self._process((yield from self._scan()))
                yield from self._sleep(self.scan_interval)
        finally:
            self.executor.shutdown()

    def stop(self, signame=None):
        logger.info('Received signal %s: stopping loop, this can take a while...'
                    ' Send it again to force it.', signame)
        if signame is not None:
            self.loop.remove_signal_handler(getattr(signal, signame))

        # Cancel pending tasks
        for (ip, handle) in self.scheduled.items():
            logger.debug('Cancelled handle for %s: %s', ip, handle.cancel())
        for (ip, future) in self.submitted.items():
            logger.debug('Cancelled future for %s: %s', ip, future.cancel())
        logger.debug('Still busy: %s', self.busy)

        self.should_stop = True
        if hasattr(self, 'sleep'): self.sleep.cancel()

if __name__ == '__main__':
    from printer import Printer
    import local_settings as conf

    loop = asyncio.get_event_loop()
    logging.config.dictConfig(conf.LOGGING)
    get_printer = lambda ip, port: Printer(ip, port)
    daemon = Daemon(loop, port=21, user='rez', passwd='rez', network='10.2.0.0/28',
            get_indexer=get_printer, scan_interval=conf.SCAN_INTERVAL,
            scan_timeout=conf.SCAN_TIMEOUT, max_scans=conf.MAX_SCAN_TASKS,
            offline_delay=conf.OFFLINE_DELAY, index_interval=conf.INDEX_INTERVAL,
            index_timeout=conf.INDEX_TIMEOUT, max_index_tasks=conf.MAX_INDEX_TASKS,
            max_index_errors=conf.MAX_INDEX_ERRORS)
    for name in conf.SOFT_SIGNALS:
        loop.add_signal_handler(getattr(signal, name), functools.partial(daemon.stop, name))

    logger.info('Daemon started')
    try:
        loop.run_until_complete(daemon.run())
    finally:
        loop.close()
        logger.info('Daemon stopped')
