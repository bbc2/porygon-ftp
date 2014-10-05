#!/usr/bin/env python3

import asyncio
import logging
from limiter import JoinableSemaphore
from ipaddress import IPv4Address, ip_network

logger = logging.getLogger(__name__)

class Scanner():
    def __init__(self, loop, port, user, passwd, timeout=None, max_tasks=1024):
        self.loop = loop
        self.port = port
        self.user = user
        self.passwd = passwd
        self.timeout = timeout
        self.limiter = JoinableSemaphore(maxsize=max_tasks)
        self.ftp_hosts = set()

    def _ftp_send(self, writer, cmds):
        writer.write(''.join(['{}\r\n'.format(cmd) for cmd in cmds]).encode())

    @asyncio.coroutine
    def _ftp_get_code(self, line):
        # A reply is defined to contain the 3-digit code, followed by Space
        # <SP>, followed by one line of text (where some maximum line length
        # has been specified), and terminated by the Telnet end-of-line code.
        # [...] the format for multi-line replies is that the first line will
        # begin with the exact required reply code, followed immediately by a
        # Hyphen, "-" (also known as Minus), followed by text.
        return line[:3]

    @asyncio.coroutine
    def _connect_ftp(self, reader, writer):
        self._ftp_send(writer, ['user {}'.format(self.user),
                                'pass {}'.format(self.passwd)])
        success = False
        (host, port) = writer.get_extra_info('peername')
        logger.info('Contacting (%s, %d)', host, port)
        while True:
            line = (yield from reader.readline()).decode().strip()
            code = yield from self._ftp_get_code(line)
            if code == '230': # User logged in, proceed.
                success = True
                logger.info('Login successful on %s:%d', host, port)
                break
            elif code[0] == '5': # Permanent Negative Completion reply
                logger.info('FTP error on %s:%d: %s', host, port, line)
                break

        self._ftp_send(writer, ['quit'])
        yield from writer.drain()
        logger.info('Disconnected from %s:%d', host, port)
        return success

    @asyncio.coroutine
    def _has_ftp(self, ip):
        try:
            (reader, writer) = yield from asyncio.open_connection(
                    host=str(ip), port=self.port)
            return (yield from self._connect_ftp(reader, writer))
        except OSError as exc:
            logger.debug('Connection refused on %s: %r', ip, exc)
            return False
        except asyncio.CancelledError as exc:
            logger.debug('Timeout on %s: %r', ip, exc)
            return False
        except Exception as exc:
            logger.error('Error with %s: %r', ip, exc)
            return False

    @asyncio.coroutine
    def _scan_port(self, ip):
        has_ftp = yield from self._has_ftp(ip)
        logger.debug('Probed %s, has_ftp: %s', ip, has_ftp)
        if has_ftp: self.ftp_hosts.add(str(ip))
        yield from self.limiter.release()

    def cancel_slow_task(self, task):
        if task.cancel():
            self.limiter.release()

    @asyncio.coroutine
    def scan(self, network):
        logger.info('Begin scan of %s', network)
        hosts = ip_network(network).hosts()
        for ip in hosts:
            yield from self.limiter.acquire()
            task = asyncio.Task(self._scan_port(ip))
            if self.timeout is not None:
                self.loop.call_later(self.timeout, self.cancel_slow_task, task)
            logger.debug('Scheduled: %s', ip)
        yield from self.limiter.join() # wait for all tasks to finish
        logger.info('Finished scan of %s, found: %s', network, self.ftp_hosts)
        return self.ftp_hosts

if __name__ == '__main__':
    # Testing
    import sys
    import local_settings as conf
    import logging.config
    network = sys.argv[1]
    logging.config.dictConfig(conf.LOGGING)

    loop = asyncio.get_event_loop()
    scanner = Scanner(loop, port=conf.PORT, user=conf.USER, passwd=conf.PASSWD,
                      timeout=conf.SCAN_TIMEOUT, max_tasks=conf.MAX_SCAN_TASKS)
    ftps = loop.run_until_complete(scanner.scan(network))
    loop.close()
    logger.info('Open FTPs: %s', ', '.join([str(ip) for ip in ftps]))
