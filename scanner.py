#!/usr/bin/env python3

import socket
import asyncore
import queue
import logging
from datetime import datetime, timedelta
from ipaddress import IPv4Network
from ftplib import FTP, FTP_PORT

DEFAULT_TIMEOUT = 5
DEFAULT_FILE_LIMIT = 800

logger = logging.getLogger(__name__)

def reverse_ip(ip):
    try:
        return socket.gethostbyaddr(ip)[0]
    except socket.herror:
        return ip

# Used to count available file descriptors
class _Counter(object):
    def __init__(self, init):
        self.value = init

    def inc(self):
        self.value += 1

    def dec(self):
        self.value -= 1

# If the connection is successful, the host's IP address is put onto
# 'detected_hosts'. 'slots' is incremented before socket creation and
# decremented after it is closed.
class _Probe(asyncore.dispatcher):
    def __init__(self, host, detected_hosts, slots):
        self.host = host
        asyncore.dispatcher.__init__(self)
        self.slots = slots
        slots.dec()
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.detected_hosts = detected_hosts
        self.connect((host, FTP_PORT))

    def handle_connect(self):
        logger.debug('Connected to {}'.format(self.host))
        self.detected_hosts.put(self.host)
        self.close()
        self.slots.inc()

    def handle_close(self):
        self.close()
        self.slots.inc()

    def handle_error(self):
        self.close()
        self.slots.inc()

class FTP_Scanner(object):
    """Search a network for FTP servers."""

    def __init__(self, login, passwd, timeout=DEFAULT_TIMEOUT,
                                      file_limit=DEFAULT_FILE_LIMIT):
        self.login = login
        self.passwd = passwd
        self.timeout = timeout
        self.file_limit = file_limit
        self.port21_hosts = queue.Queue()
        self.active_probes = queue.Queue(maxsize=50)

    def ftp_iter(self, network):
        slots = _Counter(self.file_limit)

        logger.info('Begin scanning {}'.format(settings.NETWORK))
        for ip in IPv4Network(network).hosts():
            if slots.value == 0:
                self._process_probes()
            else:
                assert(slots.value > 0) # no problem because no concurrency
                try:
                    _Probe(str(ip), self.port21_hosts, slots)
                except OSError:
                    logger.error('Error probing {}. Maybe too many open files.'.format(ip))
        self._process_probes()
        logger.info('End scanning {}'.format(settings.NETWORK))

        while not self.port21_hosts.empty():
            host = self.port21_hosts.get()
            if self._has_ftp(host):
                yield (reverse_ip(host), datetime.utcnow())

    def _process_probes(self):
        start = datetime.utcnow()
        while asyncore.socket_map:
            asyncore.loop(timeout=self.timeout, count=1, use_poll=True)
            too_long = datetime.utcnow() - start > timedelta(seconds=self.timeout)
            if too_long: break

    def _has_ftp(self, ip):
        try:
            ftp = FTP(ip, timeout=self.timeout)
            ftp.login(self.login, self.passwd)
        except:
            return False
        return True

if __name__ == '__main__':
    import settings
    import sqlite3
    import logging.config

    logging.config.dictConfig(settings.SCAN_LOGGING)
    start_time = datetime.utcnow()
    ftp_db = sqlite3.connect(settings.FTP_DB)
    ftp_db.execute('create table if not exists ftp (host text, last_updated date)')
    scanner = FTP_Scanner(settings.FTP_USER, settings.FTP_PASSWD)
    ftp_iter = scanner.ftp_iter(settings.NETWORK)
    with ftp_db:
        ftp_db.executemany('insert or replace into ftp values (?, ?)', ftp_iter)
        ftp_db.execute('delete from ftp where last_updated < ?', (start_time,))
