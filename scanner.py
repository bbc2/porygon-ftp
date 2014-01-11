#!/usr/bin/env python3

from datetime import datetime
from ftplib import FTP
import socket
import parallel
from ipaddress import IPv4Network, ip_network, collapse_addresses

import settings

class FTP_Scanner(object):
    def __init__(self, login, passwd):
        self.login = login
        self.passwd = passwd
        self.scan = parallel.Parallel(self._has_ftp, self._targets, process_count=20)

    def ftp_iter(self, network):
        scan = scanner._scan(network)
        for result in scan:
            if result[1]:
                yield (result[0], datetime.utcnow())

    def _scan(self, network):
        self.cur_network = IPv4Network(network).hosts()
        self.scan.start()
        return self.scan.results()

    def _has_ftp(self, ip, timeout=5):
        try:
            ftp = FTP(ip, timeout=timeout)
            ftp.login(self.login, self.passwd)
        except:
            return (ip, False)
        return (ip, True)

    def _targets(self):
        for ip in self.cur_network:
            yield str(ip)

if __name__ == '__main__':
    import sqlite3
    start_time = datetime.utcnow()
    ftp_db = sqlite3.connect(settings.FTP_DB)
    ftp_db.execute('create table if not exists ftp (ip text, last_updated date)')
    scanner = FTP_Scanner(settings.FTP_USER, settings.FTP_PASSWD)
    ftp_iter = scanner.ftp_iter(settings.NETWORK)
    with ftp_db:
        ftp_db.executemany('insert or replace into ftp values (?, ?)', ftp_iter)
        ftp_db.execute('delete from ftp where last_updated < ?', (start_time,))
