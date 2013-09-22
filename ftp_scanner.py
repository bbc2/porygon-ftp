#!/usr/bin/env python2

from ftplib import FTP
import socket
import parallel
from ipaddress import IPv4Network, ip_network

class FTP_Scanner(object):
    def __init__(self, login, passwd):
        self.login = login
        self.passwd = passwd
        self.open_ports = parallel.Parallel(self._has_ftp, self._targets, process_count=20)

    def scan(self, network):
        self.cur_network = IPv4Network(unicode(network, 'utf-8')).hosts()
        self.open_ports.start()
        return self.open_ports.results()

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
    scanner = FTP_Scanner('rez', '35zero')
    ftps = scanner.scan('10.0.0.0/24')
    for ftp in ftps:
        if ftp[1]:
            print(ftp[0])
