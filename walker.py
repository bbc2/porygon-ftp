#!/usr/bin/env python3

import os
import ftplib
import logging
import logging.config

class TooManyErrors(Exception):
    pass

class MLSDNotSupported(Exception):
    pass

class BadEncoding(Exception):
    pass

def _(latin1_string):
    try:
        return latin1_string.encode('latin-1').decode('utf-8')
    except UnicodeError:
        raise BadEncoding(latin1_string)

class Walker():
    def __init__(self, ip, port, user, passwd, timeout, max_errors, handler):
        self.logger = logging.getLogger('Walker({})'.format(ip))
        self.conn = Connection(ip, port, user, passwd, timeout, self.logger, max_errors)
        self.todo = ['']
        self.handler = handler

    def walk(self):
        with self.handler:
            while True:
                try: path, *self.todo = self.todo
                except ValueError: break # run until todo is empty
                (files, dirs) = self.conn.ls(path)
                self.todo += dirs
                try:
                    self.handler.index([(_(path), _(name), size)
                                        for (path, name, size) in files])
                except BadEncoding as exc:
                    self.logger.warn('Bad encoding in %s: %s', path, exc.args[0])

class Connection():
    def __init__(self, ip, port, user, passwd, timeout, logger, max_errors=0):
        self.ip = ip
        self.port = port
        self.user = user
        self.passwd = passwd
        self.logger = logger
        self.errors_left = max_errors
        self.timeout = timeout
        self.mlsd_support = None
        self.ftp = None

    def _get_conn(self):
        if self.ftp is not None: return self.ftp
        self.ftp = ftplib.FTP()
        try:
            self.ftp.connect(self.ip, self.port, timeout=self.timeout)
            self.ftp.login(self.user, self.passwd)
        except ftplib.all_errors as exc:
            self.logger.warn('Connection error (%d before fatal): %r',
                    self.errors_left, exc)
            self._error()
            return self._get_conn()
        else:
            return self.ftp

    def _error(self):
        if self.errors_left == 0:
            raise TooManyErrors
        try:
            self.ftp.quit()
        except Exception as exc:
            self.logger.error('Error on QUIT: %r', exc)
        self.ftp = None
        self.errors_left -= 1

    def _handle_mlsd(self, path, listing):
        (files, dirs) = ([], [])

        for (name, attrs) in listing:
            if name[0] == '.': continue
            if attrs['type'] == 'file':
                files.append((path, name, attrs['size']))
            elif attrs['type'] == 'dir':
                dirs.append(os.path.join(path, name))

        return (files, dirs)

    def ls(self, path):
        ftp = self._get_conn()
        try:
            if self.mlsd_support or self.mlsd_support is None:
                listing = list(ftp.mlsd(path, facts=['type', 'size']))
                self.mlsd_support = True # previous command worked
            else:
                pass # only MLSD is supported at the moment
        except ftplib.error_perm:
            # First MLSD command is used to determine whether MLSD is supported or not.
            if self.mlsd_support is None: raise MLSDNotSupported
        except ftplib.all_errors as exc:
            self.logger.warn('FTP error (%d before fatal): %r', self.errors_left, exc)
            self._error()
            return self.ls(path)
        else:
            return self._handle_mlsd(path, listing)

if __name__ == '__main__':
    import sys
    import socket
    import local_settings as conf
    from backends import get_backend

    host = sys.argv[1]

    logging.config.dictConfig(conf.LOGGING)
    backend = get_backend(conf.BACKEND['NAME'])
    ip = socket.gethostbyname(host)
    walker = Walker(host, conf.PORT, user='rez', passwd='rez',
                    timeout=conf.INDEX_TIMEOUT, max_errors=conf.MAX_INDEX_ERRORS,
                    handler=backend.IndexHandler(conf.BACKEND, ip))
    walker.walk()
