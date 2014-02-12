#!/usr/bin/env python3

import os
import sys
import ftplib
import traceback
import logging
import logging.config
from datetime import datetime
from whoosh.query import DateRange, Term
from whoosh.fields import Schema, TEXT, ID, NUMERIC, DATETIME
from whoosh.analysis import FancyAnalyzer, CharsetFilter
from whoosh.support.charset import accent_map
from whoosh.index import create_in, open_dir
from whoosh.qparser import MultifieldParser
from ftp_retry import FTP_Retry

import settings

logging.config.dictConfig(settings.INDEX_LOGGING)
logger = logging.getLogger()

def _(string):
    return string.encode('latin-1').decode('utf-8')

class MLSD_NotSupported(Exception):
    pass

class FTP_Indexer(object):
    def __init__(self, index, host, user, passwd):
        self.index = index
        self.host = host
        self.user = user
        self.passwd = passwd
        self.logger = logging.getLogger(host)
        self._new_ftp()

    def walk(self):
        start_time = datetime.utcnow()
        self.index.start()

        try:
            self._walk()
        except Exception as exn:
            self.index.cancel()
            raise exn

        self.index.purge(Term('host', self.host) & DateRange('last_updated', None, start_time))
        self.index.commit()

    def _walk(self, path=None):
        try:
            if path == None:
                path = self.ftp.pwd()

            try:
                files = self.ftp.mlsd(facts=['type', 'size'])
            except ftplib.error_perm:
                raise MLSD_NotSupported

            for (filename, attrs) in files:
                if filename[0] == '.':
                    continue

                try:
                    filename_u = _(filename)
                    path_u = _(path)
                except UnicodeDecodeError:
                    self.logger.warning('Could not decode {}'.format(os.path.join(path, filename)))
                    continue

                self.logger.debug('{} {}'.format(os.path.join(path_u, filename_u), attrs))
                if attrs['type'] == 'dir':
                    self.ftp.cwd(filename)
                    self._walk(os.path.join(path, filename))
                    self.ftp.cwd(path)
                elif attrs['type'] == 'file':
                    self.index.add(self.host, filename_u, path_u, int(attrs['size']) // 1024)
        except ftplib.error_perm:
            self.logger.info('Permission denied on {}'.format(os.path.join(path, filename)))
        except (OSError, ftplib.error_reply): # timeout or desynchronization
            self._new_ftp()
            self.ftp.cwd(path)

    def _new_ftp(self):
        self.logger.info('Connecting'.format(self.host))
        self.ftp = FTP_Retry(self.host, timeout=settings.FTP_INDEX_TIMEOUT)
        self.logger.info('Logging in as {}:{}'.format(self.user, self.passwd))
        self.ftp.login(self.user, self.passwd)

class Index(object):
    def __init__(self, indexdir, erase=False):
        if not os.path.isdir(indexdir):
            os.mkdir(indexdir)
        if erase or os.listdir(indexdir) == []:
            analyzer = FancyAnalyzer() | CharsetFilter(accent_map)
            schema = Schema(fullpath=ID(unique=True),
                            last_updated=DATETIME(),
                            filename=TEXT(stored=True, analyzer=analyzer),
                            host=TEXT(stored=True),
                            path=TEXT(stored=True, analyzer=analyzer),
                            size=NUMERIC(stored=True, sortable=True))
            self.db = create_in(indexdir, schema)
        else:
            self.db = open_dir(indexdir)

    def start(self):
        self.writer = self.db.writer()

    def commit(self):
        self.writer.commit(optimize=True)

    def cancel(self):
        self.writer.cancel()

    def search(self, txt):
        with self.db.searcher() as searcher:
            # iter([]) instead of just [] is a hack to circumvent a bug in Whoosh
            parser = MultifieldParser(['filename', 'path'], self.db.schema, plugins=iter([]))
            query = parser.parse(txt)
            results = searcher.search(query, limit=settings.HIT_LIMIT,
                                      sortedby="size", reverse=True)
            return([{'host': hit['host'], 'filename': hit['filename'], 'size': hit['size'], 'path': hit['path']} for hit in results])

    def add(self, host, filename, path, size):
        self.writer.add_document(fullpath=os.path.join(path, filename),
                                 last_updated=datetime.utcnow(),
                                 host=host,
                                 filename=filename,
                                 path=path,
                                 size=size)

    def purge(self, query):
        self.writer.delete_by_query(query)

if __name__ == '__main__':
    import sqlite3
    ftp_db = sqlite3.connect(settings.FTP_DB)
    cur = ftp_db.cursor()
    cur.execute('select host from ftp')
    ftps = cur.fetchall()
    ftp_db.close()
    index = Index(settings.INDEX_DIR)
    for (host,) in ftps:
        logger.info('Begin indexing {}'.format(host))
        try:
            ftp_indexer = FTP_Indexer(index, host, settings.FTP_USER, settings.FTP_PASSWD)
            ftp_indexer.walk()
        except MLSD_NotSupported:
            logger.warning('FTP MLSD not supported on {}'.format(host))
        except:
            logger.exception('Exception occured during indexing of {}'.format(host))
        logger.info('End indexing {}'.format(host))
