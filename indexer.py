#!/usr/bin/env python3

import os
import ftplib
from datetime import datetime
from whoosh.query import DateRange
from whoosh.fields import Schema, TEXT, ID, NUMERIC, DATETIME
from whoosh.analysis import FancyAnalyzer
from whoosh.index import create_in, open_dir
from whoosh.qparser import MultifieldParser
from ftp_retry import FTP_Retry

import settings

class FTP_Indexer(object):
    def __init__(self, index, host, user, passwd):
        self.index = index
        self.host = host
        self.user = user
        self.passwd = passwd
        self._new_ftp()

    def walk(self):
        start_time = datetime.utcnow()
        self.index.start()
        self._walk()
        self.index.purge(start_time)
        self.index.commit()

    def _walk(self):
        dirs = self.ftp.nlst()
        path = self.ftp.pwd()
        for dir in dirs:
            try:
                self.ftp.cwd(dir)
                self._walk()
            except ftplib.error_perm:
                # that's a file
                filename = dir
                self.ftp.sendcmd('TYPE i')
                size = self.ftp.size(dir)
                print("{}/{} ({})".format(path.encode('latin-1'), dir.encode('latin-1'), size))
                self.index.add(self.host.decode('utf-8'),
                               filename.encode('latin-1').decode('utf-8'),
                               path.encode('latin-1').decode('utf-8'),
                               size // 1024)
            except OSError:
                self._new_ftp()
            except ftplib.all_errors:
                self.ftp.quit()
                self._new_ftp()
            finally:
                self.ftp.cwd(path)

    def _new_ftp(self):
        print('Connecting to {}'.format(self.host))
        self.ftp = FTP_Retry(self.host, timeout=5)
        print('Logging in as {}:{}'.format(self.user, self.passwd))
        self.ftp.login(self.user, self.passwd)

class Index(object):
    def __init__(self, indexdir, erase=False):
        if not os.path.isdir(indexdir):
            os.mkdir(indexdir)
        if erase or os.listdir(indexdir) == []:
            schema = Schema(fullpath=ID(unique=True),
                            last_updated=DATETIME(),
                            filename=TEXT(stored=True, analyzer=FancyAnalyzer()),
                            host=TEXT(stored=True),
                            path=TEXT(stored=True, analyzer=FancyAnalyzer()),
                            size=NUMERIC(stored=True))
            self.db = create_in(indexdir, schema)
        else:
            self.db = open_dir(indexdir)

    def start(self):
        self.writer = self.db.writer()

    def commit(self):
        self.writer.commit()

    def search(self, txt):
        with self.db.searcher() as searcher:
            parser = MultifieldParser(['filename', 'path'], self.db.schema)
            query = parser.parse(txt)
            results = searcher.search(query, limit=settings.HIT_LIMIT)
            return([{'host': hit['host'], 'filename': hit['filename'], 'size': hit['size'], 'path': hit['path']} for hit in results])

    def add(self, host, filename, dir, size):
        self.writer.update_document(fullpath=os.path.join(dir, filename),
                                    last_updated=datetime.utcnow(),
                                    host=host,
                                    filename=filename,
                                    path=dir,
                                    size=size)

    def purge(self, before):
        deleted_files = DateRange('last_updated', None, before)
        self.writer.delete_by_query(deleted_files)

if __name__ == '__main__':
    import sqlite3
    ftp_db = sqlite3.connect(settings.FTP_DB)
    cur = ftp_db.cursor()
    cur.execute('select ip from ftp')
    ftps = cur.fetchall()
    ftp_db.close()
    index = Index(settings.INDEX_DIR)
    for ftp in ftps:
        ftp_indexer = FTP_Indexer(index, ftp[0].encode('utf-8'), settings.FTP_USER, settings.FTP_PASSWD)
        ftp_indexer.walk()
