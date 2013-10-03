#!/usr/bin/env python

import os
import ftplib
from datetime import datetime
from whoosh.fields import Schema, TEXT, ID, NUMERIC, DATETIME
from whoosh.index import create_in, open_dir
from whoosh.qparser import MultifieldParser
from ftp_retry import FTP_Retry

import settings

def _(string):
    return unicode(string, 'utf-8')

class FTP_Indexer(FTP_Retry):
    def __init__(self, index, address, user, passwd):
        FTP_Retry.__init__(self, address, timeout=5)
        self.login(user=user, passwd=passwd)
        self.index = index
        self.host = address

    def walk(self):
        self.index.start()
        self._walk()
        self.index.commit()

    def _walk(self):
        dirs = self.nlst()
        path = self.pwd()
        for dir in dirs:
            try:
                self.cwd(dir)
                self._walk()
            except ftplib.error_perm:
                # that's a file
                filename = dir
                self.sendcmd('TYPE i')
                size = self.size(dir)
                print(path, dir, size)
                self.index.add(self.host, filename, path, str(size // 1024))
            finally:
                self.cwd(path)

class Index(object):
    def __init__(self, indexdir, erase=False):
        if not os.path.isdir(indexdir):
            os.mkdir(indexdir)
        if erase or os.listdir(indexdir) == []:
            schema = Schema(fullpath=ID(unique=True),
                            last_updated=DATETIME(),
                            filename=TEXT(stored=True),
                            host=TEXT(stored=True),
                            path=ID(stored=True),
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
        self.writer.update_document(fullpath=_(os.path.join(dir, filename)),
                                    last_updated=datetime.utcnow(),
                                    host=_(host),
                                    filename=_(filename),
                                    path=_(dir),
                                    size=_(size))

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
