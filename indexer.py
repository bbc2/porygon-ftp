#!/usr/bin/env python

import os
import ftplib
from whoosh.fields import Schema, TEXT, ID, NUMERIC
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser

class FTP_Indexer(ftplib.FTP):
    def __init__(self, index, address, user, passwd):
        ftplib.FTP.__init__(self, address, timeout=5)
        self.login(user=user, passwd=passwd)
        self.index = index

    def scan(self):
        self.index.start()
        self._scan()
        self.index.commit()

    def _scan(self):
        dirs = self.nlst()
        path = self.pwd()
        for dir in dirs:
            try:
                self.cwd(dir)
                self._scan()
            except ftplib.error_perm:
                # that's a file
                filename = dir
                self.sendcmd("TYPE i")
                size = self.size(dir)
                print(path, dir, size)
                self.index.add(filename, path, size // 1024)
            finally:
                self.cwd(path)

class Index(object):
    def __init__(self, indexdir):
        if not os.path.isdir(indexdir):
            os.mkdir(indexdir)
        if os.listdir(indexdir) == []:
            schema = Schema(filename=TEXT(stored=True),
                                          path=ID(stored=True),
                                          size=NUMERIC(stored=True))
            self.db = create_in('index', schema)
        else:
            self.db = open_dir('index')

    def start(self):
        self.writer = self.db.writer()

    def commit(self):
        self.writer.commit()

    def search(self, txt):
        with self.db.searcher() as searcher:
            parser = QueryParser('filename', self.db.schema)
            query = parser.parse(txt)
            results = searcher.search(query)
            return([{'filename': hit['filename'], 'size': hit['size'], 'path': hit['path']} for hit in results])

    def add(self, filename, dir, size):
        self.writer.add_document(filename=unicode(filename), path=unicode(dir), size=unicode(size))

if __name__ == '__main__':
    index = Index('index')
    ftp = FTP_Indexer(index, 'localhost', 'rez', '35zero')
    ftp.scan()
    print(index.search(u'*pokemon*'))
