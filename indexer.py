#!/usr/bin/env python

import ftplib
import whoosh
import whoosh.fields
import whoosh.index
import whoosh.qparser

class Index(ftplib.FTP):
    def __init__(self, address, user, passwd):
        ftplib.FTP.__init__(self, address, timeout=5)
        self.login(user=user, passwd=passwd)
        schema = whoosh.fields.Schema(filename=whoosh.fields.TEXT(stored=True),
                                      path=whoosh.fields.ID(stored=True),
                                      size=whoosh.fields.NUMERIC(stored=True))
        self.db = whoosh.index.create_in('index', schema)

    def scan(self):
        self.writer = self.db.writer()
        self._scan()
        self.writer.commit()

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
                self._index(filename, path, size // 1024)
            finally:
                self.cwd(path)

    def _index(self, filename, dir, size):
        self.writer.add_document(filename=filename, path=dir, size=size)

    def search(self, txt):
        with self.db.searcher() as searcher:
            parser = whoosh.qparser.QueryParser('filename', self.db.schema)
            parser.add_plugin(whoosh.qparser.FuzzyTermPlugin())
            query = parser.parse(txt)
            results = searcher.search(query)
            print(results[:])

if __name__ == '__main__':
    index = Index('localhost', 'rez', '35zero')
    index.scan()
    index.search('*pokemon*')
