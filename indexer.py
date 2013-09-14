#!/usr/bin/env python3

import ftplib

class Index(ftplib.FTP):
    def __init__(self, address, user, passwd):
        ftplib.FTP.__init__(self, address)
        self.login(user=user, passwd=passwd)

    def scan(self):
        dirs = self.nlst()
        top = self.pwd()
        for dir in dirs:
            try:
                self.cwd(dir)
                self.scan()
            except ftplib.error_perm:
                # that's a file
                print(self.pwd(), dir, self.size(dir))
            finally:
                self.cwd(top)

if __name__ == '__main__':
    index = Index('localhost', 'rez', '35zero')
    index.scan()
