#!/usr/bin/env python3

import os
from subprocess import call
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
from pyftpdlib.filesystems import AbstractedFS

def init_fs(base, fs):
    os.makedirs(base, exist_ok=True)
    for (name, value) in fs.items():
        path = os.path.join(base, name)
        if isinstance(value, int):
            call(['fallocate', '-l', str(value), path])
        else:
            init_fs(path, value)

def run(root):
    authorizer = DummyAuthorizer()
    authorizer.add_user('two', 'flower', root)

    handler = FTPHandler
    handler.authorizer = authorizer
    handler.banner = "pyftpdlib based ftpd ready."
    address = ('', 21)

    server = FTPServer(address, handler)
    server.serve_forever()

def main(root, fs):
    init_fs(root, fs)
    run(root)

if __name__ == '__main__':
    root = '/srv/ftp'
    fs = {
        'the_file': 1024,
        'the_folder': {
            'another_file': 1024 * 1024,
            'yet_another': 17 * 1024 * 1024,
        },
    }

    main(root, fs)
