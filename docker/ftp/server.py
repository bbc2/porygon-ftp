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
        'another_folder': {
            'a_file_with_a_tiny_little_bit_long_name_that'
            'you_would_not_have_wished_for_in_any_case': 1,
            'a file with a tiny little bit long name that'
            'you would not have wished for in any case, but this time with spaces': 1,
            'nested_folder': {
                'file_{:02}'.format(n): 1 for n in range(0, 100)
            },
        },
    }

    print(fs)

    main(root, fs)
