import os
import sqlite3

class ScanHandler:
    def __init__(self, conf):
        self.path = conf['SCAN_DB']
        con = sqlite3.connect(self.path)
        con.execute('create table if not exists hosts ('
                    'ip text primary key on conflict replace,'
                    'name text,'
                    'last_online text not null,'
                    'last_indexed text)')

    def __enter__(self):
        self.con = sqlite3.connect(self.path)
        self.cur = self.con.cursor()

    def update(self, hosts, erase=True):
        if erase: self.cur.execute('delete from hosts')
        self.cur.executemany('insert into hosts values (?, ?, ?, ?)',
                ((ip, info.get('name', None), info['last_online'],
                  info.get('last_indexed', None)) for (ip, info) in hosts.items()))

    def __exit__(self, type, value, tb):
        self.con.commit()
        self.cur.close()
        self.con.close()

class IndexHandler:
    def __init__(self, conf, ip):
        self.ip = ip
        self.path = conf['INDEX_DB']
        with sqlite3.connect(self.path) as con:
            # Enable WAL (https://www.sqlite.org/wal.html) to allow reads while writing.
            con.execute('pragma journal_mode=wal')
            con.execute('create virtual table if not exists files using fts4('
                        'path text,'
                        'name text,'
                        'ip text,'
                        'size integer,'
                        'notindexed=ip, notindexed=size, tokenize=unicode61)')

    def __enter__(self):
        self.con = sqlite3.connect(self.path)
        self.cur = self.con.cursor()
        self.cur.execute('delete from files where ip=?', (self.ip,))

    def index(self, files):
        self.cur.executemany('insert into files values (?, ?, ?, ?)',
                ((path, name, self.ip, size) for (path, name, size) in files))

    def __exit__(self, type, value, tb):
        self.con.commit()
        self.cur.close()
        self.con.close()

def get_hosts(conf):
    con = sqlite3.connect(conf['SCAN_DB'])
    cur = con.cursor()
    cur.execute('select ip, name, last_online, last_indexed from hosts')
    return { ip: { 'name': n, 'last_online': o, 'last_indexed': i } for (ip, n, o, i) in cur }

def search(conf, terms, hosts, limit=None):
    con = sqlite3.connect(conf['INDEX_DB'])
    cur = con.cursor()
    match_param = ' '.join(terms)
    limit_param = limit is None and -1 or limit

    query = '''select path, name, ip, size from files
               where files match ? and ip in ({})
               limit ?'''.format(','.join('?' * len(hosts)))
    bindings = (match_param,) + tuple(hosts) + (limit_param,)

    try:
        cur.execute(query, bindings)
    except sqlite3.OperationalError:
        # file table has not been created yet
        return []

    return [{ 'path': p, 'name': n, 'host': hosts[ip], 'size': float(s) } for (p, n, ip, s) in cur]
