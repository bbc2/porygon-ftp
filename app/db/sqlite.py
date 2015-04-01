import os
import sqlite3

class _Database:
    def __enter__(self):
        self.con = sqlite3.connect(self.db)
        self.cur = self.con.cursor()
        return self

    def __exit__(self, type, value, tb):
        self.con.commit()
        self.cur.close()
        self.con.close()

class _ScanDatabase(_Database):
    def __init__(self, db):
        self.db = db
        with sqlite3.connect(self.db) as con:
            con = sqlite3.connect(self.db)
            con.execute('create table if not exists hosts ('
                        'ip text primary key on conflict replace,'
                        'name text,'
                        'online boolean,'
                        'last_online text not null,'
                        'last_indexed text,'
                        'file_count integer,'
                        'size)')

    def set_hosts(self, hosts):
        self.cur.execute('delete from hosts')

        values = ((ip,
                   info.get('name', None),
                   info['online'],
                   info['last_online'],
                   info.get('last_indexed', None),
                   info.get('file_count', None),
                   info.get('size', None))
                  for (ip, info) in hosts.items())

        self.cur.executemany('insert into hosts values (?, ?, ?, ?, ?, ?, ?)', values)

    def get_hosts(self):
        self.cur.execute('select ip, name, online, last_online, last_indexed,'
                         'file_count, size from hosts')

        return { ip: { 'name': n, 'online': o, 'last_online': l, 'last_indexed': i,
                       'file_count': f, 'size': s }
                 for (ip, n, o, l, i, f, s) in self.cur }

class _IndexDatabase(_Database):
    def __init__(self, db):
        self.db = db
        with sqlite3.connect(self.db) as con:
            # Enable WAL (https://www.sqlite.org/wal.html) to allow reads while writing.
            con.execute('pragma journal_mode=wal')
            con.execute('create virtual table if not exists files using fts4('
                        'path text,'
                        'name text,'
                        'ip text,'
                        'size integer,'
                        'notindexed=ip, notindexed=size, tokenize=unicode61)')

    def delete(self, ip):
        self.cur.execute('delete from files where ip=?', (ip,))

    def prune(self, hosts_to_keep):
        set_param = '({})'.format(','.join('?' * len(hosts_to_keep)))
        query = 'delete from files where ip not in {}'.format(set_param)
        self.cur.execute(query, hosts_to_keep)

    def index(self, ip, files):
        self.cur.executemany('insert into files values (?, ?, ?, ?)',
                ((path, name, ip, size) for (path, name, size) in files))

    def search(self, terms, hosts, limit=None):
        match_param = ' '.join(terms)
        limit_param = limit is None and -1 or limit

        query = '''select path, name, ip, size from files
                   where files match ? and ip in ({})
                   limit ?'''.format(','.join('?' * len(hosts)))
        bindings = (match_param,) + tuple(hosts) + (limit_param,)

        self.cur.execute(query, bindings)

        return [{ 'path': p, 'name': n, 'host': hosts[ip], 'size': float(s) }
                for (p, n, ip, s) in self.cur]

    def get_stat(self, ip):
        self.cur.execute('select count(*), sum(size) from files where ip = ?', (ip,))
        ((file_count, size),) = self.cur
        return { 'file_count': file_count, 'size': size }

class Store:
    def __init__(self, conf):
        self.scan_file = conf['scan_file']
        self.index_file = conf['index_file']

    def scan_db(self):
        return _ScanDatabase(self.scan_file)

    def index_db(self):
        return _IndexDatabase(self.index_file)
