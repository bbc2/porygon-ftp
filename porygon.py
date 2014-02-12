# encoding: utf-8

import os
from flask import Flask, render_template, request, url_for, redirect
from indexer import Index
app = Flask(__name__)

import settings

def sizeof_format(num):
    format_str = '{:.1f}\xa0{}'
    for x in ['o','k','M','G']:
        if num < 1024.0:
            return format_str.format(num, x)
        num /= 1024.0
    return format_str.format(num, 'T')

def get_url(host, path=''):
    return 'ftp://{}:{}@{}{}'.format(settings.FTP_USER, settings.FTP_PASSWD, host, path)

def get_ftp(ftp_db):
    import sqlite3
    ftp_db = sqlite3.connect(ftp_db)
    cur = ftp_db.cursor()
    cur.execute('select host from ftp')
    ftps = cur.fetchall()
    ftp_db.close()
    return [{ 'host': host, 'url': get_url(host) } for (host,) in ftps]

@app.route('/')
def home():
    return render_template('search.html', ftps=get_ftp(settings.FTP_DB))

@app.route('/search')
def search():
    query = request.args.get('query', '')
    if query == '': return redirect(url_for('home'))

    index = Index(settings.INDEX_DIR)
    hits = index.search(query)
    for hit in hits:
        hit['size'] = sizeof_format(int(hit['size']) * 1024)
        hit['url'] = get_url(hit['host'], os.path.join(hit['path'], hit['filename']))
        hit['dir_url'] = get_url(hit['host'], os.path.join(hit['path']))

    return render_template('search.html', hits=hits, hit_page=True, query=query)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
