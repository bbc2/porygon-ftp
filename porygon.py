# encoding: utf-8

import os
from flask import Flask, render_template, request, url_for
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

def get_ftp(ftp_db):
    import sqlite3
    ftp_db = sqlite3.connect(ftp_db)
    cur = ftp_db.cursor()
    cur.execute('select ip from ftp')
    ftps = cur.fetchall()
    ftp_db.close()
    return [{ 'host': ftp[0], 'url': 'ftp://rez:rez@%s' % ftp[0] } for ftp in ftps]

@app.route('/', methods=['POST', 'GET'])
def search():
    if request.method == 'POST':
        index = Index(settings.INDEX_DIR)
        query = request.form['query']
        hits = index.search(query)
        for hit in hits:
            hit['size'] = sizeof_format(int(hit['size']) * 1024)
            hit['url'] = 'ftp://rez:rez@%s%s' % (hit['host'], os.path.join(hit['path'], hit['filename']))
            hit['dir_url'] = 'ftp://rez:rez@%s%s' % (hit['host'], os.path.join(hit['path']))

        return render_template('search.html', hits=hits, hit_page=True, query=query)
    else:
        return render_template('search.html', ftps=get_ftp(settings.FTP_DB))

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
