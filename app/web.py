# encoding: utf-8

import os
import re
from slugify import slugify
from flask import Flask, render_template, request, url_for, redirect
app = Flask(__name__)

from db import get_backend
import local_settings as conf

def format_size(num):
    if num is None:
        return None

    format_str = '{:.1f}\xa0{}'
    for x in ['o','kio','Mio','Gio']:
        if num < 1024.0:
            return format_str.format(num, x)
        num /= 1024.0
    return format_str.format(num, 'Tio')

def url_of(host, path=''):
    return 'ftp://{}:{}@{}'.format(conf.USER, conf.PASSWD, os.path.join(host, path))

def get_servers():
    store = get_backend(conf.STORE['NAME']).Store(conf.STORE['CONF'])

    with store.scan_db() as db:
        hosts = db.get_hosts()

    return [{ 'name': info['name'], 'url': url_of(info['name']),
              'last_indexed': info['last_indexed'],
              'file_count': info['file_count'], 'size': format_size(info['size']) }
            for (_, info) in hosts.items()]

@app.route('/')
def home():
    return render_template('home.html', servers=get_servers(), online=True)

@app.route('/search')
def search():
    query = request.args.get('query', '')
    if query == '': return redirect(url_for('home'))

    online = request.args.get('online', 'off') == 'on'

    # Normalize terms and then make sure they only contain alphanumeric characters
    simple_terms = slugify(query, separator=' ').split(' ')
    safe_terms = [re.sub(r'[^a-zA-Z0-9]+', '', term) for term in simple_terms]

    store = get_backend(conf.STORE['NAME']).Store(conf.STORE['CONF'])

    with store.scan_db() as db:
        hosts = db.get_hosts()

    if online:
        hosts = { ip: info for (ip, info) in hosts.items() if info['online'] }

    with store.index_db() as db:
        hits = db.search(safe_terms, hosts, limit=100)

    for hit in hits:
        hit['size'] = format_size(hit['size'])
        hit['url'] = url_of(hit['host']['name'], os.path.join(hit['path'], hit['name']))
        hit['dir_url'] = url_of(hit['host']['name'], os.path.join(hit['path']))

    return render_template('search.html', hits=hits, query=query, online=online)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
