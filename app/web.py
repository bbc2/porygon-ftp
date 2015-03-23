# encoding: utf-8

import os
import re
from slugify import slugify
from flask import Flask, render_template, request, url_for, redirect
app = Flask(__name__)

from db import get_backend
import local_settings as conf

def sizeof_format(num):
    format_str = '{:.1f}\xa0{}'
    for x in ['o','k','M','G']:
        if num < 1024.0:
            return format_str.format(num, x)
        num /= 1024.0
    return format_str.format(num, 'T')

def get_url(host, path=''):
    return 'ftp://{}:{}@{}'.format(conf.USER, conf.PASSWD, os.path.join(host, path))

def get_ftp():
    store = get_backend(conf.STORE['NAME']).Store(conf.STORE['CONF'])

    with store.scan_db() as db:
        hosts = db.get_hosts()

    return [{ 'name': info['name'], 'url': get_url(info['name']) }
            for (_, info) in hosts.items()]

@app.route('/')
def home():
    return render_template('search.html', ftps=get_ftp())

@app.route('/search')
def search():
    query = request.args.get('query', '')
    if query == '': return redirect(url_for('home'))

    # Normalize terms and then make sure they only contain alphanumeric characters
    simple_terms = [slugify(term, separator='') for term in query.split(' ')]
    safe_terms = [re.sub(r'[^a-zA-Z0-9]+', '', term) for term in simple_terms]

    store = get_backend(conf.STORE['NAME']).Store(conf.STORE['CONF'])

    with store.scan_db() as db:
        hosts = db.get_hosts()

    with store.index_db() as db:
        hits = db.search(safe_terms, hosts, limit=100)

    for hit in hits:
        hit['size'] = sizeof_format(hit['size'])
        hit['url'] = get_url(hit['host']['name'], os.path.join(hit['path'], hit['name']))
        hit['dir_url'] = get_url(hit['host']['name'], os.path.join(hit['path']))

    return render_template('search.html', hits=hits, hit_page=True, query=query)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
