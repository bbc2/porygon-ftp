# encoding: utf-8

import os
from flask import Flask, render_template, request, url_for
from indexer import Index
app = Flask(__name__)

def sizeof_fmt(num):
    for x in ['o','K','M','G']:
        if num < 1024.0:
            return u'%3.1f\xa0%s' % (num, x)
        num /= 1024.0
    return u'%3.1f\xa0%s' % (num, 'T')

@app.route('/', methods=['POST', 'GET'])
def search():
    if request.method == 'POST':
        index = Index('index')
        hits = index.search(' '.join(['*%s*' % word for word in request.form['query'].split()]))
        for hit in hits:
            hit['size'] = sizeof_fmt(int(hit['size']) * 1024)
            hit['url'] = 'ftp://rez:rez@%s%s' % (hit['host'], os.path.join(hit['path'], hit['filename']))
        return render_template('search.html', hits=hits, hit_page=True)
    else:
        return render_template('search.html')

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
