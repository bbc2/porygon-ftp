from flask import Flask, render_template, request
from indexer import Index
app = Flask(__name__)

@app.route('/', methods=['POST', 'GET'])
def search():
    if request.method == 'POST':
        index = Index('index')
        hits = index.search(' '.join(['*%s*' % word for word in request.form['query'].split()]))
        return render_template('search.html', hits=hits)
    else:
        return render_template('search.html')

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
