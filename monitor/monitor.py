from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def index():
    schdlr = app.config.get('schdlr')
    return render_template('index.html', stat=stat())


@app.route('/stat')
def stat():
    schdlr = app.config.get('schdlr')
    stat = schdlr.tasks_stat()
    return "<div><br>in_queue={in_queue}, <br>in_progress={in_progress}, <br>done={done}</div>".format(
        in_queue=stat['in_queue'], in_progress=stat['in_progress'], done=stat['done'])


def run_monitor(schdlr, port=None):
    app.config['schdlr'] = schdlr
    app.run(port=port)


if __name__ == '__main__':
    class A:
        pass
    run_monitor(A())
