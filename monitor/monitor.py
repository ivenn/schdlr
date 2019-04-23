import collections
import time

from flask import Flask, render_template
from schdlr import task

app = Flask(__name__)


task_state_color = collections.OrderedDict([
    (task.PENDING, "#00ffff"),
    (task.SCHEDULED, "#ffff00"),
    (task.IN_PROGRESS, "#FFA500"),
    (task.FAILED, "#ff0000"),
    (task.DONE, "#00ff00"),
])


def task_cell(tsk):
    task_events = [tsk.name] + ["{event}: {ts}".format(event=event,
                                ts=time.strftime("%d %b %Y %H:%M:%S",
                                                 time.localtime(ts)))
                                for event, ts in tsk.events.items()]

    return """<td bgcolor={color}>
                <a href="#">{name}</a>
                    <div class='details'>
                        {task_details}
                    </div>
                </div>
            </td>""".format(
            name=tsk.name, color=task_state_color[tsk.state], task_details="<br>".join(task_events))


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/scheduler')
def scheduler():
    return render_template('scheduler.html', stat=scheduler_stat())


@app.route('/workflows')
def workflows():
    states_cells = ["<td bgcolor={color}>{state}</td>".format(
                    color=color, state=state)
                    for state, color in task_state_color.items()]
    colors = "<div id='colors'><br><table border='1'>{states}</table><br></div>".format(
        states="".join(states_cells))

    return render_template('workflows.html', colors=colors, stat=workflow_stat())


@app.route('/cron')
def cron():
    return render_template('cron.html', details=cron_info())


@app.route('/scheduler_stat')
def scheduler_stat():
    schdlr = app.config.get('schdlr')
    stat = schdlr.tasks_stat(detailed=True)

    in_queue_tbl = "<table border='1'>{cells}</table>".format(
        cells="".join([task_cell(t) for t in stat['in_queue']]))
    in_queue = "<div id='in_queue'>IN QUEUE: {tbl}</div>".format(tbl=in_queue_tbl)

    in_progress_tbls = []
    for w, t in stat['in_progress'].items():
        in_progress_tbls.append("""<table border='1'>
                                    <td>{name}</td>
                                    {task}
                                    </table>""".format(
            name=w.name, task=task_cell(t) if t else "<td></td>"))
    in_progress = "<div id='in_progress'>IN PROGRESS: {tbl}</div>".format(
        tbl="".join(in_progress_tbls))

    done_tbls = []
    for w, tasks in stat['done'].items():
        done_tbls.append("""<table border='1'>
                                <td>{name}</td>
                                {tasks}
                            </table>""".format(
            name=w.name,
            tasks="".join([task_cell(t) for t in tasks] if tasks else "")))
    done = "<div id='done'>DONE: {tbl}</div>".format(tbl="".join(done_tbls))

    return "<br>".join([in_queue, in_progress, done])


@app.route('/workflow_stat')
def workflow_stat():
    schdlr = app.config.get('schdlr')
    workflows = list(schdlr.workflows.values()) + list(schdlr.workflows_done.values())
    wfs = []
    for wf in workflows:
        wfs.append("""<div name={name}>
                        {name}
                        <table border='1'>
                            {tasks}
                        </table>
                      </div>""".format(
            name=wf.name, tasks="".join([task_cell(t) for t in wf.tasks])
        ))

    return "<br>".join(wfs)


@app.route('/cron_info')
def cron_info():
    schdlr = app.config.get('schdlr')

    return render_template('cron_details.html', repeatables=schdlr.to_repeat)


def run_monitor(schdlr, port=None):
    app.config['schdlr'] = schdlr
    app.run(port=port)

