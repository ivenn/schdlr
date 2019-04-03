import random
import string
import networkx as nx

from schdlr import etc
from schdlr import task


class DummyNode:

    def __init__(self, name, done):
        self.name = name
        self.done = done
        self.failed = False

    @property
    def state(self):
        return task.DONE if self.done else task.PENDING

    def __repr__(self):
        return self.name


def build_DAG(deps):
    g = nx.DiGraph()

    nodes = set()
    dep_pairs = []

    for v, deps_from in deps.items():
        nodes.add(v)
        for u in deps_from:
            nodes.add(u)
            dep_pairs.append((u, v))

    for node in nodes:
        g.add_node(node)

    for v, u in dep_pairs:
        g.add_edge(v, u)

    start = DummyNode('START', True)
    end = DummyNode('END', False)
    g.add_node(start)
    g.add_node(end)

    for v in g.nodes:
        if v in [start, end]:
            continue
        if not list(g.successors(v)):
            g.add_edge(v, end)
        if not list(g.predecessors(v)):
            g.add_edge(start, v)

    # verify that graph has valid workflow structure(DAG)
    if not nx.has_path(g, start, end):
        raise Exception("There is no path in generated graph from START to END")
    if not nx.is_directed_acyclic_graph(g):
        raise Exception("Generated graph is not a valid DAG")

    return g, start, end


NOT_STARTED = "NOT_STARTED"
IN_PROGRESS = "IN_PROGRESS"
FAILED = "FAILED"
COMPLETED = "COMPLETED"


class Workflow:

    def __init__(self, deps, name=None):
        """
        :param deps: dependencies dict
         task1: [task2,task3] -> task1 depends on task2 and task3
        """
        self.name = name if name else "".join(
            [random.choice(string.ascii_letters) for _ in range(2)] +
            [str(random.randint(0, 10)) for _ in range(2)]
        )
        self.deps = deps
        self.g, self.start, self.end = build_DAG(self.deps)
        self._state = NOT_STARTED

        self.tasks_to_do = [self.start, ]
        self.tasks_in_progress = []
        self.tasks_done = []
        self.logger = etc.get_logger("wflow.{name}".format(name=self.name))

    def print(self):
        import matplotlib.pyplot as plt
        nx.draw(self.g, None, edge_color='b', with_labels=True)
        plt.show()

    @property
    def state(self):
        if self._state == NOT_STARTED:
            # recalculate tasks ??
            if any([t.state != task.PENDING for t in self.g.successors(self.start)]):
                self.logger.info("{old} -> {new}".format(old=self._state, new=IN_PROGRESS))
                self._state = IN_PROGRESS

        return self._state

    @state.setter
    def state(self, new_state):
        self.logger.info("{old} -> {new}".format(old=self._state, new=new_state))
        self._state = new_state

    @property
    def done(self):
        return self.end.done

    def to_do(self):
        if self.state in [FAILED, COMPLETED]:
            return None

        to_do = set()
        in_progress = set()
        for v in self.tasks_in_progress + self.tasks_to_do:
            if v.state == task.PENDING:
                to_do.add(v)
            elif v.state in [task.SCHEDULED, task.IN_PROGRESS]:
                in_progress.add(v)
            elif v.state == task.FAILED:
                self.state = FAILED
                return None
            else:  # task.DONE
                if v is not self.start:
                    self.tasks_done.append(v)
                for u in self.g.successors(v):
                    if all([t.done for t in self.g.predecessors(u)]):
                        to_do.add(u)

        self.tasks_in_progress = list(in_progress)

        if to_do == {self.end} and not in_progress:
            self.end.done = True
            self.state = COMPLETED
            return None

        self.tasks_to_do = list(to_do)

        return list(to_do)




