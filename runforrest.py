from uuid import uuid4 as uuid
from pathlib import Path
from subprocess import Popen
import sys
import os
from argparse import ArgumentParser
import dill
import time

def defer(fun, *args, **kwargs):
    """Wrap a function for execution.

    Returns a `Task` without running the function. This `Task` can
    be used as an argument for other deferred functions to build a
    call graph. The call graph can then be executed by an `Executor`.

    Additionally, you can access attributes and indexes of the
    `Task`.

    """
    return Task(fun, args, kwargs)


class Task:
    """A proxy for a function result.

    Accessing attributes, indexing, and calling returns more
    `Tasks`.

    """

    def __init__(self, fun, args, kwargs):
        self._fun = fun
        self._args = args
        self._kwargs = kwargs
        self._id = str(id(self))

    def __eq__(self, other):
        return self._id == other._id and self._args == other._args and self._fun == other._fun

    def __getattr__(self, name):
        if name in ['__getstate__', '_id', '_kwargs', '_args', '_fun']:
            raise AttributeError()
        return TaskAttribute(self, name)

    def __getitem__(self, key):
        return TaskItem(self, key)


class PartOfTask(Task):
    """A proxy for a part of a Task.

    Tasks from accessing attributes, indexing, or calling a
    `Task`. Each `PartOfTask` has an `_id` that is shared for all
    equivalent attribute/index/call accesses.

    """
    def __init__(self, parent, index):
        self._parent = parent
        self._index = index
        self._id = parent._id + str(index)


class TaskAttribute(PartOfTask):
    pass


class TaskItem(PartOfTask):
    pass


class TaskList:
    """Evaluate `Tasks` and calculate their true return values.

    An `TaskList` schedules `Tasks`, and then executes them on
    several processes in parallel. For each `Task`, it walks the
    call chain, and executes all the code necessary to calculate the
    return values. The `TaskList` takes great pride in not evaluating
    `Tasks` more often than necessary, even if several
    `PartOfTasks` lead to the same original `Task`.

    By default, `schedule` dumps each `Task` in the directory
    `rf_todo`. Once the `Task` has been executed, it is transferred
    to either `rf_done` or `rf_failed`, depending on whether it raised
    errors or not.

    The `TaskList` delegates all the actual running of code to
    `evaluate`, which is called by invoking this very script as a
    command line program. The `TaskList` merely makes sure that a
    fixed number of processes are running at all times.

    """

    def __init__(self, todo_dir='rf_todo', done_dir='rf_done', fail_dir='rf_fail', session_file='rf_session', autoclean=False):
        self.todo_dir = Path(todo_dir)
        self.done_dir = Path(done_dir)
        self.fail_dir = Path(fail_dir)
        self.session_file = Path(session_file)
        self.autoclean = autoclean
        self.processes = {}

    def __del__(self):
        if self.autoclean:
            self.clean()

    def schedule(self, fun, *args, **kwargs):
        """Schedule a function or file for later execution.

        If `fun` is a function, supply arguments as arguments to
        `schedule`. It will be treated as if `fun` was a `Task` that
        already contained all arguments. This is merely for
        convenience.

        If `fun` is a `Task`, it is saved to the TODO directory. Use
        `run` to execute all the `Tasks` in the TODO directory.

        """

        if callable(fun):
            fun = defer(fun, *args, **kwargs)

        if not self.todo_dir.exists():
            self.todo_dir.mkdir()

        with open(self.todo_dir / str(uuid()), 'wb') as f:
            dill.dump(fun, f)

    def run(self, nprocesses=4, flags=None, save_session=False):
        """Execute all `Tasks` in TODO directory and yield values.

        All `Tasks` are executed in their own processes, and `run`
        makes sure that no more than `nprocesses` are active at any
        time.

        `Tasks` that raise errors are not yielded.

        `flags` can be one of `print` or `raise` if you want errors to
        be printed or raised.

        Use `save_session` to recreate all current globals in each
        process.

        """

        if not self.done_dir.exists():
            self.done_dir.mkdir()
        if not self.fail_dir.exists():
            self.fail_dir.mkdir()

        if save_session:
            dill.dump_session(self.session_file)

        class TaskIterator:
            def __init__(self, parent, todos, save_session):
                self.parent = parent
                self.todos = todos
                self.save_session = save_session

            def __iter__(self):
                for todo in self.todos:
                    yield from self.parent._wait(nprocesses)
                    self.parent._start_task(todo.name, flags, save_session)
                # wait for running jobs to finish:
                yield from self.parent._wait(1)

            def __len__(self):
                return len(self.todos)

        return TaskIterator(self, list(self.todo_dir.iterdir()), save_session)

    def _start_task(self, file, flags, save_session):
        args = ['python', '-m', 'runforrest', self.todo_dir / file, self.done_dir / file]
        if save_session:
            args += ['-s', self.session_file]
        if flags == 'print':
            args.append('-p')
        if flags == 'raise':
            args.append('-r')
        self.processes[file] = Popen(args, cwd=os.getcwd())

    def _wait(self, nprocesses):
        while len(self.processes) >= nprocesses:
            for file, proc in list(self.processes.items()):
                if proc.poll() is not None:
                    yield from self._finish_task(file)
            else:
                time.sleep(0.1)

    def _finish_task(self, file):
        process = self.processes.pop(file)

        if not (self.done_dir / file).exists():
            (self.todo_dir / file).rename(self.fail_dir / file)
            print(f'task {file} failed without error.')
            return

        (self.todo_dir / file).unlink()

        try:
            with open(self.done_dir / file, 'rb') as f:
                task = dill.load(f)
        except Exception as err:
            print(f'could not load result of task {file} because {err}.')
            (self.done_dir / file).rename(self.fail_dir / file)
            return

        if process.returncode == 0:
            yield task._value
        else:
            print(f'task {file} failed with error {task._error}.')
            (self.done_dir / file).rename(self.fail_dir / file)

    def todo_tasks(self):
        for todo in self.todo_dir.iterdir():
            with open(todo, 'rb') as f:
                yield dill.load(f)

    def done_tasks(self):
        for done in self.done_dir.iterdir():
            with open(done, 'rb') as f:
                yield dill.load(f)

    def fail_tasks(self):
        for fail in self.fail_dir.iterdir():
            with open(fail, 'rb') as f:
                yield dill.load(f)

    def clean(self):
        def remove(dir):
            if dir.exists():
                for f in dir.iterdir():
                    f.unlink()
                dir.rmdir()
        remove(self.todo_dir)
        remove(self.fail_dir)
        remove(self.done_dir)
        if self.session_file.exists():
            self.session_file.unlink()


def main():
    parser = ArgumentParser(description="Run an enqueued function")
    parser.add_argument('infile', help='contains the enqueued function')
    parser.add_argument('outfile', help='contains the evaluation results')
    parser.add_argument('-s', '--sessionfile', action='store', default=None)
    parser.add_argument('-p', '--do_print', action='store_true', default=False)
    parser.add_argument('-r', '--do_raise', action='store_true', default=False)

    args = parser.parse_args()
    run_task(args.infile, args.outfile, args.sessionfile, args.do_print, args.do_raise)


def run_task(infile, outfile, sessionfile, do_print, do_raise):
    """Execute `infile` and produce `outfile`.

    If `sessionfile` is given, load session from that file.

    Set `do_print` or `do_raise` to `True` if errors should be printed or
    raised.

    """

    if sessionfile:
        dill.load_session(Path(sessionfile))

    with open(infile, 'rb') as f:
        task = dill.load(f)

    try:
        task._value = evaluate(task)
        task._error = None
    except Exception as err:
        task._error = err
        task._value = None
    finally:
        with open(outfile, 'wb') as f:
            dill.dump(task, f)

    if task._error and do_raise:
        raise task._error

    if task._error and do_print:
        print(task._error)

    sys.exit(0 if task._error is None else -1)


def evaluate(result, known_results=None):
    """Execute a `result` and calculate its return value.

    `evaluate` walks the call chain to the `result`, and executes all
    the code necessary to calculate the return values. No `result` are
    executed more than once, even if several `PartOfTasks` lead to
    the same original `Task`.

    This is a recursive function that passes its state in
    `known_results`, where return values of all executed `Tasks` are
    stored.

    """

    # because pickling breaks isinstance(result, Task)
    if not 'Task' in result.__class__.__name__:
        return result

    if known_results is None:
        known_results = {}

    if result._id not in known_results:
        if result.__class__.__name__ in ['TaskItem', 'TaskCall', 'TaskAttribute']:
            return_value = evaluate(result._parent, known_results)
            if result.__class__.__name__ == 'TaskItem':
                known_results[result._id] = return_value[result._index]
            elif result.__class__.__name__ == 'TaskCall':
                known_results[result._id] = return_value()
            elif result.__class__.__name__ == 'TaskAttribute':
                known_results[result._id] = getattr(return_value, result._index)
            else:
                raise TypeError(f'unknown Task {type(result)}')
        else: # is Task
            args = [evaluate(arg, known_results) for arg in result._args]
            kwargs = {k: evaluate(v, known_results) for k, v in result._kwargs.items()}
            return_value = result._fun(*args, **kwargs)
            known_results[result._id] = return_value

    return known_results[result._id]


if __name__ == '__main__':
    main()
