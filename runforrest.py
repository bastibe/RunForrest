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
        self.metadata = None
        self.returnvalue = None
        self.errorvalue = None

    def __eq__(self, other):
        return self._id == other._id and self._args == other._args and self._fun == other._fun

    def __getattr__(self, name):
        if name in ['__getstate__', '_id']:
            raise AttributeError()
        return TaskAttribute(self, name)

    def __getitem__(self, key):
        return TaskItem(self, key)


class PartOfTask():
    """A proxy for a part of a Task.

    Tasks from accessing attributes, indexing, or calling a
    `Task`. Each `PartOfTask` has an `_id` that is shared for all
    equivalent attribute/index/call accesses.

    """

    def __init__(self, parent, index):
        self._parent = parent
        self._index = index
        self._id = parent._id + str(index)
        self.metadata = None
        self.returnvalue = None
        self.errorvalue = None

    def __eq__(self, other):
        return self._id == other._id and self._parent == other._parent

    def __getattr__(self, name):
        if name in ['__getstate__', '_id']:
            raise AttributeError()
        return TaskAttribute(self, name)

    def __getitem__(self, key):
        return TaskItem(self, key)


class TaskAttribute(PartOfTask):
    pass


class TaskItem(PartOfTask):
    pass


class TaskList:
    """Schedule tasks and run them in many processes.

    A `TaskList` schedules `Tasks`, and then executes them on several
    processes in parallel. For each `Task`, it walks the call chain,
    and executes all the code necessary to calculate the return
    values. The `TaskList` takes great pride in not evaluating `Tasks`
    more often than necessary, even if several `PartOfTasks` lead to
    the same original `Task`.

    By default, `schedule` dumps each `Task` in the directory
    `{directory}/todo`. Once the `Task` has been executed, it is
    transferred to either `{directory}/done` or `{directory}/failed`,
    depending on whether it raised errors or not.

    The `TaskList` delegates all the actual running of code to
    `evaluate`, which is called by invoking this very script as a
    command line program. The `TaskList` merely makes sure that a
    fixed number of processes are running at all times.

    """

    def __init__(self, directory, exist_ok=False, pre_clean=True, post_clean=False):
        self._directory = Path(directory)
        self._post_clean = post_clean

        if self._directory.exists():
            if not exist_ok:
                raise RuntimeError(f'TaskList directory {str(self._directory)} already exists')
            if pre_clean:
                self.clean()

        for dir in [self._directory, self._directory / 'todo',
                    self._directory / 'done', self._directory / 'fail']:
            if not dir.exists():
                dir.mkdir()

        self._processes = {}

    def __del__(self):
        if self._post_clean:
            self.clean()

    def schedule(self, task, metadata=None):
        """Schedule a task for later execution.

        The task is saved to the `{directory}/todo` directory. Use
        `run` to execute all the tasks in the `{directory}/todo}
        directory.

        If you want, you can attach metadata to the task, which you
        can retrieve as `task.metadata` after the task has been run.

        """

        if metadata is not None:
            task.metadata = metadata

        with (self._directory / 'todo' / (str(uuid()) + '.pkl')).open('wb') as f:
            dill.dump(task, f)

    def run(self, nprocesses=4, print_errors=False, save_session=False):
        """Execute all tasks in the `{directory}/todo}` directory.

        All tasks are executed in their own processes, and `run` makes
        sure that no more than `nprocesses` are active at any time.

        If `print_errors=True`, processes will print full stack traces
        of failing tasks. Since these errors happen on another
        process, this will not be caught by the debugger, and will not
        stop the `run`.

        Use `save_session` to recreate all current globals in each
        process.

        """

        if save_session:
            dill.dump_session(self._directory / 'session.pkl')

        class TaskIterator:
            def __init__(self, parent, todos, save_session):
                self.parent = parent
                self.todos = todos
                self.save_session = save_session

            def __iter__(self):
                for todo in self.todos:
                    yield from self.parent._finish_tasks(nprocesses)
                    self.parent._start_task(todo.name, print_errors, save_session)
                # wait for running jobs to finish:
                yield from self.parent._finish_tasks(1)

            def __len__(self):
                return len(self.todos)

        return TaskIterator(self, list((self._directory / 'todo').iterdir()), save_session)

    def _start_task(self, taskfilename, print_errors, save_session):
        """Start a new process, and append to self._processes."""
        args = ['python', '-m', 'runforrest',
                self._directory / 'todo' / taskfilename,
                self._directory / 'done' / taskfilename]
        if print_errors:
            args += ['-p']
        if save_session:
            args += ['-s', self._directory / 'session.pkl']
        self._processes[taskfilename] = Popen(args, cwd=os.getcwd())

    def _finish_tasks(self, nprocesses):
        """Wait while `nprocesses` are running and return finished tasks."""
        while len(self._processes) >= nprocesses:
            for file, proc in list(self._processes.items()):
                if proc.poll() is not None:
                    yield self._retrieve_task(file)
                    del self._processes[file]
            else:
                time.sleep(0.1)

    def _retrieve_task(self, taskfilename):
        """Load task, and sort into `{directory}/done` or `{directory}/fail`."""
        if not (self._directory / 'done' / taskfilename).exists():
            with (self._directory / 'todo' / taskfilename).open('rb') as f:
                task = dill.load(f)
                task.returnvalue = None
                task.errorvalue = RuntimeError('Task failed with unknown error.')
            with (self._directory / 'done' / taskfilename).open('wb') as f:
                dill.dump(task, f)

        (self._directory / 'todo' / taskfilename).unlink()

        with (self._directory / 'done' / taskfilename).open('rb') as f:
            task = dill.load(f)

        if task.errorvalue is not None:
            (self._directory / 'done' / taskfilename).rename(self._directory / 'fail' / taskfilename)

        return task

    def todo_tasks(self):
        """Yield all tasks in `{directory}/todo`."""
        for todo in (self._directory / 'todo').iterdir():
            with todo.open('rb') as f:
                yield dill.load(f)

    def done_tasks(self):
        """Yield all tasks in `{directory}/done`."""
        for done in (self._directory / 'done').iterdir():
            with done.open('rb') as f:
                yield dill.load(f)

    def fail_tasks(self):
        """Yield all tasks in `{directory}/fail`."""
        for fail in (self._directory / 'fail').iterdir():
            with fail.open('rb') as f:
                yield dill.load(f)

    def clean(self, clean_todo=True, clean_done=True, clean_fail=True):
        """Remove `{directory}` and all todo/done/fail tasks."""
        def remove(dir):
            if dir.exists():
                for f in dir.iterdir():
                    f.unlink()
                dir.rmdir()
        if clean_todo:
            remove(self._directory / 'todo')
        if clean_fail:
            remove(self._directory / 'fail')
        if clean_done:
            remove(self._directory / 'done')
        if clean_todo and clean_fail and clean_done:
            if (self._directory / 'session.pkl').exists():
                (self._directory / 'session.pkl').unlink()
            remove(self._directory)


def main():
    parser = ArgumentParser(description="Run an enqueued function")
    parser.add_argument('infile', type=Path, help='contains the enqueued function')
    parser.add_argument('outfile', type=Path, help='contains the evaluation results')
    parser.add_argument('-s', '--sessionfile', type=Path, action='store', default=None)
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

    with infile.open('rb') as f:
        task = dill.load(f)

    try:
        task.returnvalue = evaluate(task)
        task.errorvalue = None
    except Exception as err:
        task.errorvalue = err
        task.returnvalue = None
    finally:
        with outfile.open('wb') as f:
            dill.dump(task, f)

    if task.errorvalue is not None and do_raise:
        raise task.errorvalue

    sys.exit(0 if task.errorvalue is None else -1)


def evaluate(task, known_results=None):
    """Execute a `task` and calculate its return value.

    `evaluate` walks the call chain to the `task`, and executes all
    the code necessary to calculate the return values. No `task` are
    executed more than once, even if several `PartOfTasks` lead to
    the same original `Task`.

    This is a recursive function that passes its state in
    `known_results`, where return values of all executed `Tasks` are
    stored.

    """

    # because pickling breaks isinstance(task, Task)
    if not 'Task' in task.__class__.__name__:
        return task

    if known_results is None:
        known_results = {}

    if task._id not in known_results:
        if task.__class__.__name__ in ['TaskItem', 'TaskAttribute']:
            returnvalue = evaluate(task._parent, known_results)
            if task.__class__.__name__ == 'TaskItem':
                known_results[task._id] = returnvalue[task._index]
            elif task.__class__.__name__ == 'TaskAttribute':
                known_results[task._id] = getattr(returnvalue, task._index)
            else:
                raise TypeError(f'unknown Task {type(task)}')
        else: # is Task
            args = [evaluate(arg, known_results) for arg in task._args]
            kwargs = {k: evaluate(v, known_results) for k, v in task._kwargs.items()}
            returnvalue = task._fun(*args, **kwargs)
            known_results[task._id] = returnvalue

    return known_results[task._id]


if __name__ == '__main__':
    main()
