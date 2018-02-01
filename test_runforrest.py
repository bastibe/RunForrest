import runforrest
import pathlib


def identity(n):
    return n


def test_run():
    tasklist = runforrest.TaskList(autoclean=True)
    task = runforrest.defer(identity, 42)
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=4))
    assert tasks[0] == 42


def test_nested_run():
    tasklist = runforrest.TaskList(autoclean=True)
    task = runforrest.defer(identity, 42)
    task = runforrest.defer(identity, task)
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=4))
    assert tasks[0] == 42


def test_multiple_runs(howmany=20):
    tasklist = runforrest.TaskList(autoclean=True)
    for v in range(howmany):
        task = runforrest.defer(identity, v)
        tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=10))
    assert len(tasks) == howmany
    for r, v in zip(sorted(tasks), range(howmany)):
        assert r == v


def test_multiple_nested_runs(howmany=20):
    tasklist = runforrest.TaskList(autoclean=True)
    for v in range(howmany):
        task = runforrest.defer(identity, v)
        task = runforrest.defer(identity, task)
        tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=10))
    assert len(tasks) == howmany
    for r, v in zip(sorted(tasks), range(howmany)):
        assert r == v


def test_task_accessor():
    tasklist = runforrest.TaskList(autoclean=True)
    # send something that has an attribute:
    task = runforrest.defer(identity, Exception(42))
    # retrieve the attribute:
    task = runforrest.defer(identity, task.args)
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=1))
    assert tasks[0] == (42,)


def test_task_indexing():
    tasklist = runforrest.TaskList(autoclean=True)
    # send something that can be indtasklistd:
    task = runforrest.defer(identity, [42])
    # retrieve at index:
    task = runforrest.defer(identity, task[0])
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=1))
    assert tasks[0] == 42


def test_todo_and_done_task_access():
    tasklist = runforrest.TaskList(autoclean=True)
    task = runforrest.defer(identity, 42)
    tasklist.schedule(task)
    todo = list(tasklist.todo_tasks())
    list(tasklist.run(nprocesses=1))
    done = list(tasklist.done_tasks())
    assert task == done[0] == todo[0]


def crash():
    raise Exception('TESTING')


def test_failing_task():
    tasklist = runforrest.TaskList(autoclean=True)
    task = runforrest.defer(crash)
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=1))
    assert len(tasks) == 0
    fail = list(tasklist.fail_tasks())
    assert len(fail) == 1
    assert fail[0] == task
    assert fail[0]._error.args == ('TESTING',)


def test_invalid_task():
    tasklist = runforrest.TaskList(autoclean=True)
    task = runforrest.defer(0) # not a function!
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=1))
    assert len(tasks) == 0
    fail = list(tasklist.fail_tasks())
    assert len(fail) == 1
    assert fail[0] == task
    assert isinstance(fail[0]._error, TypeError)


def test_autoclean_true():
    tasklist = runforrest.TaskList(autoclean=True)
    task = runforrest.defer(crash)
    tasklist.schedule(task)
    list(tasklist.run(nprocesses=1))
    del tasklist
    assert not pathlib.Path('./rf_todo').exists()
    assert not pathlib.Path('./rf_done').exists()
    assert not pathlib.Path('./rf_fail').exists()


def test_autoclean_false():
    tasklist = runforrest.TaskList(autoclean=False)
    task = runforrest.defer(crash)
    tasklist.schedule(task)
    list(tasklist.run(nprocesses=1))
    del tasklist
    for path in ['./rf_todo', './rf_done', './rf_fail']:
        path = pathlib.Path(path)
        assert path.exists()
        for file in path.iterdir():
            file.unlink()
        path.rmdir()
