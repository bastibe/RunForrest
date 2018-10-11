import runforrest
import pathlib

def identity(n):
    return n


def test_run():
    tasklist = runforrest.TaskList('tmp', post_clean=True)
    task = runforrest.defer(identity, 42)
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=4))
    assert tasks[0].returnvalue == 42


def test_data_task():
    tasklist = runforrest.TaskList('tmp', post_clean=True)
    task = runforrest.defer(42)
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=4))
    assert tasks[0].returnvalue == 42


def test_nested_run():
    tasklist = runforrest.TaskList('tmp', post_clean=True)
    task = runforrest.defer(identity, 42)
    task = runforrest.defer(identity, task)
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=4))
    assert tasks[0].returnvalue == 42


def test_multiple_runs(howmany=20):
    tasklist = runforrest.TaskList('tmp', post_clean=True)
    for v in range(howmany):
        task = runforrest.defer(identity, v)
        tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=10))
    assert len(tasks) == howmany
    for r, v in zip(sorted(t.returnvalue for t in tasks), range(howmany)):
        assert r == v


def test_multiple_nested_runs(howmany=20):
    tasklist = runforrest.TaskList('tmp', post_clean=True)
    for v in range(howmany):
        task = runforrest.defer(identity, v)
        task = runforrest.defer(identity, task)
        tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=10))
    assert len(tasks) == howmany
    for r, v in zip(sorted(t.returnvalue for t in tasks), range(howmany)):
        assert r == v


def test_task_accessor():
    tasklist = runforrest.TaskList('tmp', post_clean=True)
    # send something that has an attribute:
    task = runforrest.defer(identity, Exception(42))
    # retrieve the attribute:
    task = runforrest.defer(identity, task.args)
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=1))
    assert tasks[0].returnvalue == (42,)


def test_task_indexing():
    tasklist = runforrest.TaskList('tmp', post_clean=True)
    # send something that can be indexed:
    task = runforrest.defer(identity, [42])
    # retrieve at index:
    task = runforrest.defer(identity, task[0])
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=1))
    assert tasks[0].returnvalue == 42


def test_todo_and_done_task_access():
    tasklist = runforrest.TaskList('tmp', post_clean=True)
    task = runforrest.defer(identity, 42)
    tasklist.schedule(task)
    todo = list(tasklist.todo_tasks())
    list(tasklist.run(nprocesses=1))
    done = list(tasklist.done_tasks())
    assert task == done[0] == todo[0]


def test_metadata():
    tasklist = runforrest.TaskList('tmp', post_clean=True)
    task = runforrest.defer(identity, 42)
    tasklist.schedule(task, metadata='the truth')
    list(tasklist.run(nprocesses=1))
    done = list(tasklist.done_tasks())
    assert done[0].metadata == 'the truth'


def crash():
    raise Exception('TESTING')


def test_failing_task():
    tasklist = runforrest.TaskList('tmp', post_clean=True)
    task = runforrest.defer(crash)
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=1))
    assert len(tasks) == 1
    assert tasks[0] == task
    fail = list(tasklist.fail_tasks())
    assert len(fail) == 1
    assert fail[0] == task
    assert fail[0].errorvalue.args == ('TESTING',)
    done = list(tasklist.done_tasks())
    assert len(done) == 0


def test_post_clean_true():
    tasklist = runforrest.TaskList('tmp', post_clean=True)
    task = runforrest.defer(crash)
    tasklist.schedule(task)
    list(tasklist.run(nprocesses=1))
    del tasklist
    assert not pathlib.Path('tmp').exists()
    assert not pathlib.Path('tmp/todo').exists()
    assert not pathlib.Path('tmp/done').exists()
    assert not pathlib.Path('tmp/fail').exists()


def test_post_clean_false():
    tasklist = runforrest.TaskList('tmp', post_clean=False)
    task = runforrest.defer(crash)
    tasklist.schedule(task)
    list(tasklist.run(nprocesses=1))
    del tasklist
    for path in ['tmp/todo', 'tmp/done', 'tmp/fail', 'tmp']:
        path = pathlib.Path(path)
        assert path.exists()
        for file in path.iterdir():
            file.unlink()
        path.rmdir()


def test_noschedule():
    tasklist = runforrest.TaskList('tmp')
    task = runforrest.defer(identity, 42)
    tasklist.schedule(task)
    # do not run, but reopen:
    tasklist = runforrest.TaskList('tmp', noschedule_if_exist=True, post_clean=True)
    task = runforrest.defer(identity, 42)
    tasklist.schedule(task) # should now be a no-op
    tasks = list(tasklist.run(nprocesses=4))
    assert len(tasks) == 1
    assert tasks[0].returnvalue == 42


def test_logging():
    logfile = pathlib.Path('tmp.log')
    if logfile.exists():
        logfile.unlink()
    tasklist = runforrest.TaskList('tmp', post_clean=True, logfile=logfile)
    task = runforrest.defer(identity, 42)
    tasklist.schedule(task)
    tasks = list(tasklist.run(nprocesses=4))
    with logfile.open() as f:
        lines = list(f)
        assert len(lines) == 3
        assert 'schedule' in lines[0]
        assert 'start' in lines[1]
        assert 'done' in lines[2]
    logfile.unlink()
