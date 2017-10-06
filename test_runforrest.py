import runforrest
import pathlib


def identity(n):
    return n


def test_run():
    exe = runforrest.Executor(autoclean=True)
    result = runforrest.defer(identity, 42)
    exe.schedule(result)
    results = list(exe.run(nprocesses=4))
    assert results[0] == 42


def test_nested_run():
    exe = runforrest.Executor(autoclean=True)
    result = runforrest.defer(identity, 42)
    result = runforrest.defer(identity, result)
    exe.schedule(result)
    results = list(exe.run(nprocesses=4))
    assert results[0] == 42


def test_multiple_runs(howmany=20):
    exe = runforrest.Executor(autoclean=True)
    for v in range(howmany):
        result = runforrest.defer(identity, v)
        exe.schedule(result)
    results = list(exe.run(nprocesses=10))
    assert len(results) == howmany
    for r, v in zip(sorted(results), range(howmany)):
        assert r == v


def test_multiple_nested_runs(howmany=20):
    exe = runforrest.Executor(autoclean=True)
    for v in range(howmany):
        result = runforrest.defer(identity, v)
        result = runforrest.defer(identity, result)
        exe.schedule(result)
    results = list(exe.run(nprocesses=10))
    assert len(results) == howmany
    for r, v in zip(sorted(results), range(howmany)):
        assert r == v


def test_result_accessor():
    exe = runforrest.Executor(autoclean=True)
    # send something that has an attribute:
    result = runforrest.defer(identity, Exception(42))
    # retrieve the attribute:
    result = runforrest.defer(identity, result.args)
    exe.schedule(result)
    results = list(exe.run(nprocesses=1))
    assert results[0] == (42,)


def test_result_indexing():
    exe = runforrest.Executor(autoclean=True)
    # send something that can be indexed:
    result = runforrest.defer(identity, [42])
    # retrieve at index:
    result = runforrest.defer(identity, result[0])
    exe.schedule(result)
    results = list(exe.run(nprocesses=1))
    assert results[0] == 42


def test_todo_and_done_task_access():
    exe = runforrest.Executor(autoclean=True)
    result = runforrest.defer(identity, 42)
    exe.schedule(result)
    todo = list(exe.todo_tasks())
    list(exe.run(nprocesses=1))
    done = list(exe.done_tasks())
    assert result == done[0] == todo[0]


def crash():
    raise Exception('TESTING')


def test_failing_task():
    exe = runforrest.Executor(autoclean=True)
    result = runforrest.defer(crash)
    exe.schedule(result)
    results = list(exe.run(nprocesses=1))
    assert len(results) == 0
    fail = list(exe.fail_tasks())
    assert len(fail) == 1
    assert fail[0] == result
    assert fail[0]._error.args == ('TESTING',)


def test_invalid_task():
    exe = runforrest.Executor(autoclean=True)
    result = runforrest.defer(0) # not a function!
    exe.schedule(result)
    results = list(exe.run(nprocesses=1))
    assert len(results) == 0
    fail = list(exe.fail_tasks())
    assert len(fail) == 1
    assert fail[0] == result
    assert isinstance(fail[0]._error, TypeError)


def test_autoclean_true():
    exe = runforrest.Executor(autoclean=True)
    result = runforrest.defer(crash)
    exe.schedule(result)
    list(exe.run(nprocesses=1))
    del exe
    assert not pathlib.Path('./rf_todo').exists()
    assert not pathlib.Path('./rf_done').exists()
    assert not pathlib.Path('./rf_fail').exists()


def test_autoclean_false():
    exe = runforrest.Executor(autoclean=False)
    result = runforrest.defer(crash)
    exe.schedule(result)
    list(exe.run(nprocesses=1))
    del exe
    for path in ['./rf_todo', './rf_done', './rf_fail']:
        path = pathlib.Path(path)
        assert path.exists()
        for file in path.iterdir():
            file.unlink()
        path.rmdir()
