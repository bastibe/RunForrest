import runforrest


def identity(n):
    return n


def test_run():
    exe = runforrest.Executor()
    result = runforrest.defer(identity, 42)
    exe.schedule(result)
    exe.run(nprocesses=4)
    results, = list(exe.gather())
    exe.clean()
    assert results._result == 42


def test_nested_run():
    exe = runforrest.Executor()
    result = runforrest.defer(identity, 42)
    result = runforrest.defer(identity, result)
    exe.schedule(result)
    exe.run(nprocesses=4)
    results, = list(exe.gather())
    exe.clean()
    assert results._result == 42


def test_several_runs(howmany=20):
    exe = runforrest.Executor()
    for v in range(howmany):
        result = runforrest.defer(identity, v)
        exe.schedule(result)
    exe.run(nprocesses=10)
    results = [r._result for r in exe.gather()]
    exe.clean()
    assert len(results) == howmany
    for r, v in zip(sorted(results), range(howmany)):
        assert r == v


def test_several_nested_runs(howmany=20):
    exe = runforrest.Executor()
    for v in range(howmany):
        result = runforrest.defer(identity, v)
        result = runforrest.defer(identity, result)
        exe.schedule(result)
    exe.run(nprocesses=10)
    results = [r._result for r in exe.gather()]
    exe.clean()
    assert len(results) == howmany
    for r, v in zip(sorted(results), range(howmany)):
        assert r == v
