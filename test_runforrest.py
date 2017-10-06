import runforrest


def identity(n):
    return n


def test_run():
    exe = runforrest.Executor()
    result = runforrest.defer(identity, 42)
    exe.schedule(result)
    exe.run(nprocesses=4)
    results = list(exe.gather())
    exe.clean()
    assert results[0]._value == 42


def test_nested_run():
    exe = runforrest.Executor()
    result = runforrest.defer(identity, 42)
    result = runforrest.defer(identity, result)
    exe.schedule(result)
    exe.run(nprocesses=4)
    results = list(exe.gather())
    exe.clean()
    assert results[0]._value == 42


def test_multiple_runs(howmany=20):
    exe = runforrest.Executor()
    for v in range(howmany):
        result = runforrest.defer(identity, v)
        exe.schedule(result)
    exe.run(nprocesses=10)
    results = [r._value for r in exe.gather()]
    exe.clean()
    assert len(results) == howmany
    for r, v in zip(sorted(results), range(howmany)):
        assert r == v


def test_multiple_nested_runs(howmany=20):
    exe = runforrest.Executor()
    for v in range(howmany):
        result = runforrest.defer(identity, v)
        result = runforrest.defer(identity, result)
        exe.schedule(result)
    exe.run(nprocesses=10)
    results = [r._value for r in exe.gather()]
    exe.clean()
    assert len(results) == howmany
    for r, v in zip(sorted(results), range(howmany)):
        assert r == v


def test_result_accessor():
    exe = runforrest.Executor()
    # send something that has an attribute:
    result = runforrest.defer(identity, Exception(42))
    # retrieve the attribute:
    result = runforrest.defer(identity, result.args)
    exe.schedule(result)
    exe.run(nprocesses=1)
    results = list(exe.gather())
    exe.clean()
    assert results[0]._value == (42,)


def test_result_indexing():
    exe = runforrest.Executor()
    # send something that can be indexed:
    result = runforrest.defer(identity, [42])
    # retrieve at index:
    result = runforrest.defer(identity, result[0])
    exe.schedule(result)
    exe.run(nprocesses=1)
    results = list(exe.gather())
    exe.clean()
    assert results[0]._value == 42
