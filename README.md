# Run, Forrest, Run!

Because sometimes a single Python is just not fast enough.

You have some code that looks like this:

```python
for item in long_list_of_stuff:
    intermediate = prepare(item)
    result = dostuff(intermediate[1], intermediate.thing)
```

And the problem is, both `prepare` and `dostuff` take too long, and
`long_list_of_stuff` is just too damn long. Running the above script
takes *ages*.

What can you do? You could use [IPyParallel][ip]!
Or [Dask.distributed][dd]! Or [multiprocessing][mp]!

[ip]: https://ipyparallel.readthedocs.io/en/latest/
[dd]: https://distributed.readthedocs.io/en/latest/
[mp]: https://docs.python.org/3.6/library/multiprocessing.html

Well, but IPyParallel has kind of a weird API, and Dask.distributed is
hard to set up, and multiprocessing is kind of limited... I've been
there, I've done that, and was not satisfied.

Furthermore, none of the above can handle seriously buggy programs
that throw segfaults, corrupt your file system, leak memory, and
exhaust your process limits. All of this has happened to me when I
tried to run scientific code.

So why is RunForrest better?

1. Understandable. Just short of 200 lines of source code is manageable.
2. No complicated dependencies. A recent version of Python with `dill` is all you need.
3. Simple. The above call graph will now look like this:
4. Robust. Runforrest survives errors, crashes, and even reboots, without losing data.

```python
from runforrest import TaskList, defer
tasklist = TaskList('tasklist')

for item in long_list_of_stuff:
    task = defer(prepare, item)
    task = defer(dostuff, task[1], task.thing)
    tasklist.schedule(task)

for task in tasklist.run(nprocesses=4):
    result = task.returnvalue # or task.errorvalue
```

Wrap your function calls in `defer`, `schedule` them to a `TaskList`,
then `run`. That's all there is to it. Deferred functions return
objects that can be indexed and getattr'd as much as you'd like and
all of that will be resolved once they are `run` (a single return
object will never execute more than one time).

But the best thing is, each `schedule` will just create a file in a
new directory `tasklist/todo`. Then `run` will execute those files,
and put them in `tasklist/done` or `tasklist/fail` depending on
whether there were errors or not.

This solves so many problems. Maybe you want to try to re-run a failed
item? Just copy them back over to `tasklist/todo`, and `run` again. Or
`python runforrest.py --do_raise infile outfile` them manually, and
observe the error first hand!

Yes, it's simple. Stupid simple even, you might say. But it is
debuggable. It doesn't start a web server. It doesn't set up fancy
kernels and messaging systems. It doesn't fork three ways and choke
on its own memory consumption. It's just one simple script.

Then again, maybe this won't run so well on more than a couple of
computers, and it probably still contains bugs and stuff.

Finally, writing this is not my main job, and I won't be able to
answer and fix every pull request at enterprise speed. Please be civil
if I can't respond quickly.

## More Details:

When creating a `TaskList`, it is assumed that you create a new, empty
list, and will keep that list around after you are done. By default,
`TaskList` therefore throws an error if the chosen directory already
exists (`exist_ok=False`) and will keep all items after you are done
(`post_clean=False`).

```python
# new, empty, unique list:
>>> tasklist = TaskList('new_directory')
```

If you are experimenting, and will create and re-create a `TaskList`
over and over, set `exist_ok=True`. This will automatically delete any
tasks in the directory when you instantiate your `TaskList`
(`pre_clean=True`):

```python
>>> # new, empty, pre-existing list:
>>> tasklist = TaskList('existing_directory', exist_ok=True)
```

If you want to add to a pre-existing `TaskList`, set
`pre_clean=False`:

```python
>>> # existing list:
>>> tasklist = TaskList('existing_directory', exist_ok=True, pre_clean=False)
```

If your computer crashed, and you want to continue where you last ran,
set `noschedule_if_exist=True`. This way, all your `schedule`s will be
skipped, and the existing `TaskList` will `run` all remaining TODO
items:

```python
tasklist = TaskList('tasklist', noschedule_if_exist=True)

for item in long_list_of_stuff:
    task = defer(prepare, item)
    task = defer(dostuff, task[1], task.thing)
    tasklist.schedule(task) # will not schedule!

for task in tasklist.run(nprocesses=4): # will run remaining TODOs
    result = task.returnvalue # or task.errorvalue
```

If your tasks are noisy, and litter your terminal with status messages,
you can supply the `TaskList` with a `logfile`. If given, all output,
and some diagnostic information, will be saved to the logfile instead of
the terminal.

### Tasks

Now you are ready to add tasks. A task is any deferred return value
from a function or method call, or plain data:

```python
>>> task = defer(numpy.zeros, 5)
>>> task = defer(dict, alpha='a', beta='b')
>>> task = defer(lambda x: x, 42)
>>> task = defer(42) # behaves like the previous line
```

Tasks can also be used as arguments to deferred function calls:

```python
>>> task1 = defer(dict, value=42)
>>> task2 = dever(lambda x: x['value'], task1)
```

You can test-run your tasks using `evaluate`:

```python
>>> evaluate(task1)
{'value': 42}
>>> evaluate(task2)
42
```

Tasks can also access attributes or indexes of arguments of tasks.
Each task attribute or task index is itself a task, that can be
indexed or attributed further:

```python
>>> task1 = defer(dict, value=42)
>>> task2 = defer(lambda x: x, task1['value'])
>>> evaluate(task2)
42
>>> task1 = defer({'value': 42})
>>> task2 = task1['value'] # also returns a Task!
>>> evaluate(task2)
42
>>> task1 = defer(list, [23, 42])
>>> task2 = defer(lambda x: x, task1[1]) # or task2 = task1[1]
>>> evaluate(task2)
42
>>> task1 = defer(Exception, 'an error message')
>>> task2 = defer(lambda x: x, task1.args) # or task2 = task1.args
>>> evaluate(task2)
('an error message',)
>>> task2 = defer(lambda x: x, task1.args[0]) # or task2 = task1.args[0]
'an error message'
```

This way, you can build arbitrary, deep trees of functions and methods
that process your data.

### Scheduling and Running

When you finally have a task that evaluates to
the result you want, you can schedule it on a `TaskList`:

```python
>>> tasklist.schedule(task2)
```

If you want, you can attach some metadata that you can retrieve later.
This can be very useful if you are running many many tasks, and want
to sort and organize them later:

```python
>>> tasklist.schedule(task2, metadata={'date': date.date()})
```

Once you have scheduled all the tasks you want, you can run them:

```python
>>> for task in tasklist.run(nprocesses=10):
>>>     do_something_with(task)
```

This will run each of your tasks in its own process, with ten
processes active at any time. In its default, this will yield every
finished task, with either the return value in `task.returnvalue`, or
the error in `task.errorvalue` if there was an error, and the
aforementioned metadata in `task.metadata`.

If you want to get more feedback for failing tasks, you can run them
with `print_errors=True`, which will print the full stack trace of
every error the moment it occurs.

Sometimes, `dill` won't catch some local functions or globals, and
your tasks will fail. In that case, set `save_session=True` and try
again.

Sometimes, tasks are unreliable, and need to be stopped before they
bring your computer to a halt. You can tell RunForrest to kill tasks if
they take longer than a set amount of seconds, by adding the `autokill`
argument:

```python
>>> for task in tasklist.run(autokill=300): # kill after five minutes
>>>     ...
```

Note that `autokill` will `SIGKILL` the whole process group, and will
not give the processes a chance to react or clean up after themselves.
This is the only way to reliably kill stuck processes, and all their
threads and child-processes they might have spawned. 

### Accessing Tasks

At any time, you can inspect all currently-scheduled tasks with

```python
>>> for task in tasklist.todo_tasks():
>>>     do_something_with(task)
```

or, accordingly for done and failed tasks:

```python
>>> for task in tasklist.done_tasks():
>>>     do_something_with(task)
>>> for task in tasklist.fail_tasks():
>>>     do_something_with(task)
```

These three task lists correspond to `*.pkl` files in the
`{tasklist}/todo`, `{tasklist}/done`, and `{tasklist}/fail` directory,
respectively. For example, you can manually re-schedule failed tasks
by moving them from the `{tasklist}/fail` to `{tasklist}/todo`.

### Cleaning

If you want to get rid of tasks, you can use 

```python
>>> tasklist.clean()
```

You can decide manually if you want to remove only selected tasks with
`clean_todo=True`, `clean_done=True`, and `clean_fail=True`.

### Running Tasks Manually

If one of your tasks is failing, and you can't figure out why, it is
useful to manually run just a single task. For this, you can use
`tasklist` as an executable library:

```bash
$ python -m runforrest path/to/task.pkl path/to/result.pkl -r
```

where `-r` raises any error; Alternatively, `-p` would print them just
like `run` does with `print_errors=True`. If needed, you can load a
session file with `-s path/to/session.pkl`.

## FAQ:

- *Code that calls Numpy crashes in `pthread_create`*: By default,
  Numpy creates a large number of threads. If you run many Numpies in
  parallel, this can exhaust your thread limit. You can either raise
  your thread limit 
  (`resources.setrlimit(resources.RLIMIT_NPROC, ...)`) or force Numpy
  to use only one thread per process 
  (`os.putenv('OMP_NUM_THREADS', '1')`).

- *Tasks fail because `"name '<module>' is not defined"`*: Sometimes,
  serialization can fail if `run` is called from a 
  `if __name__ == "__main__"` block. Set `save_session=True` in run
  to fix this problem (for a small performance overhead).
