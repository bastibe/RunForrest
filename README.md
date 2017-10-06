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

So why is RunForrest better? 

1. Understandable. Just short of 200 lines of source code is manageable.
2. No complicated dependencies. A recent version of Python with `dill` and `tqdm` is all you need.
3. Simple. The above call graph will now look like this:

```python
from runforrest import Executor, defer
exe = Executor()

for item in long_list_of_stuff:
    intermediate = defer(prepare, item)
    result = defer(dostuff, intermediate[1], intermediate.thing)
    exe.schedule(result)
    
for result in exe.run(nprocesses=4):
    # use result
```

Wrap your function calls in `defer`, `schedule` them on an `Executor`,
then `run`. That's all there is to it. Deferred functions return
objects that can be indexed and getattr'd as much as you'd like and
all of that will be resolved once they are `run` (a single return
object will never execute more than one time).

But the best thing is, each `schedule` will just create a file in a
new directory `rf_todo`. Then `run` will execute those files, and put
them in `rf_done` or `rf_fail` depending on whether there were errors
or not.

This solves so many problems. Maybe you want to try to re-run a failed
item? Just copy them back over to `rf_todo`, and `run` again. Or
`python runforrest.py --do_raise infile outfile` them manually, and
observe the error first hand!

Yes, it's simple. Stupid simple even, you might say. But it is
debuggable. It doesn't start a web server. It doesn't set up fancy
kernels and messaging systems. It doesn't fork three ways and chokes
on its own memory consumption. It's just one simple script.

Then again, maybe this won't run so well on more than a couple of
computers, and it probably still contains bugs and stuff.

Finally, writing this is not my main job, and I won't be able to
answer and fix every pull request at enterprise speed. Please be civil
if I can't respond quickly.
