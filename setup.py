from setuptools import setup

setup(
    name='runforrest',
    version='0.4.3',
    description='Batch and run your code in parallel. Simply.',
    author='Bastian Bechtold',
    author_email='basti@bastibe.de',
    url='https://github.com/bastibe/RunForrest',
    license='BSD 3-clause',
    py_modules=['runforrest'],
    install_requires=['dill'],
    python_requires='>=3.4'
)
