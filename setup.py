from setuptools import setup

import pyvertica


setup(
    name='pyvertica',
    version=pyvertica.__version__,
    url='https://github.com/spilgames/pyvertica',
    license='BSD',
    author='Guillaume Roger, Orne Brocaar',
    author_email='datawarehouse@spilgames.com',
    description='Tools for performing batch imports into Vertica',
    long_description=open('README.rst').read(),
    packages=[
        'pyvertica',
        'pyvertica.tests',
        'pyvertica.tests.unit',
    ],
    scripts=[
        'scripts/vertica_batch_import',
    ],
    install_requires=[
        'argparse',
        'logutils',
        'pyodbc==3.0.3',
    ]
)
