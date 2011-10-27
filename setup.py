#!/usr/bin/env python

from distutils.core import setup

with open('README') as f:
    long_description = f.read()

setup(
    name='rpqueue',
    version='.12',
    description='Use Redis as a priority-enabled and time-based task queue.',
    author='Josiah Carlson',
    author_email='josiah.carlson@gmail.com',
    url='https://github.com/josiahcarlson/rpqueue',
    packages=['rpqueue', 'tests'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Programming Language :: Python',
    ],
    license='GNU GPL v2.0',
    long_description=long_description,
)
