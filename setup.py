#!/usr/bin/env python

from distutils.core import setup

with open('README.rst') as f:
    long_description = f.read()

setup(
    name='rpqueue',
    version='0.21',
    description='Use Redis as a priority-enabled and time-based task queue.',
    author='Josiah Carlson',
    author_email='josiah.carlson@gmail.com',
    url='https://github.com/josiahcarlson/rpqueue',
    packages=['rpqueue', 'tests'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    license='GNU LGPL v2.1',
    long_description=long_description,
    requires=['redis'],
)
