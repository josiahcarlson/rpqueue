SHELL=/bin/bash

clean:
	-rm -f *.pyc rpqueue/*.pyc README.html MANIFEST
	-rm -rf build dist

install:
	python setup.py install

test:
	python2.6 -m tests.test_rpqueue
	python2.7 -m tests.test_rpqueue
	python3.3 -m tests.test_rpqueue
	python3.4 -m tests.test_rpqueue
	python3.5 -m tests.test_rpqueue

upload:
	python setup.py sdist upload
