SHELL=/bin/bash

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
PAPER         =
BUILDDIR      = _build

# User-friendly check for sphinx-build
ifeq ($(shell which $(SPHINXBUILD) >/dev/null 2>&1; echo $$?), 1)
$(error The '$(SPHINXBUILD)' command was not found. Make sure you have Sphinx installed, then set the SPHINXBUILD environment variable to point to the full path of the '$(SPHINXBUILD)' executable. Alternatively you can add the directory with the executable to your PATH. If you don't have Sphinx installed, grab it from http://sphinx-doc.org/)
endif

# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .
# the i18n builder cannot share the environment and doctrees with the others
I18NSPHINXOPTS  = $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .

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
	python3.6 -m tests.test_rpqueue

upload:
	git tag `cat VERSION`
	git push origin --tags
	python setup.py sdist upload

docs:
	python -c "import rpqueue; open('VERSION', 'wb').write(rpqueue.VERSION);"
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	cd _build/html/ && zip -r9 ../../rpqueue_docs.zip * && cd ../../
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."
