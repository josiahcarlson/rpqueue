FILES=`ls docker-compose.*.yaml`
GET_TARGET=grep rpqueue-test docker-compose.$${target}.yaml | sed 's/[ :]//g'
COMPOSE_PREFIX=docker-compose -f docker-compose.

# sphinx options
SHELL=/bin/bash
# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
PAPER         =
BUILDDIR      = _build
# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .
# the i18n builder cannot share the environment and doctrees with the others
I18NSPHINXOPTS  = $(PAPEROPT_$(PAPER)) $(SPHINXOPTS)



.PHONY: clean docs testall

clean:
	-rm -f *.pyc rpqueue/*.pyc README.html MANIFEST
	-rm -rf build dist

install:
	python setup.py install

compose-build-all:
	echo ${FILES}
	# A little nasty here, but we can do it!
	# The grep finds the 'rpqueue-test-<service version>' in the .yaml
	# The sed removes extra spaces and colons
	# Which we pass into our rebuild
	for target in ${FILES} ; do \
		docker-compose -f $${target} build -- `${GET_TARGET}` redis-task-broker; \
	done

compose-build-%:
	for target in $(patsubst compose-build-%,%,$@) ; do \
		${COMPOSE_PREFIX}$${target}.yaml build `${GET_TARGET}`; \
	done


compose-up-%:
	for target in $(patsubst compose-up-%,%,$@) ; do \
		${COMPOSE_PREFIX}$${target}.yaml up --remove-orphans `${GET_TARGET}`; \
	done

compose-down-%:
	for target in $(patsubst compose-down-%,%,$@) ; do \
		echo ${COMPOSE_PREFIX}$${target}.yaml down `${GET_TARGET}`; \
	done

testall:
	# they use the same Redis, so can't run in parallel
	make -j1 test-3.11 test-3.10 test-3.9 test-3.8 test-3.7 test-3.6
	# EOL :/ test-3.5 test-3.4 test-3.3 test-2.7

test-%:
	# the test container runs the tests on up, then does an exit 0 when done
	for target in $(patsubst test-%,%,$@) ; do \
		make compose-build-$${target} && make compose-up-$${target}; \
	done

upload:
	git tag `cat VERSION`
	git push origin --tags
	python3.6 setup.py sdist
	python3.6 -m twine upload --verbose dist/rpqueue-`cat VERSION`.tar.gz

docs:
	python -c "import rpqueue; open('VERSION', 'w').write(rpqueue.VERSION);"
	make compose-build-docs
	docker-compose -f docker-compose.docs.yaml run rpqueue-test-docs $(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	cp -r $(BUILDDIR)/html/. docs
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."
