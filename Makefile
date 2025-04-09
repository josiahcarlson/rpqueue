GET_TARGET=grep rpqueue-test-$${target} docker-compose.yaml | sed 's/[ :]//g'
COMPOSE_PREFIX=docker-compose -f docker-compose

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

clean: perms
	-rm -f *.pyc rpqueue/*.pyc README.html MANIFEST
	-rm -rf build

perms:
	-sudo chown ${USER}:${USER} -R .

compose-build-all:
	docker-compose build

compose-up-%:
	for target in $(patsubst compose-up-%,%,$@) ; do \
		${COMPOSE_PREFIX}.yaml up --remove-orphans `${GET_TARGET}`; \
	done

compose-down-%:
	for target in $(patsubst compose-down-%,%,$@) ; do \
		echo ${COMPOSE_PREFIX}.yaml down `${GET_TARGET}`; \
	done

testall:
	# they use the same Redis, so can't run in parallel
	make -j1 test-3.11 test-3.10 test-3.9 test-3.8 test-3.7 test-3.6
	# EOL :/ test-3.5 test-3.4 test-3.3 test-2.7

test-%:
	# the test container runs the tests on up, then does an exit 0 when done
	for target in $(patsubst test-%,%,$@) ; do \
		make compose-up-$${target}; \
	done

upload:
	git tag -f `cat VERSION`
	git push origin -f --tags
	docker-compose run --rm -w /source rpqueue-uploader python3.13 -m build --sdist
	docker-compose run --rm -w /source rpqueue-uploader python3.13 -m twine upload --skip-existing dist/rpqueue-`cat VERSION`.tar.gz
	make perms

docs:
	python3 -c "import rpqueue; open('VERSION', 'w').write(rpqueue.VERSION);"
	docker-compose build rpqueue-uploader
	docker-compose run --rm -w /source rpqueue-uploader $(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	make perms
	cp -r $(BUILDDIR)/html/. docs
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."
