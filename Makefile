
.PHONY: check check-verbose html-docs

check: build-inplace
	python -m testtools.run discover

build-inplace:
	python setup.py build_ext -i

check-verbose:
	python -c "import unittest, sys; from testtools import run; run.TestProgram(argv=sys.argv, testRunner=unittest.TextTestRunner(verbosity=2), stdout=sys.stdout)" discover

html-docs:
	cd html-docs; make html
