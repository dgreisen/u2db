
.PHONY: check check-verbose html-docs

check:
	python -m testtools.run discover

check-verbose:
	python -c "import unittest, sys; from testtools import run; run.TestProgram(argv=sys.argv, testRunner=unittest.TextTestRunner(verbosity=2), stdout=sys.stdout)" discover

html-docs:
	cd html-docs; make html