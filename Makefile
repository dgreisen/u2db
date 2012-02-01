
.PHONY: check check-verbose html-docs

check: build-inplace
	python -m testtools.run discover

build-inplace: src/u1db_schema.c
	python setup.py build_ext -i

src/u1db_schema.c: u1db/backends/dbschema.sql
	python sql_to_c.py u1db/backends/dbschema.sql u1db__schema src/u1db_schema.c

check-verbose:
	python -c "import unittest, sys; from testtools import run; run.TestProgram(argv=sys.argv, testRunner=unittest.TextTestRunner(verbosity=2), stdout=sys.stdout)" discover

html-docs:
	cd html-docs; make html
