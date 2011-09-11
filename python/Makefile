
.PHONY: check build_inplace

all: build_inplace


check: build_inplace
	nosetests u1db

build_inplace:
	python setup.py build_ext -i
