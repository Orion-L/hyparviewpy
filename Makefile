PYTHON ?= python3

ifeq (, $(shell which python))
  $(error "$(PYTHON) not found in $(PATH)")
endif

PYTHON_MIN=3.7
PYTHON_VER=$(shell $(PYTHON) -c 'import sys; \
  print("%d.%d"% sys.version_info[0:2])' )
PYTHON_OK=$(shell $(PYTHON) -c 'import sys; \
  print(int(float("%d.%d"% sys.version_info[0:2]) >= $(PYTHON_MIN)))' )

ifeq ($(PYTHON_OK),0)
  $(error "Need python $(PYTHON_VER) >= $(PYTHON_MIN)")
endif

.PHONY: all flake test coverage clean

all: flake test

flake:
	$(PYTHON) -m flake8 hyparviewpy setup.py tests examples


test:
	$(PYTHON) -m coverage run -m unittest -v


coverage: test
	$(PYTHON) -m coverage report -m
	$(PYTHON) -m coverage html


clean:
	rm -rf `find . -name __pycache__`
	rm -f .coverage
	rm -rf htmlcov
