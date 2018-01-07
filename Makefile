.DEFAULT_GOAL   := all

.PHONY: all
all: compile

.PHONY: compile
compile:
	python setup.py build

.PHONY: sdist
sdist:
	python setup.py sdist --formats zip

.PHONY: install
install: all
	python setup.py install

.PHONY: clean-all
clean-all: clean

.PHONY: test
test: unit ;

.PHONY: check
check: 
	tox -- --timeout=30 ./tests/unit

.PHONY: check-integration
check-integration:
	tox -- --timeout=30 ./tests/integration
