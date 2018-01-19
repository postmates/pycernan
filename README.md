# pycernan

[![Build Status](https://travis-ci.org/postmates/pycernan.svg?branch=master)](https://travis-ci.org/postmates/pycernan) [![Codecov](https://img.shields.io/codecov/c/github/postmates/pycernan.svg)](https://codecov.io/gh/postmates/pycernan)

A Python client for [Cernan](https://github.com/postmates/cernan).

## Available Clients

`pycernan` supports clients for the following Cernan sources:

* [Avro](./pycernan/avro/README.md)

## Installing

The following may require elevated system privileges depending
on your environment:

```
make install
```

## Developing

### Etiquette

This project makes use of flake8 and git hooks to enforce style.

Before altering code, install these components by running:

```
./bin/setup_hooks.sh
```

The above command will install flake8 and configure our custom git hooks
within your environment.

### Running Unit Tests

#### Requirements

* Tox

```
make check
```
