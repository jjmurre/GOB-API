#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo "Running style checks"
flake8

export GOB_RUN_MODE=TEST

echo "Running unit tests"
pytest

echo "Running coverage tests"
pytest --cov=gobapi --cov-report html --cov-fail-under=99 tests/
