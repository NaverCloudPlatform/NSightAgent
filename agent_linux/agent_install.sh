#!/bin/bash

AGENT_ROOT=$(cd `dirname $0`; pwd)
VIRTUALENV_DIR="${AGENT_ROOT}/virtualenv"
VENV_BIN="${AGENT_ROOT}/.venv/bin"
WHEELS_DIR="${AGENT_ROOT}/wheels"
CONTROLLER_HOME=$AGENT_ROOT/../..
PYTHON_HOME=$CONTROLLER_HOME/agent_python

install_agent() {
  $PYTHON_HOME/bin/python ${VIRTUALENV_DIR}/virtualenv.py --no-download -p $PYTHON_HOME/bin/python2.7 $AGENT_ROOT/.venv
  ${VENV_BIN}/pip install --no-index --find-links=${WHEELS_DIR} APScheduler diskcache ntplib

  echo "NSight-Agent installed"
}

install_agent
