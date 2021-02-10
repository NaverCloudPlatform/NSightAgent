#!/bin/bash

AGENT_ROOT=$(cd `dirname $0`; pwd)
VENV_BIN=$AGENT_ROOT/.venv/bin

if [ ! -d $AGENT_ROOT/wheels ]; then
  $VENV_BIN/pip wheel --wheel-dir=$AGENT_ROOT/wheels APScheduler
  $VENV_BIN/pip wheel --wheel-dir=$AGENT_ROOT/wheels ntplib
fi
