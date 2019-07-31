@echo OFF

set NSIGHT_AGENT=%~dp0

AGENT_PACKAGE=`dirname $0`
AGENT_ROOT=$(cd $AGENT_PACKAGE; pwd)

uninstall_agent() {
  echo 'uninstalling agent'
  echo $AGENT_ROOT
  echo $AGENT_PACKAGE
  cd $AGENT_ROOT/.. && rm -rf $AGENT_PACKAGE
  echo 'NSight-Agent uninstalled'
}

uninstall_agent