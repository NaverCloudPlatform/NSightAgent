@echo OFF

set NSIGHT_AGENT=%~dp0
set CONTROLLER_HOME=%NSIGHT_AGENT%..\..
set PYTHON_HOME=%CONTROLLER_HOME%\agent_python
set VENV_BIN=%NSIGHT_AGENT%venv\Scripts
set WHEELS_DIR=%NSIGHT_AGENT%wheels

echo install started
echo %NSIGHT_AGENT%
echo %PYTHON_HOME%

"%PYTHON_HOME%\python" "%NSIGHT_AGENT%virtualenv\virtualenv.py" --no-download -p "%PYTHON_HOME%\python.exe" "%NSIGHT_AGENT%venv"
"%VENV_BIN%\pip" install --no-index --find-links="%WHEELS_DIR%" APScheduler diskcache psutil pywin32 ntplib chardet WMI

echo NSight-Agent installed
exit /b
