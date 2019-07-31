@echo OFF

set NSIGHT_AGENT=%~dp0
set PYTHON_HOME=%NSIGHT_AGENT%python
set VENV_BIN=%NSIGHT_AGENT%venv\Scripts
set VENV_PACKAGES_DIR=%NSIGHT_AGENT%venv\Lib\site-packages
set WHEELS_DIR=%NSIGHT_AGENT%wheels
set VIRTUALENV_DIR=%NSIGHT_AGENT%virtualenv

if exist %PYTHON_HOME%\python.exe (
    echo file %PYTHON_HOME%\python.exe is exist ......
) else (
    echo Start to install "%NSIGHT_AGENT%install\python-2.7.5.amd64.msi" ......
    msiexec /i "%NSIGHT_AGENT%install\python-2.7.5.amd64.msi" TARGETDIR=%PYTHON_HOME% /quiet /passive
    echo install python successfully......
)

if not exist C:\Windows\system32\python27.dll (
    copy %PYTHON_HOME%\python27.dll C:\Windows\system32\
) else (
    echo python27.dll already exists
)

echo %path%|findstr /i "%PYTHON_HOME%"&&(goto run)
set path=%PYTHON_HOME%;%path%

:run
if "%1"=="install" call:agentInstall
if "%1"=="uninstall" call:agentUninstall
if "%1"=="start" call:agentStart
if "%1"=="stop" call:agentStop
exit /b

:agentInstall
python %VIRTUALENV_DIR%\virtualenv.py venv
%VENV_BIN%\pip install --no-index --find-links=%WHEELS_DIR% APScheduler diskcache psutil pywin32 ntplib chardet

%VENV_BIN%\python %VENV_BIN%\pywin32_postinstall.py -install
%VENV_PACKAGES_DIR%\win32\pythonservice.exe /register

%VENV_BIN%\python agent_service.py --startup auto install
%VENV_BIN%\python agent_service.py start
goto:eof

:agentUninstall
if exist %VENV_BIN% %VENV_BIN%\python agent_service.py remove
goto:eof

:agentStart
if exist %VENV_BIN% %VENV_BIN%\python agent_service.py start
goto:eof

:agentStop
if exist %VENV_BIN% %VENV_BIN%\python agent_service.py stop
goto:eof
