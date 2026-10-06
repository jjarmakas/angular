@echo off
rem Runs mqwf with the MQ client classes from the local MQ installation.
rem   mqwf list
rem   mqwf status   [WORKFLOW...]
rem   mqwf shutdown [WORKFLOW...] [--dry-run] [--yes] [--no-rollback] [--timeout SECONDS]
rem   mqwf start    [WORKFLOW...] [--dry-run] [--yes]
rem Without WORKFLOW names, all workflows from the "workflows" property are processed.
rem The exit code is passed through for schedulers (see README).
setlocal
if not defined MQ_JAVA_LIB set "MQ_JAVA_LIB=C:\Program Files\IBM\MQ\java\lib"
if defined JAVA_HOME (
  set "JAVA=%JAVA_HOME%\bin\java.exe"
) else (
  set "JAVA=C:\Program Files\IBM\MQ\java\jre\bin\java.exe"
)
if not exist "%JAVA%" (
  echo Java not found at "%JAVA%". Set JAVA_HOME to a Java 8 runtime.
  exit /b 9
)

rem Bindings mode needs the native MQ library from java\lib64.
"%JAVA%" -Djava.library.path="%MQ_JAVA_LIB%64" ^
  -cp "%~dp0mqwf.jar;%MQ_JAVA_LIB%\com.ibm.mq.allclient.jar" ^
  com.ops.mqwf.Main --config "%~dp0mqwf.properties" %*
exit /b %ERRORLEVEL%
