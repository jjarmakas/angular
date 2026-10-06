@echo off
rem Builds mqwf.jar with plain javac (no Maven or internet access needed on the build host).
rem Needs a Java 8 JDK: set JAVA_HOME, e.g. to the JDK shipped with ACE (<ACE install>\common\jdk).
setlocal
if not defined MQ_JAVA_LIB set "MQ_JAVA_LIB=C:\Program Files\IBM\MQ\java\lib"
if not defined JAVA_HOME (
  echo JAVA_HOME is not set. Point it at a Java 8 JDK.
  exit /b 1
)
set "MQ_JAR=%MQ_JAVA_LIB%\com.ibm.mq.allclient.jar"
if not exist "%MQ_JAR%" (
  echo Cannot find %MQ_JAR%. Set MQ_JAVA_LIB to the MQ java\lib directory.
  exit /b 1
)

set "OUT=%~dp0build"
if exist "%OUT%" rmdir /s /q "%OUT%"
mkdir "%OUT%\classes"

dir /s /b "%~dp0src\main\java\*.java" > "%OUT%\sources.txt"
"%JAVA_HOME%\bin\javac" -source 1.8 -target 1.8 -encoding UTF-8 -cp "%MQ_JAR%" -d "%OUT%\classes" @"%OUT%\sources.txt"
if errorlevel 1 exit /b 1

"%JAVA_HOME%\bin\jar" cfe "%~dp0mqwf.jar" com.ops.mqwf.Main -C "%OUT%\classes" .
if errorlevel 1 exit /b 1
echo Built %~dp0mqwf.jar
