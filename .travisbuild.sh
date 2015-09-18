#!/bin/bash
# Abort on Error
set -e

export PING_SLEEP=60s
export WORKDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export BUILD_OUTPUT=$WORKDIR/build.out

touch $BUILD_OUTPUT

# set up a repeating loop to send some output to Travis
echo [INFO] $(date -u '+%F %T UTC') - build starting
bash -c "while true; do sleep $PING_SLEEP; echo [INFO] \$(date -u '+%F %T UTC') - build continuing...; done" &
PING_LOOP_PID=$!

# build using the maven executable, not the zinc maven compiler (which uses too much memory)
mvn clean install -Ptravis-ci 2>&1 | tee -a $BUILD_OUTPUT | grep -e '^\[INFO\] Building GeoMesa' -e '^\[INFO\] --- \(maven-surefire-plugin\|maven-install-plugin\|scala-maven-plugin.*:compile\)'
RESULT=${PIPESTATUS[0]} # capture the status of the maven build

if [ $RESULT -ne 0 ]; then
  echo -e "[ERROR] Build failed!\n"
fi

# dump out the end of the build log, to show success or errors
tail -500 $BUILD_OUTPUT

# nicely terminate the ping output loop
kill $PING_LOOP_PID

# exit with the result of the maven build to pass/fail the travis build
exit $RESULT
