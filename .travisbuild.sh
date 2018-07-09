#!/bin/bash

export PING_SLEEP=60s
export WORKDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export BUILD_OUTPUT=$WORKDIR/build.out

touch $BUILD_OUTPUT

# set up a repeating loop to send some output to Travis
echo [INFO] $(date -u '+%F %T UTC') - build starting
bash -c "while true; do sleep $PING_SLEEP; echo [INFO] \$(date -u '+%F %T UTC') - build continuing...; done" &
PING_LOOP_PID=$!

# first build using zinc
./build/mvn clean install -DskipTests -T4 2>&1 | tee -a $BUILD_OUTPUT | grep -e 'Building GeoMesa' -e '\(maven-surefire-plugin\|maven-jar-plugin\|scala-maven-plugin.*:compile\)'
RESULT=${PIPESTATUS[0]} # capture the status of the maven build

if [[ $RESULT -ne 0 ]]; then
  # dump out the end of the build log, to show success or errors
  tail -500 $BUILD_OUTPUT
  echo -e "[ERROR] Build failed!\n"
else
  # now run tests - using the maven executable, as zinc uses too much memory
  # download the plugin first, then run in offline mode (otherwise seems to download jars again?)
  mvn dependency:resolve-plugins -DincludeArtifactIds=maven-surefire-plugin 2>&1 | tee -a $BUILD_OUTPUT
  RESULT=${PIPESTATUS[0]} # capture the status of the maven build	
  if [[ $RESULT -ne 0 ]]; then
    # dump out the end of the build log, to show success or errors
    tail -500 $BUILD_OUTPUT
    echo -e "[ERROR] Build failed!\n"
  else
    mvn -o surefire:test -DargLine="-Duser.timezone=UTC -Xmx4g -XX:-UseGCOverheadLimit -Djava.net.preferIPv4Stack=true -Djava.awt.headless=true -Dgeomesa.scan.ranges.target=500" 2>&1 | tee -a $BUILD_OUTPUT | grep -e 'Building GeoMesa' -e '\(maven-surefire-plugin\|maven-jar-plugin\|scala-maven-plugin.*:compile\)'
    RESULT=${PIPESTATUS[0]} # capture the status of the maven build

    # dump out the end of the build log, to show success or errors
    tail -500 $BUILD_OUTPUT

    if [[ $RESULT -ne 0 ]]; then
      echo -e "[ERROR] Build failed!\n"
    fi
  fi
fi

# nicely terminate the ping output loop
kill $PING_LOOP_PID

# exit with the result of the maven build to pass/fail the travis build
exit $RESULT
