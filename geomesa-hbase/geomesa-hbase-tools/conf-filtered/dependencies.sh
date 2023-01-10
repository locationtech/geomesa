#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This file lists the dependencies required for running the geomesa-accumulo command-line tools.
# Usually these dependencies will be provided by the environment (e.g. ACCUMULO_HOME).
# Update the versions as required to match the target environment.

hbase_install_version="%%hbase.version.recommended%%"
hbase_thirdparty_install_version="%%hbase.thirdparty.version.recommended%%"
hadoop_install_version="%%hadoop.version%%"
zookeeper_install_version="%%zookeeper.version.recommended%%"
# required for hadoop - make sure it corresponds to the hadoop installed version
guava_install_version="%%hbase.guava.version%%"

function version_ge() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"; }

# gets the dependencies for this module
# args:
#   $1 - current classpath
function dependencies() {
  local classpath="$1"

  local hbase_version="$hbase_install_version"
  local hbase_thirdparty_version="$hbase_thirdparty_install_version"
  local hadoop_version="$hadoop_install_version"
  local zk_version="$zookeeper_install_version"

  if [[ -n "$classpath" ]]; then
    hbase_version="$(get_classpath_version hbase-client "$classpath" "$hbase_version")"
    hbase_thirdparty_version="$(get_classpath_version hbase-shaded-protobuf "$classpath" "$hbase_thirdparty_version")"
    hadoop_version="$(get_classpath_version hadoop-common "$classpath" "$hadoop_version")"
    zk_version="$(get_classpath_version zookeeper "$classpath" "$zk_version")"
  fi

  declare -a gavs=(
    "org.apache.hbase:hbase-client:${hbase_version}:jar"
    "org.apache.hbase:hbase-common:${hbase_version}:jar"
    "org.apache.hbase:hbase-hadoop-compat:${hbase_version}:jar"
    "org.apache.hbase:hbase-protocol:${hbase_version}:jar"
    "com.google.protobuf:protobuf-java:2.5.0:jar"
    "org.apache.zookeeper:zookeeper:${zk_version}:jar"
    "org.apache.hadoop:hadoop-auth:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-common:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs-client:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-mapreduce-client-core:${hadoop_version}:jar"
    "com.fasterxml.woodstox:woodstox-core:5.3.0:jar"
    "org.codehaus.woodstox:stax2-api:4.2.1:jar"
    "commons-collections:commons-collections:3.2.2:jar"
    "commons-configuration:commons-configuration:1.6:jar"
    "commons-lang:commons-lang:2.6:jar"
    "commons-logging:commons-logging:1.1.3:jar"
    "commons-cli:commons-cli:1.2:jar"
    "commons-io:commons-io:2.5:jar"
    "org.apache.commons:commons-configuration2:2.8.0:jar"
    "javax.servlet:servlet-api:2.4:jar"
    "io.netty:netty-buffer:%%netty.version%%:jar"
    "io.netty:netty-codec:%%netty.version%%:jar"
    "io.netty:netty-common:%%netty.version%%:jar"
    "io.netty:netty-handler:%%netty.version%%:jar"
    "io.netty:netty-resolver:%%netty.version%%:jar"
    "io.netty:netty-transport:%%netty.version%%:jar"
    "io.netty:netty-transport-classes-epoll:%%netty.version%%:jar"
    "io.netty:netty-transport-native-epoll:%%netty.version%%:jar:linux-x86_64"
    "io.netty:netty-transport-native-unix-common:%%netty.version%%:jar"
    "io.netty:netty:3.10.6.Final:jar"
    # htrace 3 required for hadoop before 2.8
    # htrace 4 required for hadoop 2.8 and later
    # since they have separate package names, should be safe to install both
    "org.apache.htrace:htrace-core:3.1.0-incubating:jar"
    "org.apache.htrace:htrace-core4:4.1.0-incubating:jar"
    "io.dropwizard.metrics:metrics-core:3.2.6:jar"
    "com.google.guava:guava:${guava_install_version}:jar"
  )

  local hbase_maj_ver
  hbase_maj_ver="$([[ "$hbase_version" =~ ([0-9][0-9]*)\. ]] && echo "${BASH_REMATCH[1]}")"

  # additional dependencies that depend on the major version
  if [[ "$hbase_maj_ver" -ge 2 ]]; then
    gavs+=(
      "org.apache.hbase:hbase-mapreduce:${hbase_version}:jar"
      "org.apache.hbase:hbase-protocol-shaded:${hbase_version}:jar"
      "org.apache.hbase.thirdparty:hbase-shaded-miscellaneous:${hbase_thirdparty_version}:jar"
      "org.apache.hbase.thirdparty:hbase-shaded-netty:${hbase_thirdparty_version}:jar"
      "org.apache.hbase.thirdparty:hbase-shaded-protobuf:${hbase_thirdparty_version}:jar"
<<<<<<< HEAD
      "org.apache.hbase.thirdparty:hbase-unsafe:${hbase_thirdparty_version}:jar"
      "io.opentelemetry:opentelemetry-api:1.15.0:jar"
      "io.opentelemetry:opentelemetry-context:1.15.0:jar"
      "io.opentelemetry:opentelemetry-semconv:1.15.0-alpha:jar"
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
    )
  else
    gavs+=(
      "com.yammer.metrics:metrics-core:2.2.0:jar"
    )
  fi

  # add hadoop 3+ jars if needed
  local hadoop_maj_ver
  hadoop_maj_ver="$([[ "$hadoop_version" =~ ([0-9][0-9]*)\. ]] && echo "${BASH_REMATCH[1]}")"
  if [[ "$hadoop_maj_ver" -ge 3 ]]; then
    gavs+=(
      "org.apache.hadoop:hadoop-client-api:${hadoop_version}:jar"
      "org.apache.hadoop:hadoop-client-runtime:${hadoop_version}:jar"
    )
  fi

  # compare the version of zookeeper to determine if we need zookeeper-jute (version >= 3.5.5)
  JUTE_FROM_VERSION="3.5.5"
  if version_ge "${zk_version}" $JUTE_FROM_VERSION; then
    gavs+=(
      "org.apache.zookeeper:zookeeper-jute:${zk_version}:jar"
    )
  fi

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

# gets any dependencies that should be removed from the classpath for this module
# args:
#   $1 - current classpath
function exclude_dependencies() {
  echo ""
}
