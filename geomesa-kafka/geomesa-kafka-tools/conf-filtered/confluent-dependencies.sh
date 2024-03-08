#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This file lists the dependencies required for running the geomesa-kafka command-line tools.
# Usually these dependencies will be provided by the environment (e.g. KAFKA_HOME).
# Update the versions as required to match the target environment.

confluent_install_version="%%confluent.version.recommended%%"

function dependencies() {
  # local classpath="$1"

  declare -a gavs=(
    "io.confluent:kafka-schema-registry-client:${confluent_install_version}:jar"
    "io.confluent:kafka-avro-serializer:${confluent_install_version}:jar"
    "io.confluent:kafka-schema-serializer:${confluent_install_version}:jar"
    "io.confluent:common-utils:${confluent_install_version}:jar"
    "io.confluent:common-config:${confluent_install_version}:jar"
  )

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

function exclude_dependencies() {
  # local classpath="$1"
  echo ""
}
