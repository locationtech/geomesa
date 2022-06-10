#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

cd "$(dirname "$0")"
export GEOMESA_DEPENDENCIES="confluent-dependencies.sh"
export GEOMESA_MAVEN_URL="${GEOMESA_MAVEN_URL:-https://packages.confluent.io/maven/}"
./install-dependencies.sh
