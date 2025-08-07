#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# https://www.apache.org/licenses/LICENSE-2.0
#

cd "$(dirname "$0")" || exit
export GEOMESA_DEPENDENCIES="parquet-dependencies.sh"
./install-dependencies.sh "$@"
