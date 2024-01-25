#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

cd "$(dirname "$0")" || exit
echo >&2 "WARNING: this script is deprecated, please use 'install-confluent-support.sh' instead"
./install-confluent-support.sh "$@"
