#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

echo "Enter a date in the form YYYYMMDD: "
read DATE
wget "http://data.gdeltproject.org/events/${DATE}.export.CSV.zip" -P $GEOMESA_HOME/data/gdelt
