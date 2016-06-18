#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

PREFIX=""
if [ -z "$GEOMESA_HOME" ]; then
	echo "GEOMESA_HOME is not set. Using working directory for download"
	PREFIX="data"
else
	PREFIX="$GEOMESA_HOME/data"
fi
echo "Enter a region to download tracks for"
echo "africa, asia, austrailia-oceania, canada, central-america,"
echo "europe, ex-ussr, south-america, or usa "
read CONTINENT
wget "http://zverik.osm.rambler.ru/gps/files/extracts/$CONTINENT.tar.xz" -P $PREFIX/osm-gpx
