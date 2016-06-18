#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

echo "Enter how many zip files to download"
echo "Each zip contains approximately one million points"
read NUM

for i in `seq 1 $NUM`;
do
	echo "Downloading zip $i of $NUM"
	wget "http://research.microsoft.com/pubs/152883/0$i.zip" -P $GEOMESA_HOME/data/tdrive
done 
wget "http://research.microsoft.com/pubs/152883/User_guide_T-drive.pdf" -P $GEOMESA_HOME/data/tdrive