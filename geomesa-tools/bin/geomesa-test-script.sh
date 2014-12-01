#! /usr/bin/env bash
#
# Copyright 2014 Commonwealth Computer Research, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
#
# This script allows testing of the geomesa command line tools to ensure that
# all commands are working properly. Change the variables at the top to suit
# your testing needs and run from the command line.

USER=USER_HERE
PASS=PASSWORD_HERE
CREATE_CATALOG=test_catalog
CREATE_FEATURENAME=test_feature
SPEC=fid:String:index=true,dtg:Date,geom:Point:srid=4326
CATALOG=geomesa_catalog
FEATURENAME=FEATURE_HERE
MAXFEATURES=100

# Helper opts for accumulo. These can optionally be supplied from the 
# ACCUMULO_HOME configuration by excluding the arguments from the geomesa command
INST=INSTANCE
ZOO=zoo1,zoo2,zoo3
ACC_OPTS="-u $USER -p $PASS -i $INST -z $ZOO"

geomesa create $ACC_OPTS -c ${CREATE_CATALOG} -f ${CREATE_FEATURENAME} -s ${SPEC} --dtg dtg
geomesa list $ACC_OPTS -c ${CATALOG}
geomesa describe $ACC_OPTS -c ${CATALOG} -f ${FEATURENAME}
geomesa explain $ACC_OPTS -c ${CATALOG} -f ${FEATURENAME} -q include

# export to std out in various formats
geomesa export $ACC_OPTS -c ${CATALOG} -f ${FEATURENAME} -o csv -m ${MAXFEATURES}
geomesa export $ACC_OPTS -c ${CATALOG} -f ${FEATURENAME} -o tsv -m ${MAXFEATURES}
geomesa export $ACC_OPTS -c ${CATALOG} -f ${FEATURENAME} -o json -m ${MAXFEATURES}
geomesa export $ACC_OPTS -c ${CATALOG} -f ${FEATURENAME} -o gml -m ${MAXFEATURES}

# export to files (includes shape file)
geomesa export $ACC_OPTS -c ${CATALOG} -f ${FEATURENAME} -o csv -m ${MAXFEATURES} --file /tmp/csv.out
geomesa export $ACC_OPTS -c ${CATALOG} -f ${FEATURENAME} -o tsv -m ${MAXFEATURES} --file /tmp/tsv.out
geomesa export $ACC_OPTS -c ${CATALOG} -f ${FEATURENAME} -o json -m ${MAXFEATURES} --file /tmp/json.out
geomesa export $ACC_OPTS -c ${CATALOG} -f ${FEATURENAME} -o gml -m ${MAXFEATURES} --file /tmp/gml.out
geomesa export $ACC_OPTS -c ${CATALOG} -f ${FEATURENAME} -o shp -m ${MAXFEATURES} --file /tmp/out.shp

# clean up previous temp feature
geomesa delete $ACC_OPTS --force -c ${CREATE_CATALOG} -f ${CREATE_FEATURENAME}
geomesa list $ACC_OPTS -c ${CREATE_CATALOG}

