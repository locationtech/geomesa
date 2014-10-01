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

USERNAME=username
PASSWORD=password
CREATE_CATALOG=test_catalog
CREATE_FEATURENAME=test_feature
SPEC=fid:String:index=true,dtg:Date,geom:Point:srid=4326
CATALOG=test_catalog
FEATURENAME=test_feature
MAXFEATURES=100

set -x
geomesa create -u ${USERNAME} -p ${PASSWORD} -c ${CREATE_CATALOG} -f ${CREATE_FEATURENAME} -s ${SPEC} -d dtg
geomesa list -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG}
geomesa describe -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME}
geomesa explain -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -q include
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o csv -m ${MAXFEATURES}
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o csv -s -m ${MAXFEATURES}
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o tsv -m ${MAXFEATURES}
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o tsv -s -m ${MAXFEATURES}
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o geojson -m ${MAXFEATURES}
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o geojson -s -m ${MAXFEATURES}
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o gml -m ${MAXFEATURES}
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o gml -s -m ${MAXFEATURES}
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o shp -m ${MAXFEATURES}
geomesa delete -u ${USERNAME} -p ${PASSWORD} --force -c ${CREATE_CATALOG} -f ${CREATE_FEATURENAME}
geomesa list -u ${USERNAME} -p ${PASSWORD} -c ${CREATE_CATALOG}
geomesa list -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG}