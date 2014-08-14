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
CATALOG=test_catalog
FEATURENAME=test_feature
SPEC=id:String:index=true,dtg:Date,geom:Point:srid=4326

geomesa create -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -s ${SPEC}
geomesa list -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG}
geomesa describe -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME}
geomesa explain -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -q include
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o csv
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o tsv
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o geojson
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o gml
geomesa export -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME} -o shp
geomesa delete -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG} -f ${FEATURENAME}
geomesa list -u ${USERNAME} -p ${PASSWORD} -c ${CATALOG}