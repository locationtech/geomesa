/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.geotools.data.FeatureWriter
import org.geotools.factory.Hints.ClassKey
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

package object data {

  import org.locationtech.geomesa.core.index._

import scala.collection.JavaConversions._

  // Datastore parameters
  val INSTANCE_ID      = "geomesa.instance.id"
  val ZOOKEEPERS       = "geomesa.zookeepers"
  val ACCUMULO_USER    = "geomesa.user"
  val ACCUMULO_PASS    = "geomesa.pass"
  val AUTHS            = "geomesa.auths"
  val AUTH_PROVIDER    = "geomesa.auth.provider"
  val VISIBILITY       = "geomesa.visibility"
  val TABLE            = "geomesa.table"
  val FEATURE_NAME     = "geomesa.feature.name"
  val FEATURE_ENCODING = "geomesa.feature.encoding"

  // Metadata keys
  val ATTRIBUTES_KEY         = "attributes"
  val BOUNDS_KEY             = "bounds"
  val SCHEMA_KEY             = "schema"
  val DTGFIELD_KEY           = "dtgfield"
  val FEATURE_ENCODING_KEY   = "featureEncoding"
  val VISIBILITIES_KEY       = "visibilities"
  val VISIBILITIES_CHECK_KEY = "visibilitiesCheck"
  val ST_IDX_TABLE_KEY       = "tables.idx.st.name"
  val ATTR_IDX_TABLE_KEY     = "tables.idx.attr.name"
  val RECORD_TABLE_KEY       = "tables.record.name"
  val QUERIES_TABLE_KEY      = "tables.queries.name"
  val SHARED_TABLES_KEY      = "tables.sharing"

  // Storage implementation constants
  val DATA_CQ              = new Text("SimpleFeatureAttribute")
  val SFT_CF               = new Text("SFT")
  val METADATA_TAG         = "~METADATA"
  val METADATA_TAG_END     = s"$METADATA_TAG~~"
  val EMPTY_STRING         = ""
  val EMPTY_VALUE          = new Value(Array[Byte]())
  val EMPTY_COLF           = new Text(EMPTY_STRING)
  val EMPTY_COLQ           = new Text(EMPTY_STRING)
  val WHOLE_WORLD_BOUNDS   = "-180.0:180.0:-90.0:90.0"

  // SimpleFeature Hints
  val TRANSFORMS           = new ClassKey(classOf[String])
  val TRANSFORM_SCHEMA     = new ClassKey(classOf[SimpleFeatureType])
  val GEOMESA_UNIQUE       = new ClassKey(classOf[String])

  type TASKIOCTX = TaskInputOutputContext[_, _, Key, Value]
  type SFFeatureWriter = FeatureWriter[SimpleFeatureType, SimpleFeature]

  def extractDtgField(sft: SimpleFeatureType) =
    sft.getAttributeDescriptors
      .filter { _.getUserData.contains(SF_PROPERTY_START_TIME) }
      .headOption
      .map { _.getName.toString }
      .getOrElse(DEFAULT_DTG_PROPERTY_NAME)

}
