/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.jobs

import cascading.scheme.Scheme
import cascading.tap.Tap
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{JobConf, OutputCollector, RecordReader, RecordWriter}

package object scalding {
  type GenericRecordReader = RecordReader[_, _]
  type GenericOutputCollector = OutputCollector[_, _]
  type GenericScheme = Scheme[JobConf, GenericRecordReader, GenericOutputCollector, _, _]
  type GenericTap = Tap[_, _, _]

  type KVRecordReader = RecordReader[Key, Value]
  type MutRecordWriter = RecordWriter[Text, Mutation]
  type MutOutputCollector = OutputCollector[Text, Mutation]
  type AccTap = Tap[JobConf, KVRecordReader, MutOutputCollector]
  type AccScheme = Scheme[JobConf, KVRecordReader, MutOutputCollector, Array[Any], Array[Any]]

  object ConnectionParams {
    val ACCUMULO_INSTANCE   = "geomesa.accumulo.instance"
    val ZOOKEEPERS          = "geomesa.accumulo.zookeepers"
    val ACCUMULO_USER       = "geomesa.accumulo.user"
    val ACCUMULO_PASSWORD   = "geomesa.accumulo.password"
    val AUTHORIZATIONS      = "geomesa.accumulo.authorizations"
    val VISIBILITIES        = "geomesa.accumulo.visibilities"
    val FEATURE_NAME        = "geomesa.feature.name"
    val CATALOG_TABLE       = "geomesa.feature.tables.catalog"
    val RECORD_TABLE        = "geomesa.feature.tables.record"
    val ATTRIBUTE_TABLE     = "geomesa.feature.tables.attribute"
  }
}
