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
import com.twitter.scalding.Args
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{JobConf, OutputCollector, RecordReader, RecordWriter}
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConverters._

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

  type GMRecordReader = RecordReader[Text, SimpleFeature]
  type GMRecordWriter = RecordWriter[Text, SimpleFeature]
  type GMOutputCollector = OutputCollector[Text, SimpleFeature]
  type GMTap = Tap[JobConf, GMRecordReader, GMOutputCollector]
  type GMScheme = Scheme[JobConf, GMRecordReader, GMOutputCollector, Array[Any], Array[Any]]

  object ConnectionParams {

    // old arg strings - replaced with standardized input/output ones below
    @Deprecated val ACCUMULO_INSTANCE_OLD = "geomesa.accumulo.instance"
    @Deprecated val ZOOKEEPERS_OLD        = "geomesa.accumulo.zookeepers"
    @Deprecated val ACCUMULO_USER_OLD     = "geomesa.accumulo.user"
    @Deprecated val ACCUMULO_PASSWORD_OLD = "geomesa.accumulo.password"
    @Deprecated val AUTHORIZATIONS_OLD    = "geomesa.accumulo.authorizations"
    @Deprecated val VISIBILITIES_OLD      = "geomesa.accumulo.visibilities"
    @Deprecated val FEATURE_NAME_OLD      = "geomesa.feature.name"
    @Deprecated val CATALOG_TABLE_OLD     = "geomesa.feature.tables.catalog"
    @Deprecated val MOCK_OLD              = "geomesa.mock"

    private val IN_PREFIX  = "geomesa.input."
    private val OUT_PREFIX = "geomesa.output."

    val CQL_IN      = s"${IN_PREFIX}cql"
    val FEATURE_IN  = s"${IN_PREFIX}feature"
    val FEATURE_OUT = s"${OUT_PREFIX}feature"

    private val params = Seq(instanceIdParam, zookeepersParam, userParam, passwordParam, authsParam,
      visibilityParam, tableNameParam, mockParam)

    /**
     * Gets a data store connection map based on the configured input
     */
    def toDataStoreInParams(args: Args): Map[String, String] = toDataStoreParams(args, IN_PREFIX)

    /**
     * Gets a data store connection map based on the configured output
     */
    def toDataStoreOutParams(args: Args): Map[String, String] = toDataStoreParams(args, OUT_PREFIX)

    /**
     * Converts our arg strings into the map needed for DataStoreFinder
     */
    private def toDataStoreParams(args: Args, prefix: String): Map[String, String] =
      args.m.map { case (key, value) =>
        if (key.startsWith(prefix)) {
          key.substring(prefix.length) -> value.headOption.getOrElse("")
        } else key match {
          // back compatible checks
          case ACCUMULO_INSTANCE_OLD => "instanceId"     -> value.headOption.getOrElse("")
          case ZOOKEEPERS_OLD        => "zookeepers"     -> value.headOption.getOrElse("")
          case ACCUMULO_USER_OLD     => "user"           -> value.headOption.getOrElse("")
          case ACCUMULO_PASSWORD_OLD => "password"       -> value.headOption.getOrElse("")
          case AUTHORIZATIONS_OLD    => "authorizations" -> value.headOption.getOrElse("")
          case VISIBILITIES_OLD      => "visibilities"   -> value.headOption.getOrElse("")
          case CATALOG_TABLE_OLD     => "tableName"      -> value.headOption.getOrElse("")
          case MOCK_OLD              => "useMock"        -> value.headOption.getOrElse("")
          case _                     => ""               -> ""
        }
      }.filter { case (k, v) => !v.isEmpty }

    /**
     * Converts a data store connection map into a configured input source
     */
    def toInArgs(dsParams: Map[String, String]): Map[String, List[String]] = toArgs(dsParams, IN_PREFIX)

    /**
     * Converts a data store connection map into a configured output sink
     */
    def toOutArgs(dsParams: Map[String, String]): Map[String, List[String]] = toArgs(dsParams, OUT_PREFIX)

    /**
     * Converts a DataStoreFinder map into args for a scalding job
     */
    private def toArgs(dsParams: Map[String, String], prefix: String): Map[String, List[String]] = {
      val jParams = dsParams.asJava
      params.map(p => s"$prefix${p.getName}" -> Option(p.lookUp(jParams).asInstanceOf[String]).toList)
          .filter { case (k, v) => !v.isEmpty }
          .toMap
    }
  }
}
