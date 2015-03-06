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

package org.locationtech.geomesa.jobs.index

import java.util

import com.twitter.scalding._
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._
import org.locationtech.geomesa.jobs.scalding.{AccumuloInputOptions, AccumuloOutputOptions, AccumuloSourceOptions, ConnectionParams}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._

trait GeoMesaResources {
  def ds: AccumuloDataStore
  def sft: SimpleFeatureType
  def visibilities: String

  // required by scalding
  def release(): Unit = {}
}

abstract class GeoMesaBaseJob(args: Args) extends Job(args) {

  lazy val feature          = args(ConnectionParams.FEATURE_NAME)
  lazy val zookeepers       = args(ConnectionParams.ZOOKEEPERS)
  lazy val instance         = args(ConnectionParams.ACCUMULO_INSTANCE)
  lazy val user             = args(ConnectionParams.ACCUMULO_USER)
  lazy val password         = args(ConnectionParams.ACCUMULO_PASSWORD)
  lazy val catalog          = args(ConnectionParams.CATALOG_TABLE)
  lazy val auths            = args.optional(ConnectionParams.AUTHORIZATIONS).getOrElse("")

  lazy val options = AccumuloSourceOptions(instance, zookeepers, user, password, input, output)

  def input: AccumuloInputOptions
  def output: AccumuloOutputOptions

  lazy val params: Map[String, String] = Map(
    "zookeepers"  -> zookeepers,
    "instanceId"  -> instance,
    "tableName"   -> catalog,
    "user"        -> user,
    "password"    -> password,
    "auths"       -> auths
  )

  // non-serializable resources we want to re-use
  class GeoMesaResources {
    val ds: AccumuloDataStore = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
    val sft: SimpleFeatureType = ds.getSchema(feature)
    val visibilities: String = ds.writeVisibilities
    // required by scalding
    def release(): Unit = {}
  }
}

object GeoMesaBaseJob {

  def buildBaseArgs(params: util.Map[String, String], feature: String): Args = {
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

    val args = new collection.mutable.ListBuffer[String]()
    args.append("--" + ConnectionParams.FEATURE_NAME, feature)
    args.append("--" + ConnectionParams.RECORD_TABLE, ds.getRecordTableForType(feature))
    args.append("--" + ConnectionParams.ST_INDEX_TABLE, ds.getSpatioTemporalIdxTableName(feature))
    args.append("--" + ConnectionParams.ATTRIBUTE_TABLE, ds.getAttrIdxTableName(feature))
    args.append("--" + ConnectionParams.ZOOKEEPERS,
      zookeepersParam.lookUp(params).asInstanceOf[String])
    args.append("--" + ConnectionParams.ACCUMULO_INSTANCE,
      instanceIdParam.lookUp(params).asInstanceOf[String])
    args.append("--" + ConnectionParams.ACCUMULO_USER,
      userParam.lookUp(params).asInstanceOf[String])
    args.append("--" + ConnectionParams.ACCUMULO_PASSWORD,
      passwordParam.lookUp(params).asInstanceOf[String])
    args.append("--" + ConnectionParams.CATALOG_TABLE,
      tableNameParam.lookUp(params).asInstanceOf[String])
    Option(authsParam.lookUp(params).asInstanceOf[String]).foreach(a =>
      args.append("--" + ConnectionParams.AUTHORIZATIONS, a))
    Option(visibilityParam.lookUp(params).asInstanceOf[String]).foreach(v =>
      args.append("--" + ConnectionParams.VISIBILITIES, v))
    Args(args)
  }
}