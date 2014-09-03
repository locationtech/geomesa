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

import com.twitter.scalding._
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.conf.Configuration
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.data.tables.AttributeTable
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.jobs.scalding.{AccumuloInputOptions, AccumuloOutputOptions, AccumuloSource, AccumuloSourceOptions, ConnectionParams}

import scala.collection.JavaConverters._

class AttributeIndexJob(args: Args) extends Job(args) {

  lazy val feature          = args(ConnectionParams.FEATURE_NAME)
  lazy val attributes       = args.list(AttributeIndexJob.Params.ATTRIBUTES_TO_INDEX)
  lazy val zookeepers       = args(ConnectionParams.ZOOKEEPERS)
  lazy val instance         = args(ConnectionParams.ACCUMULO_INSTANCE)
  lazy val user             = args(ConnectionParams.ACCUMULO_USER)
  lazy val password         = args(ConnectionParams.ACCUMULO_PASSWORD)
  lazy val catalog          = args(ConnectionParams.CATALOG_TABLE)
  lazy val recordTable      = args(ConnectionParams.RECORD_TABLE)
  lazy val attributeTable   = args(ConnectionParams.ATTRIBUTE_TABLE)
  lazy val auths            = args.optional(ConnectionParams.AUTHORIZATIONS).getOrElse("")
  lazy val useMock            = args.optional(ConnectionParams.USEMOCKACCUMULO).getOrElse(false)

  lazy val input   = AccumuloInputOptions(recordTable)
  lazy val output  = AccumuloOutputOptions(attributeTable)
  lazy val options = AccumuloSourceOptions(instance, zookeepers, user, password, input, output)

  lazy val params = Map("zookeepers"  -> zookeepers,
                        "instanceId"  -> instance,
                        "tableName"   -> catalog,
                        "user"        -> user,
                        "password"    -> password,
                        "auths"       -> auths,
                        "useMock"     -> useMock)

  // non-serializable resources we want to re-use if possible
  class Resources {
    val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
    val sft = ds.getSchema(feature)
    val visibilities = ds.writeVisibilities
    val decoder = ds.getFeatureEncoder(feature)
    // the attributes we want to index
    val attributeDescriptors = sft.getAttributeDescriptors
                                 .asScala
                                 .filter(ad => attributes.contains(ad.getLocalName))

    // required by scalding
    def release(): Unit = {}
  }

  // scalding job
  AccumuloSource(options)
    .using(new Resources())
    .flatMap(('key, 'value) -> 'mutation) {
      (r: Resources, kv: (Key, Value)) => getAttributeIndexMutation(r, kv._1, kv._2)
    }.write(AccumuloSource(options))

  /**
   * Converts a key/value pair from the record table into attribute index mutations
   *
   * @param r
   * @param key
   * @param value
   * @return
   */
  def getAttributeIndexMutation(r: Resources, key: Key, value: Value): Seq[Mutation] = {
    val feature = r.decoder.decode(r.sft, value)
    val prefix = org.locationtech.geomesa.core.index.getTableSharingPrefix(r.sft)

    AttributeTable.getAttributeIndexMutations(
      feature,
      r.attributeDescriptors,
      new ColumnVisibility(r.visibilities),
      prefix
    )
  }
}

object AttributeIndexJob {

  object Params {
    val ATTRIBUTES_TO_INDEX   = "geomesa.index.attributes"
  }

  def runJob(conf: Configuration, params: Map[String, String], feature: String, attributes: Seq[String]) = {

    if (attributes.isEmpty) {
      throw new IllegalArgumentException("No attributes specified")
    }

    val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]

    if (ds == null) {
      throw new IllegalArgumentException("Data store could not be loaded")
    } else if (!ds.catalogTableFormat(feature)) {
      throw new IllegalStateException("Feature does not have an attribute index")
    }

    val jParams = params.asJava

    import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._

    // create args to pass to scalding job based on our input parameters
    val args = new collection.mutable.ListBuffer[String]()
    args.append("--" + ConnectionParams.FEATURE_NAME, feature)
    args.appendAll(Seq("--" + Params.ATTRIBUTES_TO_INDEX) ++ attributes)
    args.append("--" + ConnectionParams.RECORD_TABLE, ds.getRecordTableForType(feature))
    args.append("--" + ConnectionParams.ATTRIBUTE_TABLE, ds.getAttrIdxTableName(feature))

    args.append("--" + ConnectionParams.ZOOKEEPERS,
                 zookeepersParam.lookUp(jParams).asInstanceOf[String])
    args.append("--" + ConnectionParams.ACCUMULO_INSTANCE,
                 instanceIdParam.lookUp(jParams).asInstanceOf[String])
    args.append("--" + ConnectionParams.ACCUMULO_USER,
                 userParam.lookUp(jParams).asInstanceOf[String])
    args.append("--" + ConnectionParams.ACCUMULO_PASSWORD,
                 passwordParam.lookUp(jParams).asInstanceOf[String])
    args.append("--" + ConnectionParams.CATALOG_TABLE,
                 tableNameParam.lookUp(jParams).asInstanceOf[String])
    Option(authsParam.lookUp(jParams).asInstanceOf[String]).foreach(a =>
      args.append("--" + ConnectionParams.AUTHORIZATIONS, a))
    Option(visibilityParam.lookUp(jParams).asInstanceOf[String]).foreach(v =>
      args.append("--" + ConnectionParams.VISIBILITIES, v))

    // set libjars so that our dependent libs get propagated to the cluster
    JobUtils.setLibJars(conf)

    // run the scalding job on HDFS
    val hdfsMode = Hdfs(strict = true, conf)
    val arguments = Mode.putMode(hdfsMode, Args(args))

    val job = new AttributeIndexJob(arguments)
    val flow = job.buildFlow
    flow.complete() // this blocks until the job is done
  }
}
