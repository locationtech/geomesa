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

import java.util.{List => JList, Map => JMap}

import cascading.flow.{Flow, FlowStep, FlowStepStrategy}
import com.twitter.scalding._
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.jobs.scalding.ConnectionParams._
import org.locationtech.geomesa.jobs.scalding._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._

trait GeoMesaResources {
  def ds: AccumuloDataStore
  def sft: SimpleFeatureType
  def visibilities: String

  // required by scalding
  def release(): Unit = {}
}

abstract class GeoMesaBaseJob(args: Args) extends Job(args) with Logging {

  lazy val feature    = args(FEATURE_NAME)
  lazy val zookeepers = args(ZOOKEEPERS)
  lazy val instance   = args(ACCUMULO_INSTANCE)
  lazy val user       = args(ACCUMULO_USER)
  lazy val password   = args(ACCUMULO_PASSWORD)
  lazy val catalog    = args(CATALOG_TABLE)
  lazy val auths      = args.optional(AUTHORIZATIONS).getOrElse("")
  lazy val mock       = args.optional(MOCK).getOrElse("false")

  lazy val options = AccumuloSourceOptions(instance, zookeepers, user, password, input, output)

  def input: AccumuloInputOptions
  def output: AccumuloOutputOptions

  // subclasses can override to perform cleanup or final tasks after the job completes
  def afterJobTasks(): Unit = {}

  def jobName: String = s"GeoMesa ${getClass.getSimpleName}[$feature]"

  lazy val params: Map[String, String] = Map(
    "zookeepers"  -> zookeepers,
    "instanceId"  -> instance,
    "tableName"   -> catalog,
    "user"        -> user,
    "password"    -> password,
    "auths"       -> auths,
    "useMock"     -> mock
  )

  // hook to set the job name
  override def stepStrategy: Option[FlowStepStrategy[_]] = Some(new JobNameFlowStepStrategy(jobName))

  // override the run method to perform tasks after the job completes
  override def run: Boolean = {
    val result = super.run
    if (result) {
      afterJobTasks()
      logger.info("Job completed successfully")
    } else {
      logger.info("Job failed")
    }
    result
  }

  // non-serializable resources we want to re-use
  class GeoMesaResources {
    val ds: AccumuloDataStore = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
    val sft: SimpleFeatureType = ds.getSchema(feature)
    val visibilityString = ds.writeVisibilities
    val visibilities = new ColumnVisibility(visibilityString)
    // required by scalding
    def release(): Unit = {}
  }
}

/**
 * Sets the job name in the flow step
 */
class JobNameFlowStepStrategy(name: String) extends FlowStepStrategy[JobConf] {
  override def apply(flow: Flow[JobConf], previous: JList[FlowStep[JobConf]], flowStep: FlowStep[JobConf]) =
    flowStep.getConfig.setJobName(name)
}

object GeoMesaBaseJob {

  def runJob(conf: Configuration,
             params: Map[String, String],
             feature: String,
             customArgs: Map[String, List[String]],
             instantiateJob: (Args) => Job) = {

    val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]

    require(ds != null, "Data store could not be loaded")

    val sft = ds.getSchema(feature)
    require(sft != null, s"Feature '$feature' does not exist")

    // create args to pass to scalding job based on our input parameters
    val args = new Args(buildBaseArgs(params.asJava, feature) ++ customArgs)

    // set libjars so that our dependent libs get propagated to the cluster
    JobUtils.setLibJars(conf)

    // run the scalding job on HDFS
    val hdfsMode = Hdfs(strict = true, conf)
    val arguments = Mode.putMode(hdfsMode, args)

    val job = instantiateJob(args)
    val flow = job.buildFlow
    flow.complete() // this blocks until the job is done
  }

  def buildBaseArgs(params: JMap[String, String], feature: String): Map[String, List[String]] = {

    implicit def objectToList(o: Object) = List(o.asInstanceOf[String])

    val args = Map[String, List[String]](
      FEATURE_NAME      -> feature,
      ZOOKEEPERS        -> zookeepersParam.lookUp(params),
      ACCUMULO_INSTANCE -> instanceIdParam.lookUp(params),
      ACCUMULO_USER     -> userParam.lookUp(params),
      ACCUMULO_PASSWORD -> passwordParam.lookUp(params),
      CATALOG_TABLE     -> tableNameParam.lookUp(params),
      AUTHORIZATIONS    -> Option(authsParam.lookUp(params).asInstanceOf[String]).toList,
      VISIBILITIES      -> Option(visibilityParam.lookUp(params).asInstanceOf[String]).toList,
      MOCK              -> Option(mockParam.lookUp(params).asInstanceOf[String]).toList
    )
    args.filter { case (k, v) => !v.isEmpty }
  }
}