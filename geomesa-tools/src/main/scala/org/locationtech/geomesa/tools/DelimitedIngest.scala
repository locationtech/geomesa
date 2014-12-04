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
package org.locationtech.geomesa.tools

import java.io.{InputStream, File}
import java.net.URLEncoder

import com.twitter.scalding.{Args, Hdfs, Local, Mode}
import org.apache.accumulo.core.client.Connector
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.tools.Utils.Formats._
import org.locationtech.geomesa.tools.Utils.Modes._
import org.locationtech.geomesa.tools.Utils.{IngestParams, Modes}
import org.locationtech.geomesa.tools.commands.IngestCommand.IngestParameters

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.Try

class DelimitedIngest(params: IngestParameters) extends AccumuloProperties {

  def run(): Unit = {
    // create schema for the feature prior to Ingest job
    FeatureCreator.createFeature(params)

    val conf = new Configuration()
    JobUtils.setLibJars(conf, libJars = ingestLibJars, searchPath = ingestJarSearchPath)

    // setup ingest
    val hdfsMode =
      if (getMode(params.files(0)) == Modes.Hdfs) {
        Hdfs(strict = true, conf)
      } else {
        Local(strictSources = true)
      }
    val arguments = Mode.putMode(hdfsMode, getScaldingArgs())
    val job = new DelimitedIngestJob(arguments)
    val flow = job.buildFlow

    //block until job is completed.
    flow.complete()
    job.printStatInfo
  }

  def ingestLibJars = {
    val is = getClass.getClassLoader.getResourceAsStream("org/locationtech/geomesa/tools/ingest-libjars.list")
    try {
      IOUtils.readLines(is)
    } catch {
      case e: Exception => throw new Exception("Error reading ingest libjars: "+e.getMessage, e)
    } finally {
      IOUtils.closeQuietly(is)
    }
  }

  def ingestJarSearchPath: Iterator[() => Seq[File]] =
    Iterator(() => JobUtils.getJarsFromEnvironment("GEOMESA_HOME"),
      () => JobUtils.getJarsFromEnvironment("ACCUMULO_HOME"),
      () => JobUtils.getJarsFromClasspath(classOf[DelimitedIngestJob]),
      () => JobUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
      () => JobUtils.getJarsFromClasspath(classOf[Connector]))

  def getScaldingArgs(): Args = {
    val singleArgs = List(classOf[DelimitedIngestJob].getCanonicalName, getModeFlag(params.files(0)))

    val requiredKvArgs: Map[String, String] = Map(
      IngestParams.FILE_PATH         -> params.files(0).getPath,
      IngestParams.SFT_SPEC          -> URLEncoder.encode(params.spec, "UTF-8"),
      IngestParams.CATALOG_TABLE     -> params.catalog,
      IngestParams.ZOOKEEPERS        -> Option(params.zookeepers).getOrElse(zookeepersProp),
      IngestParams.ACCUMULO_INSTANCE -> Option(params.instance).getOrElse(instanceName),
      IngestParams.ACCUMULO_USER     -> params.user,
      IngestParams.ACCUMULO_PASSWORD -> getPassword(params.password),
      IngestParams.DO_HASH           -> params.hash.toString,
      IngestParams.FORMAT            -> getFileExtension(params.files(0)),
      IngestParams.FEATURE_NAME      -> params.featureName,
      IngestParams.IS_TEST_INGEST    -> false.toString
    )

    val optionalKvArgs: Map[String, String] =
      List(
        IngestParams.COLS              -> Option(params.columns),
        IngestParams.DT_FORMAT         -> Option(params.dtFormat),
        IngestParams.ID_FIELDS         -> Option(params.idFields),
        IngestParams.DT_FIELD          -> Option(params.dtgField),
        IngestParams.LON_ATTRIBUTE     -> Option(params.lon),
        IngestParams.LAT_ATTRIBUTE     -> Option(params.lat),
        IngestParams.AUTHORIZATIONS    -> Option(params.auths),
        IngestParams.VISIBILITIES      -> Option(params.visibilities),
        IngestParams.INDEX_SCHEMA_FMT  -> Option(params.indexSchema),
        IngestParams.SHARDS            -> Option(params.numShards))
      .filter(p => p._2.nonEmpty)
      .map { case (k,o) => k -> o.get.toString }
      .toMap

    if ( !optionalKvArgs.contains(IngestParams.DT_FIELD) ) {
      // assume user has no date field to use and that there is no column of data signifying it.
      logger.warn("Warning: no date-time field specified. Assuming that data contains no date column. \n" +
        s"GeoMesa is defaulting to the system time for ingested features.")
    }

    val kvArgs = (requiredKvArgs ++ optionalKvArgs).flatMap { case (k,v) => List(s"--$k", v) }
    Args(singleArgs ++ kvArgs)
  }
}
