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
package org.locationtech.geomesa.tools.ingest

import java.io.File
import java.nio.charset.StandardCharsets

import com.twitter.scalding.{Args, Hdfs, Mode}
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.Connector
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path, FileStatus, FileSystem}
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.raster.data.AccumuloCoverageStore
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import org.locationtech.geomesa.tools.Utils.Modes._

import scala.collection.JavaConversions._

class RemoteRasterIngest(config: Map[String, Option[String]]) extends RasterIngest with Logging {
  lazy val rasterName = config(IngestRasterParams.TABLE).get
  lazy val cs = createCoverageStore(config)

  def run(): Unit = {
    val conf = new Configuration()
    JobUtils.setLibJars(conf, libJars = ingestLibJars, searchPath = ingestJarSearchPath)

    //TODO: Only support HDFS mode. Investigate Local mode with scalding in future.
    //General thought is scalding Local mode can be done through local parallel ingestion in more efficient way.
    logger.info("Running raster ingest job in HDFS Mode")
    val mode = Hdfs(strict = true, conf)

    val arguments = Mode.putMode(mode, getScaldingArgs(conf))
    val job = new ScaldingRasterIngestJob(arguments)
    val flow = job.buildFlow

    //block until job is completed.
    flow.complete()
    job.printStatInfo

    cs.geoserverClientServiceO.foreach { geoserverClientService => {
      geoserverClientService.registerRasterStyles()
      geoserverClientService.registerRaster(rasterName, rasterName, "Raster data", None)
    }}
  }

  def getFileList(path: String, conf: Configuration): List[String] = {
    val hdfs = FileSystem.get(conf)
    val hdfsPath = new Path(path)
    if (hdfs.isDirectory(hdfsPath)) {
      val fileStatus: Array[FileStatus] = hdfs.listStatus(hdfsPath)
      val paths: Array[Path] = FileUtil.stat2Paths(fileStatus)
      paths.map(_.toString).toList
    } else List(hdfsPath.toString)
  }

  def ingestLibJars = {
    val is = getClass.getClassLoader.getResourceAsStream("org/locationtech/geomesa/tools/ingest-raster-libjars.list")
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
      () => JobUtils.getJarsFromClasspath(classOf[ScaldingRasterIngestJob]),
      () => JobUtils.getJarsFromClasspath(classOf[AccumuloCoverageStore]),
      () => JobUtils.getJarsFromClasspath(classOf[Connector]))

  def getScaldingArgs(conf: Configuration): Args = {
    import RemoteRasterIngest._

    val singleArgs = List(classOf[ScaldingRasterIngestJob].getCanonicalName, getModeFlag(config(IngestRasterParams.FILE_PATH).get))

    val path = config(IngestRasterParams.FILE_PATH).get
    val requiredKvArgs: Map[String, String] = Map(
      IngestRasterParams.ZOOKEEPERS        -> config(IngestRasterParams.ZOOKEEPERS).get,
      IngestRasterParams.ACCUMULO_INSTANCE -> config(IngestRasterParams.ACCUMULO_INSTANCE).get,
      IngestRasterParams.ACCUMULO_USER     -> config(IngestRasterParams.ACCUMULO_USER).get,
      IngestRasterParams.ACCUMULO_PASSWORD -> config(IngestRasterParams.ACCUMULO_PASSWORD).get,
      IngestRasterParams.HDFS_FILES        -> encodeFileList(getFileList(path, conf)),
      IngestRasterParams.TABLE             -> config(IngestRasterParams.TABLE).get,
      IngestRasterParams.FORMAT            -> config(IngestRasterParams.FORMAT).get,
      IngestRasterParams.ACCUMULO_MOCK     -> config(IngestRasterParams.ACCUMULO_MOCK).get,
      IngestRasterParams.IS_TEST_INGEST    -> false.toString
    )

    val optionalKvArgs: Map[String, String] =
      List(
        IngestRasterParams.AUTHORIZATIONS  -> config(IngestRasterParams.AUTHORIZATIONS),
        IngestRasterParams.VISIBILITIES    -> config(IngestRasterParams.VISIBILITIES),
        IngestRasterParams.SHARDS          -> config(IngestRasterParams.SHARDS),
        IngestRasterParams.PARLEVEL        -> config(IngestRasterParams.PARLEVEL),
        IngestRasterParams.TIME            -> config(IngestRasterParams.TIME),
        IngestRasterParams.WRITE_MEMORY    -> config(IngestRasterParams.WRITE_MEMORY),
        IngestRasterParams.WRITE_THREADS   -> config(IngestRasterParams.WRITE_THREADS),
        IngestRasterParams.GEOSERVER_REG   -> config(IngestRasterParams.GEOSERVER_REG),
        IngestRasterParams.QUERY_THREADS   -> config(IngestRasterParams.QUERY_THREADS))
      .filter(p => p._2.nonEmpty)
      .map { case (k,o) => k -> o.get.toString }
      .toMap

    val kvArgs = (requiredKvArgs ++ optionalKvArgs).flatMap { case (k, v) => List(s"--$k", v) }
    Args(singleArgs ++ kvArgs)
  }
}

object RemoteRasterIngest {
  def encodeFileList(files: List[String]) =
    files.map { s => Hex.encodeHexString(s.getBytes(StandardCharsets.UTF_8)) }.mkString(" ")

  def decodeFileList(encoded: String) =
    encoded.split(" ").map { s => new String(Hex.decodeHex(s.toCharArray)) }
}
