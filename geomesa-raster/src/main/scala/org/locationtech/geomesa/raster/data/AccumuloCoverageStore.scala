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


package org.locationtech.geomesa.raster.data

import java.awt.image.RenderedImage
import java.io.Serializable
import java.util.{Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{Connector, Scanner}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory._
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._
import org.locationtech.geomesa.raster.AccumuloStoreHelper
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.raster.ingest.GeoserverClientService
import org.locationtech.geomesa.raster.util.RasterUtils._

import scala.collection.JavaConversions._
import scala.util.Try

trait CoverageStore {
  def getAuths: Authorizations
  def getVisibility: String
  def getConnector: Connector
  def getTable: String
  def saveRaster(raster: Raster): Unit
  def registerToGeoserver(raster: Raster): Unit
}

/**
 *
 *  This class handles operations on a coverage, including cutting coverage into chunks
 *  and resembling chunks to a coverage, saving/retrieving coverage to/from data source,
 *  and registering coverage to Geoserver.
 *
 * @param rasterStore Raster store instance
 * @param geoserverClientServiceO Optional Geoserver client instance
 */
class AccumuloCoverageStore(val rasterStore: RasterStore,
                            val geoserverClientServiceO: Option[GeoserverClientService] = None)
  extends CoverageStore with Logging {

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)
  //TODO: WCS: remove if no longer needed
  rasterStore.ensureTableExists()

  def getAuths = rasterStore.getAuths

  def getVisibility = rasterStore.getVisibility

  def getConnector = rasterStore.getConnector

  def getTable = rasterStore.getTable

  def getRasters(rasterQuery: RasterQuery): Iterator[Raster] = rasterStore.getRasters(rasterQuery)

  def saveRaster(raster: Raster) = {
    rasterStore.putRaster(raster)
    registerToGeoserver(raster)
  }

  def registerToGeoserver(raster: Raster) {
    geoserverClientServiceO.foreach { geoserverClientService => {
      registerToGeoserver(raster, geoserverClientService)
      logger.debug(s"Register raster ${raster.id} to geoserver at ${geoserverClientService.geoserverUrl}")
    }}
  }

  private def registerToGeoserver(raster: Raster, geoserverClientService: GeoserverClientService) {
    geoserverClientService.registerRasterStyles()
    geoserverClientService.registerRaster(raster.id,
                                          raster.name,
                                          raster.time.getMillis,
                                          raster.id,
                                          "Raster data",
                                          raster.mbgh.hash,
                                          raster.mbgh.prec,
                                          None)
  }

  def getChunk(geohash: String, iRes: Int): RenderedImage = {
    withScanner(scanner => {
      val row = new Text(s"~$iRes~$geohash")
      scanner.setRange(new org.apache.accumulo.core.data.Range(row))
    })(_.map(entry => {
      imageDeserialize(entry.getValue.get)
    })).head
  }

  /**
   * Included for when mosaicing and final key structure are utilized
   *
   * def getChunks(geohash: String, iRes: Int, timeParam: Option[Either[Date, DateRange]], bbox: BoundingBox): Iterator[GridCoverage] = {
   *   withScanner(scanner => {
   *     val row = new Text(s"~$iRes~$geohash")
   *     scanner.setRange(new org.apache.accumulo.core.data.Range(row))
   *     val name = "version-" + Random.alphanumeric.take(5).mkString
   *     val cfg = new IteratorSetting(2, name, classOf[VersioningIterator])
   *     VersioningIterator.setMaxVersions(cfg, 1)
   *     scanner.addScanIterator(cfg)
   *   })(_.map(entry => {
   *     this.coverageFactory.create(coverageName,
   *       rasterImageDeserialize(entry.getValue.get),
   *       new ReferencedEnvelope(RasterIndexEntry.decodeIndexCQMetadata(entry.getKey).geom.getEnvelopeInternal, CRS.decode("EPSG:4326")))
   *   })).toIterator
   * }
   */

  protected def withScanner[A](configure: Scanner => Unit)(f: Scanner => A): A = {
    val scanner = getConnector.createScanner(getTable, getAuths)
    try {
      configure(scanner)
      f(scanner)
    } catch {
      case e: Exception => throw new Exception(s"Error accessing table ", e)
    }
  }
}

object AccumuloCoverageStore extends Logging {
   //TODO: WCS: ensure that this is as clean as possible -- GEOMESA-567
   def apply(username: String,
             password: String,
             instanceId: String,
             zookeepers: String,
             tableName: String,
             auths: String,
             writeVisibilities: String): AccumuloCoverageStore = {

     val rs = RasterStore(username,
             password,
             instanceId,
             zookeepers,
             tableName,
             auths,
             writeVisibilities)

     new AccumuloCoverageStore(rs, None)
   }

  def apply(config: JMap[String, Serializable]): AccumuloCoverageStore = {
    val visibility = AccumuloStoreHelper.getVisibility(config)
    val tableName = tableNameParam.lookUp(config).asInstanceOf[String]
    val useMock = java.lang.Boolean.valueOf(mockParam.lookUp(config).asInstanceOf[String])
    val connector =
      if (config.containsKey(connParam.key)) connParam.lookUp(config).asInstanceOf[Connector]
      else AccumuloStoreHelper.buildAccumuloConnector(config, useMock)
    val authorizationsProvider = AccumuloStoreHelper.getAuthorizationsProvider(config, connector)
    val collectStats = !useMock && Try(statsParam.lookUp(config).asInstanceOf[java.lang.Boolean] == true).getOrElse(false)

    val shardsConfig = shardsParam.lookupOpt(config)
    val writeMemoryConfig = writeMemoryParam.lookupOpt(config)
    val writeThreadsConfig = writeThreadsParam.lookupOpt(config)
    val queryThreadsConfig = queryThreadsParam.lookupOpt(config)

    // TODO: WCS: refactor by using companion object of RasterStore if appropriate
    // GEOMESA-567
    val rasterOps =
      AccumuloBackedRasterOperations(connector,
                                     tableName,
                                     authorizationsProvider,
                                     visibility,
                                     shardsConfig,
                                     writeMemoryConfig,
                                     writeThreadsConfig,
                                     queryThreadsConfig,
                                     collectStats)

    val dsConnectConfig: Map[String, String] = Map(
      IngestRasterParams.ACCUMULO_INSTANCE -> instanceIdParam.lookUp(config).asInstanceOf[String],
      IngestRasterParams.ZOOKEEPERS -> zookeepersParam.lookUp(config).asInstanceOf[String],
      IngestRasterParams.ACCUMULO_USER -> userParam.lookUp(config).asInstanceOf[String],
      IngestRasterParams.ACCUMULO_PASSWORD -> passwordParam.lookUp(config).asInstanceOf[String],
      IngestRasterParams.TABLE -> tableName,
      IngestRasterParams.AUTHORIZATIONS -> authorizationsProvider.getAuthorizations.toString
    )

    val geoserverConfig = geoserverParam.lookUp(config).asInstanceOf[String]
    val geoserverClientServiceO: Option[GeoserverClientService] =
      if (geoserverConfig == null) None
      else {
        val gsConnectConfig: Map[String, String] =
          geoserverConfig.split(",").map(_.split("=") match {
            case Array(s1, s2) => (s1, s2)
            case _ =>
              logger.error("Failed to instantiate Geoserver client service: wrong parameters.")
              sys.exit()
          }).toMap
        Some(new GeoserverClientService(gsConnectConfig))
      }

    new AccumuloCoverageStore(new RasterStore(rasterOps), geoserverClientServiceO)
  }

}

