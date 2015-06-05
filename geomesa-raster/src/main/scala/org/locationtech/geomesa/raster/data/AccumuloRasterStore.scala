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

import java.io.Serializable
import java.util.Map.Entry
import java.util.concurrent.{Callable, TimeUnit}
import java.util.{Map => JMap}

import com.google.common.cache.CacheBuilder
import com.google.common.collect.ImmutableSetMultimap
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{BatchWriterConfig, Connector, TableExistsException}
import org.apache.accumulo.core.data.{Key, Mutation, Range, Value}
import org.apache.accumulo.core.security.TablePermission
import org.geotools.coverage.grid.GridEnvelope2D
import org.joda.time.DateTime
import org.locationtech.geomesa.accumulo.index.Strategy._
import org.locationtech.geomesa.accumulo.iterators.BBOXCombiner._
import org.locationtech.geomesa.accumulo.stats.{RasterQueryStat, RasterQueryStatTransform, StatWriter}
import org.locationtech.geomesa.accumulo.util.{SelfClosingBatchScanner, SelfClosingScanner}
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.raster.index.RasterIndexSchema
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.locationtech.geomesa.utils.stats.{MethodProfiling, NoOpTimings, Timings, TimingsImpl}

import scala.collection.JavaConversions._

class AccumuloRasterStore(val connector: Connector,
                  val tableName: String,
                  val authorizationsProvider: AuthorizationsProvider,
                  val writeVisibilities: String,
                  writeMemoryConfig: Option[String] = None,
                  writeThreadsConfig: Option[Int] = None,
                  queryThreadsConfig: Option[Int] = None,
                  collectStats: Boolean = false) extends MethodProfiling with StatWriter with Logging {
  val writeMemory = writeMemoryConfig.getOrElse("10000").toLong
  val writeThreads = writeThreadsConfig.getOrElse(10)
  val bwConfig: BatchWriterConfig =
    new BatchWriterConfig().setMaxMemory(writeMemory).setMaxWriteThreads(writeThreads)
  val numQThreads = queryThreadsConfig.getOrElse(20)

  // TODO: WCS: GEOMESA-585 Add ability to use arbitrary schemas
  val schema = RasterIndexSchema()
  lazy val queryPlanner: AccumuloRasterQueryPlanner = new AccumuloRasterQueryPlanner(schema)

  private val tableOps       = connector.tableOperations()
  private val securityOps    = connector.securityOperations
  private val profileTable   = s"${tableName}_queries"
  private def getBoundsRowID = tableName + "_bounds"

  def getAuths = authorizationsProvider.getAuthorizations

  def getMosaicedRaster(query: RasterQuery, params: GeoMesaCoverageQueryParams) = {
    implicit val timings = if (collectStats) new TimingsImpl else NoOpTimings
    val rasters = getRasters(query)
    val (image, numRasters) = profile(
        RasterUtils.mosaicChunks(rasters, params.width.toInt, params.height.toInt, params.envelope),
      "mosaic")
    if (timings.isInstanceOf[TimingsImpl]) {
      val stat = RasterQueryStat(tableName,
        System.currentTimeMillis(),
        query.toString,
        timings.time("planning"),
        timings.time("scanning") - timings.time("planning"),
        timings.time("mosaic"),
        numRasters)
      this.writeStat(stat, profileTable)
    }
    image
  }

  def getRasters(rasterQuery: RasterQuery)(implicit timings: Timings): Iterator[Raster] = {
    profile({
      val batchScanner = connector.createBatchScanner(tableName, authorizationsProvider.getAuthorizations, numQThreads)
      val plan = queryPlanner.getQueryPlan(rasterQuery, getAvailabilityMap)
      plan match {
        case Some(qp) =>
          configureBatchScanner(batchScanner, qp)
          adaptIteratorToChunks(SelfClosingBatchScanner(batchScanner))
        case _        => Iterator.empty
      }
    }, "scanning")
  }

  def getQueryRecords(numRecords: Int): Iterator[String] = {
    val scanner = connector.createScanner(profileTable, authorizationsProvider.getAuthorizations)
    scanner.iterator.take(numRecords).map(RasterQueryStatTransform.decodeStat)
  }

  def getBounds: BoundingBox = {
    ensureBoundsTableExists()
    val scanner = connector.createScanner(GEOMESA_RASTER_BOUNDS_TABLE, authorizationsProvider.getAuthorizations)
    scanner.setRange(new Range(getBoundsRowID))
    val resultingBounds = SelfClosingScanner(scanner)
    if (resultingBounds.isEmpty) {
      BoundingBox(-180, 180, -90, 90)
    } else {
      //TODO: GEOMESA-646 anti-meridian questions
      reduceValuesToBoundingBox(resultingBounds.map(_.getValue))
    }
  }

  def getAvailableResolutions: Seq[Double] = {
    // TODO: Consider adding resolutions + extent info  https://geomesa.atlassian.net/browse/GEOMESA-645
    getAvailabilityMap.keySet.toSeq.sorted
  }

  def getAvailableGeoHashLengths: Set[Int] = {
    getAvailabilityMap.values().toSet
  }

  def getAvailabilityMap: ImmutableSetMultimap[Double, Int] = {
    AccumuloRasterStore.boundsCache.get(tableName, callable)
  }

  def callable = new Callable[ImmutableSetMultimap[Double, Int]] {
    override def call(): ImmutableSetMultimap[Double, Int] = {
      ensureBoundsTableExists()
      val scanner = connector.createScanner(GEOMESA_RASTER_BOUNDS_TABLE, getAuths)
      scanner.setRange(new Range(getBoundsRowID))
      val scanResultingKeys = SelfClosingScanner(scanner).map(_.getKey).toSeq
      val geohashlens = scanResultingKeys.map(_.getColumnFamily.toString).map(lexiDecodeStringToInt)
      val resolutions = scanResultingKeys.map(_.getColumnQualifier.toString).map(lexiDecodeStringToDouble)
      val m = new ImmutableSetMultimap.Builder[Double, Int]()
      (resolutions zip geohashlens).foreach(x => m.put(x._1, x._2))
      m.build()
    }
  }

  def getGridRange: GridEnvelope2D = {
    val bounds = getBounds
    val resolutions = getAvailableResolutions
    // If no resolutions are available, then we have an empty table so assume 1.0 for now
    val resolution = resolutions match {
      case Nil => 1.0
      case _   => resolutions.min
    }
    val width  = Math.abs(bounds.getWidth / resolution).toInt
    val height = Math.abs(bounds.getHeight / resolution).toInt

    new GridEnvelope2D(0, 0, width, height)
  }

  def adaptIteratorToChunks(iter: java.util.Iterator[Entry[Key, Value]]): Iterator[Raster] = {
    iter.map{ entry => schema.decode((entry.getKey, entry.getValue)) }
  }

  private def dateToAccTimestamp(dt: DateTime): Long =  dt.getMillis / 1000

  private def createBoundsMutation(raster: Raster): Mutation = {
    // write the bounds mutation
    val mutation = new Mutation(getBoundsRowID)
    val value = bboxToValue(BoundingBox(raster.metadata.geom.getEnvelopeInternal))
    val resolution = lexiEncodeDoubleToString(raster.resolution)
    val geohashlen = lexiEncodeIntToString(raster.minimumBoundingGeoHash.map( _.hash.length ).getOrElse(0))
    mutation.put(geohashlen, resolution, value)
    mutation
  }

  private def createMutation(raster: Raster): Mutation = {
    val (key, value) = schema.encode(raster, writeVisibilities)
    val mutation = new Mutation(key.getRow)
    val colFam   = key.getColumnFamily
    val colQual  = key.getColumnQualifier
    val colVis   = key.getColumnVisibilityParsed
    // TODO: WCS: determine if this is wise/useful
    // GEOMESA-562
    val timestamp: Long = dateToAccTimestamp(raster.time)
    mutation.put(colFam, colQual, colVis, timestamp, value)
    mutation
  }

  def putRasters(rasters: Seq[Raster]) {
    rasters.foreach(putRaster)
  }

  def putRaster(raster: Raster) {
    writeMutations(tableName, createMutation(raster))
    writeMutations(GEOMESA_RASTER_BOUNDS_TABLE, createBoundsMutation(raster))
  }

  private def writeMutations(tableName: String, mutations: Mutation*) {
    val writer = connector.createBatchWriter(tableName, bwConfig)
    mutations.foreach { m => writer.addMutation(m) }
    writer.flush()
    writer.close()
  }

  def createTableStructure() = {
    ensureTableExists(tableName)
    ensureBoundsTableExists()
  }

  def ensureBoundsTableExists() = {
    createTable(GEOMESA_RASTER_BOUNDS_TABLE)
    if (!tableOps.listIterators(GEOMESA_RASTER_BOUNDS_TABLE).containsKey("GEOMESA_BBOX_COMBINER")) {
      val bboxcombinercfg =  AccumuloRasterBoundsPlanner.getBoundsScannerCfg(tableName)
      tableOps.attachIterator(GEOMESA_RASTER_BOUNDS_TABLE, bboxcombinercfg)
    }
  }
  
  private def ensureTableExists(tableName: String) {
    // TODO: WCS: ensure that this does not duplicate what is done in AccumuloDataStore
    // Perhaps consolidate with different default configurations
    // GEOMESA-564
    val user = connector.whoami
    val defaultVisibilities = authorizationsProvider.getAuthorizations.toString.replaceAll(",", "&")
    if (!tableOps.exists(tableName)) {
        createTables(user, defaultVisibilities, Array(tableName, profileTable):_*)
    }
  }

  private def createTables(user: String, defaultVisibilities: String, tableNames: String*) = {
    tableNames.foreach(tableName => {
      createTable(tableName)
      AccumuloRasterTableConfig.settings(defaultVisibilities).foreach { case (key, value) =>
        tableOps.setProperty(tableName, key, value)
      }
      AccumuloRasterTableConfig.permissions.split(",").foreach { p =>
        securityOps.grantTablePermission(user, tableName, TablePermission.valueOf(p))
      }
    })
  }

  private def createTable(tableName: String) = {
    if(!tableOps.exists(tableName)) {
      try {
        tableOps.create(tableName)
      } catch {
        case e: TableExistsException => // this can happen with multiple threads but shouldn't cause any issues
      }
    }
  }

  def deleteRasterTable(): Unit = {
    deleteMetaData()
    deleteTable(profileTable)
    deleteTable(tableName)
  }

  private def deleteTable(table: String): Unit = {
    try {
      if (tableOps.exists(table)) {
        tableOps.delete(table)
      }
    } catch {
      case e: Exception => logger.warn(s"Error occurred when attempting to delete table: $table", e)
    }
  }

  private def deleteMetaData(): Unit = {
    try {
      if (tableOps.exists(GEOMESA_RASTER_BOUNDS_TABLE)) {
        val deleter = connector.createBatchDeleter(GEOMESA_RASTER_BOUNDS_TABLE, getAuths, 3, bwConfig)
        val deleteRange = new Range(getBoundsRowID)
        deleter.setRanges(Seq(deleteRange))
        deleter.delete()
        deleter.close()
        AccumuloRasterStore.boundsCache.invalidate(tableName)
      }
    } catch {
      case e: Exception => logger.warn(s"Error occurred when attempting to delete Metadata for table: $tableName")
    }
  }

}

object AccumuloRasterStore {
  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory._
  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.params._

  def apply(username: String,
            password: String,
            instanceId: String,
            zookeepers: String,
            tableName: String,
            auths: String,
            writeVisibilities: String,
            useMock: Boolean = false,
            writeMemoryConfig: Option[String] = None,
            writeThreadsConfig: Option[Int] = None,
            queryThreadsConfig: Option[Int] = None,
            collectStats: Boolean = false): AccumuloRasterStore = {

    val conn = AccumuloStoreHelper.buildAccumuloConnector(username, password, instanceId, zookeepers, useMock)

    val authorizationsProvider = AccumuloStoreHelper.getAuthorizationsProvider(auths.split(","), conn)

    val rasterStore = new AccumuloRasterStore(conn, tableName, authorizationsProvider, writeVisibilities,
                        writeMemoryConfig, writeThreadsConfig, queryThreadsConfig, collectStats)
    // this will actually create the Accumulo Table
    rasterStore.createTableStructure()

    rasterStore
  }

  def apply(config: JMap[String, Serializable]): AccumuloRasterStore = {
    val username: String     = userParam.lookUp(config).asInstanceOf[String]
    val password: String     = passwordParam.lookUp(config).asInstanceOf[String]
    val instance: String     = instanceIdParam.lookUp(config).asInstanceOf[String]
    val zookeepers: String   = zookeepersParam.lookUp(config).asInstanceOf[String]
    val auths: String        = authsParam.lookupOpt[String](config).getOrElse("")
    val vis: String          = visibilityParam.lookupOpt[String](config).getOrElse("")
    val tablename: String    = tableNameParam.lookUp(config).asInstanceOf[String]
    val useMock: Boolean     = mockParam.lookUp(config).asInstanceOf[String].toBoolean
    val wMem: Option[String] = writeMemoryParam.lookupOpt[String](config)
    val wThread: Option[Int] = writeThreadsParam.lookupOpt[Int](config)
    val qThread: Option[Int] = queryThreadsParam.lookupOpt[Int](config)
    val cStats: Boolean      = java.lang.Boolean.valueOf(statsParam.lookupOpt[Boolean](config).getOrElse(false))

    AccumuloRasterStore(username, password, instance, zookeepers, tablename,
      auths, vis, useMock, wMem, wThread, qThread, cStats)
  }

  val boundsCache =
    CacheBuilder.newBuilder()
      .expireAfterAccess(10, TimeUnit.MINUTES)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build[String, ImmutableSetMultimap[Double, Int]]
}

object AccumuloRasterTableConfig {
  /**
   * documentation for raster table settings:
   *
   * table.security.scan.visibility.default
   * - The default visibility for the table
   *
   * table.iterator.majc.vers.opt.maxVersions
   * - Versioning iterator setting
   * - max versions, major compaction
   *
   * table.iterator.minc.vers.opt.maxVersions
   * - Versioning iterator setting
   * - max versions, minor compaction
   *
   * table.iterator.scan.vers.opt.maxVersions
   * - Versioning iterator setting
   * - max versions, scan time
   *
   * table.split.threshold
   * - The split threshold for the table, when reached
   * - Accumulo splits the table into tablets of this size.
   *
   * @param visibilities
   * @return
   */
  def settings(visibilities: String): Map[String, String] = Map (
    "table.security.scan.visibility.default" -> visibilities,
    "table.iterator.majc.vers.opt.maxVersions" -> "1",
    "table.iterator.minc.vers.opt.maxVersions" -> "1",
    "table.iterator.scan.vers.opt.maxVersions" -> "1",
    "table.split.threshold" -> "512M"
  )
  val permissions = "BULK_IMPORT,READ,WRITE,ALTER_TABLE"
}

