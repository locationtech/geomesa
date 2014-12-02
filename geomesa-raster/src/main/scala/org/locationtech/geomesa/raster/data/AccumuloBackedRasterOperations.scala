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

import org.apache.accumulo.core.client.{BatchWriterConfig, Connector, TableExistsException}
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.accumulo.core.security.{ColumnVisibility, TablePermission}
import org.apache.hadoop.io.Text
import org.joda.time.DateTime
import org.locationtech.geomesa.core.security.AuthorizationsProvider
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.utils.geohash.GeoHash

trait RasterOperations {
  def ensureTableExists(): Unit
  def getAuths(): String
  def getVisibility(): String
  def putRaster(raster: Raster): Unit
}

/**
 * This class handles Accumulo related operations including tables creation,
 * read data from /write data to Accumulo tables.
 *
 * @param connector
 * @param coverageTable
 * @param authorizationsProvider
 * @param writeVisibilities
 * @param shardsConfig
 * @param writeMemoryConfig
 * @param writeThreadsConfig
 * @param queryThreadsConfig
 */
class AccumuloBackedRasterOperations(val connector: Connector,
                                     val coverageTable: String,
                                     val authorizationsProvider: AuthorizationsProvider,
                                     val writeVisibilities: String,
                                     shardsConfig: Option[Int] = None,
                                     writeMemoryConfig: Option[Long] = None,
                                     writeThreadsConfig: Option[Int] = None,
                                     queryThreadsConfig: Option[Int] = None) extends RasterOperations {
  //By default having at least as many shards as tservers provides optimal parallelism in queries
  val shards = shardsConfig.getOrElse(connector.instanceOperations().getTabletServers.size())
  val writeMemory = writeMemoryConfig.getOrElse(10000L)
  val writeThreads = writeThreadsConfig.getOrElse(10)
  val bwConfig: BatchWriterConfig =
    new BatchWriterConfig().setMaxMemory(writeMemory).setMaxWriteThreads(writeThreads)

  private val tableOps = connector.tableOperations()
  private val securityOps = connector.securityOperations

  def getAuths() = authorizationsProvider.getAuthorizations.toString

  def getVisibility() = writeVisibilities

  def putRasters(rasters: Seq[Raster]) {
    rasters.foreach{ putRaster(_) }
  }

  def putRaster(raster: Raster) {
    writeMutations(createMutation(raster))
  }

  //Should support ECQL
  def getRasters(query: String): Iterator[Raster] = {
    null
  }

  def ensureTableExists() = ensureTableExists(coverageTable)

  private def getRow(geo: GeoHash) = new Text(s"~${geo.prec}~${geo.hash}")

  private def getCF(raster: Raster): Text = new Text("")

  private def getCQ(raster: Raster): Text = {
    val timeStampString = dateToAccTimestamp(raster.getTime).toString
    new Text(s"${raster.getID}~$timeStampString")
  }

  /**
   * Serialize Raster instance to byte array
   *
   * @param raster Raster instance
   * @return Value
   */
  private def encodeValue(raster: Raster): Value =
    new Value(raster.encodeValue)

  /**
   * Deserialize value in byte array to Raster instance
   *
   * @param value Value obtained from Accumulo table
   * @return byte array
   */
  private def decodeValue(value: Value): Raster =
    null

  private def dateToAccTimestamp(dt: DateTime): Long =  dt.getMillis / 1000

  /**
   * Create Mutation instance from input Raster instance
   *
   * @param raster Raster instance
   * @return Mutation instance
   */
  private def createMutation(raster: Raster): Mutation = {
    val mutation = new Mutation(getRow(raster.getMbgh))
    val colFam = getCF(raster)
    val colQual = getCQ(raster)
    val timestamp: Long = dateToAccTimestamp(raster.getTime)
    val colVis = new ColumnVisibility(writeVisibilities)
    val value = encodeValue(raster)
    mutation.put(colFam, colQual, colVis, timestamp, value)
    mutation
  }

  /**
   * Write mutations into accumulo table
   *
   * @param mutations
   */
  private def writeMutations(mutations: Mutation*) {
    val writer = connector.createBatchWriter(coverageTable, bwConfig)
    mutations.foreach { m => writer.addMutation(m) }
    writer.flush()
    writer.close()
  }

  /**
   * Create table if it doesn't exist.
   *
   * @param tableName
   */
  private def ensureTableExists(tableName: String) {
    val user = connector.whoami
    val defaultVisibilities = authorizationsProvider.getAuthorizations.toString.replaceAll(",", "&")
    if (!tableOps.exists(tableName)) {
      try {
        tableOps.create(tableName)
        CoverageTableConfig.settings(defaultVisibilities).foreach { case (key, value) =>
          tableOps.setProperty(tableName, key, value)
        }
        CoverageTableConfig.permissions.split(",").foreach { p =>
          securityOps.grantTablePermission(user, tableName, TablePermission.valueOf(p))
        }
      } catch {
        case e: TableExistsException => // this can happen with multiple threads but shouldn't cause any issues
      }
    }
  }
}

object AccumuloBackedRasterOperations {
}

object CoverageTableConfig {
  def settings(visibilities: String): Map[String, String] = Map (
    "table.security.scan.visibility.default" -> visibilities,
    "table.iterator.majc.vers.opt.maxVersions" -> "2147483647",
    "table.iterator.minc.vers.opt.maxVersions" -> "2147483647",
    "table.iterator.scan.vers.opt.maxVersions" -> "2147483647",
    "table.split.threshold" -> "16M"
  )
  val permissions = "BULK_IMPORT,READ,WRITE,ALTER_TABLE"
}