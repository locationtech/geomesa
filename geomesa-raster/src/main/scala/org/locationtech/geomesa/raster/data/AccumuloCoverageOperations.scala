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

import org.apache.accumulo.core.client.{TableExistsException, BatchWriterConfig, Connector}
import org.apache.accumulo.core.data.{Value, Mutation}
import org.apache.accumulo.core.security.{TablePermission, ColumnVisibility}
import org.apache.hadoop.io.Text
import org.geotools.coverage.grid.GridCoverage2D
import org.joda.time.DateTime
import org.locationtech.geomesa.core.security.AuthorizationsProvider
import org.locationtech.geomesa.raster.ingest.RasterMetadata
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.GeoHash

trait CoverageOperations {
  def saveChunk(raster: GridCoverage2D, rm: RasterMetadata, visibilities: String): Unit
}

class AccumuloCoverageOperations(connector: Connector,
                                 coverageTable: String,
                                 writeVisibilities: String,
                                 authorizationsProvider: AuthorizationsProvider,
                                 maxShard: Int,
                                 bwConfig: BatchWriterConfig,
                                 writeMemory: Long,
                                 writeThreads: Int) extends CoverageOperations {
  private val tableOps = connector.tableOperations()
  private val securityOps = connector.securityOperations

  private def getRow(geo: GeoHash) = new Text(s"~${geo.prec}~${geo.hash}")

  private def getCF(rm: RasterMetadata): Text = new Text("")

  private def getCQ(rm: RasterMetadata): Text = {
    val timeStampString = dateToAccTimestamp(rm.time).toString
    new Text(s"${rm.id}~$timeStampString")
  }

  private def encodeValue(raster: GridCoverage2D): Value = {
    //TODO: Replace with aannex's encoding method
    new Value(RasterUtils.doubleToBytes(1.0D))
  }

  def dateToAccTimestamp(dt: DateTime): Long =  dt.getMillis / 1000

  def saveChunk(raster: GridCoverage2D, rm: RasterMetadata, visibilities: String): Unit = {
    writeMutations(createMutation(raster, rm, visibilities))
  }

  def createMutation(raster: GridCoverage2D, rm: RasterMetadata, visibilities: String): Mutation = {
    val mutation = new Mutation(getRow(rm.mbgh))
    val colFam = getCF(rm)
    val colQual = getCQ(rm)
    val timestamp: Long = dateToAccTimestamp(rm.time)
    val colVis = new ColumnVisibility(visibilities)
    val value = encodeValue(raster)
    mutation.put(colFam, colQual, colVis, timestamp, value)
    mutation
  }

  /**
   * Write mutations into accumulo table
   *
   * @param mutations
   */
  def writeMutations(mutations: Mutation*): Unit = {
    val writer = connector.createBatchWriter(coverageTable, bwConfig)
    mutations.foreach { m => writer.addMutation(m) }
    writer.flush()
    writer.close()
  }

  def ensureTableExists(tableName: String) = {
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

object AccumuloCoverageOperations {

}