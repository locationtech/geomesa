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

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client._
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.factory.Hints
import org.locationtech.geomesa.core.security.AuthorizationsProvider
import org.locationtech.geomesa.raster.ingest.RasterMetadata

/**
 * @param connector Accumulo connector
 * @param coverageTable Table name in Accumulo to store coverage data
 * @param authorizationsProvider Provides the authorizations used to access data
 * @param writeVisibilities Visibilities applied to any data written by this store
 * @param queryThreadsConfig Threads number used for querying data
 * @param writeMemoryConfig Memory allocation for writing data
 * @param writeThreadsConfig Threads number used for writing data
 *
 *
 *  This class handles coverage DataStores which stores coverage data to and queries coverages
 *  from Accumulo Tables.
 */
class AccumuloCoverageStore(val connector: Connector,
                            val coverageTable: String,
                            val authorizationsProvider: AuthorizationsProvider,
                            val writeVisibilities: String,
                            val shardsConfig: Option[Int] = None,
                            val writeMemoryConfig: Option[Long] = None,
                            val writeThreadsConfig: Option[Int] = None,
                            val queryThreadsConfig: Option[Int] = None)
  extends Logging {
  //By default having at least as many shards as tservers provides optimal parallelism in queries
  val shards = shardsConfig.getOrElse(connector.instanceOperations().getTabletServers.size())
  val writeMemory = writeMemoryConfig.getOrElse(10000L)
  val writeThreads = writeThreadsConfig.getOrElse(10)
  val bwConfig: BatchWriterConfig =
    new BatchWriterConfig().setMaxMemory(writeMemory).setMaxWriteThreads(writeThreads)

  val coverageOps: AccumuloCoverageOperations =
    new AccumuloCoverageOperations(connector,
                                   coverageTable,
                                   writeVisibilities,
                                   authorizationsProvider,
                                   shards,
                                   bwConfig,
                                   writeMemory,
                                   writeThreads)

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  coverageOps.ensureTableExists(coverageTable)


  def saveRaster(raster: GridCoverage2D, rm: RasterMetadata) =
    coverageOps.saveChunk(raster, rm, writeVisibilities)
}

object AccumuloCoverageStore {
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