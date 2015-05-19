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

import com.twitter.scalding._
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.hadoop.io.BytesWritable
import org.locationtech.geomesa.raster.data.{AccumuloRasterStore, Raster}
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams

class ScaldingRasterIngestJob(args: Args) extends Job(args) with RasterIngest with Logging {

  var lineNumber            = 0
  var failures              = 0
  var successes             = 0

  lazy val pathList        = DelimitedIngest.decodeFileList(args(IngestRasterParams.HDFS_FILES))
  lazy val fileType        = args(IngestRasterParams.FORMAT)
  lazy val rasterName      = args(IngestRasterParams.TABLE)
  lazy val isTestRun       = args(IngestRasterParams.IS_TEST_INGEST).toBoolean

  // non-serializable resources.
  class Resources {
    val rs = createRasterStore(args)
    def release(): Unit = {}
  }

  def printStatInfo() {
    Mode.getMode(args) match {
      case Some(Hdfs(_, _)) =>
        logger.info("Ingest completed in HDFS mode")
      case _ =>
        logger.warn("Could not determine job mode")
    }
  }

  // Check to see if this an actual ingest job or just a test.
  if (!isTestRun) {
    new MultipleWritableSequenceFiles[BytesWritable, BytesWritable](pathList, ('k, 'v)).using(new Resources)
      .foreach('v) { (cres: Resources, v: BytesWritable) => lineNumber += 1; ingestRaster(cres.rs, v.getBytes) }
  }

  def ingestRaster(rs: AccumuloRasterStore, serializedRaster: Array[Byte]) {
    val raster = Raster(serializedRaster)
    logger.info("Ingest raster: " + raster.id)
    rs.putRaster(raster)
  }
}

