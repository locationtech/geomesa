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

import java.io.{FilenameFilter, File}

import org.geotools.coverage.grid.GridCoverage2D
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core.index.DecodedIndex
import org.locationtech.geomesa.raster.data.Raster
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.locationtech.geomesa.tools.Utils.Formats._

import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Try

class LocalRasterIngest(config: Map[String, Option[String]]) extends RasterIngest {

  lazy val path = config(IngestRasterParams.FILE_PATH).get
  lazy val fileType = config(IngestRasterParams.FORMAT).get
  lazy val rasterName = config(IngestRasterParams.TABLE).get
  lazy val parLevel = config(IngestRasterParams.PARLEVEL).get.toInt
  lazy val cs = createCoverageStore(config)

  val df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  def runIngestTask() = Try {
    val fileOrDir = new File(path)

    val files =
      (if (fileOrDir.isDirectory)
         fileOrDir.listFiles(new FilenameFilter() {
           override def accept(dir: File, name: String) =
             getFileExtension(name).endsWith(fileType)
         })
       else Array(fileOrDir)).par

    files.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parLevel))
    files.foreach(ingestRasterFromFile(_))

    cs.geoserverClientServiceO.foreach { geoserverClientService => {
      geoserverClientService.registerRasterStyles()
      geoserverClientService.registerRaster(rasterName, rasterName, "Raster data", None)
    }}
  }

  def ingestRasterFromFile(file: File) {
    val rasterReader = getReader(file, fileType)
    val rasterGrid: GridCoverage2D = rasterReader.read(null)

    val projection = rasterGrid.getCoordinateReferenceSystem.getName.toString
    if (projection != "EPSG:WGS 84") {
      throw new Exception(s"Error, Projection: $projection is unsupported.")
    }

    val envelope = rasterGrid.getEnvelope2D
    val bbox = BoundingBox(envelope.getMinX, envelope.getMaxX, envelope.getMinY, envelope.getMaxY)

    val ingestTime = config(IngestRasterParams.TIME).map(df.parseDateTime(_)).getOrElse(new DateTime(DateTimeZone.UTC))
    val metadata = DecodedIndex(Raster.getRasterId(rasterName), bbox.geom, Some(ingestTime.getMillis))

    val res =  RasterUtils.sharedRasterParams(rasterGrid.getGridGeometry, envelope).suggestedQueryResolution

    val raster = Raster(rasterGrid.getRenderedImage, metadata, res)

    cs.saveRaster(raster)
  }
}
