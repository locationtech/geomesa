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

import java.io.{File, FilenameFilter}
import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IOUtils, SequenceFile}
import org.geotools.coverage.grid.GridCoverage2D
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core.index.DecodedIndex
import org.locationtech.geomesa.raster.data.Raster
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import org.locationtech.geomesa.tools.Utils.Formats._
import org.locationtech.geomesa.utils.geohash.BoundingBox

import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Try

class RasterFilesSerialization(config: Map[String, Option[String]]) extends RasterIngest with Logging {

  lazy val path = config(IngestRasterParams.FILE_PATH).get
  lazy val fileType = config(IngestRasterParams.FORMAT).get
  lazy val rasterName = config(IngestRasterParams.TABLE).get
  lazy val visibilities = config(IngestRasterParams.VISIBILITIES).get
  lazy val parLevel = config(IngestRasterParams.PARLEVEL).get.toInt

  val df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val conf = new Configuration()

  def runSerializationTask() = Try {
    val fileOrDir = new File(path)
    val outPath = s"/tmp/raster_ingest_${UUID.randomUUID.toString}"

    val files =
      (if (fileOrDir.isDirectory)
         fileOrDir.listFiles(new FilenameFilter() {
           override def accept(dir: File, name: String) =
             getFileExtension(name).endsWith(fileType)
         })
       else Array(fileOrDir)).par

    files.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parLevel))
    files.foreach { file =>
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

      val bytes = Raster.encodeToBytes(raster)
      val name = raster.id
      val outFile = s"$outPath/${raster.id}.seq"
      logger.debug("Save bytes into Hdfs file: " + outFile)
      saveBytesToHdfsFile(name, bytes, outFile)
    }
    outPath
  }

  def saveBytesToHdfsFile(name: String, bytes: Array[Byte], outFile: String) {
    val outPath = new Path(outFile)
    val key = new BytesWritable
    val value = new BytesWritable
    var writer: SequenceFile.Writer = null

    try {
      val optPath = SequenceFile.Writer.file(outPath)
      val optKey =  SequenceFile.Writer.keyClass(key.getClass)
      val optVal =  SequenceFile.Writer.valueClass(value.getClass)
      writer = SequenceFile.createWriter(conf, optPath, optKey, optVal)
      writer.append(new BytesWritable(name.getBytes), new BytesWritable(bytes))
    } catch {
      case e: Exception =>
        System.out.println("Cannot write to Hdfs sequence file: " + e.getMessage())
    } finally {
      IOUtils.closeStream(writer)
    }
  }
}
