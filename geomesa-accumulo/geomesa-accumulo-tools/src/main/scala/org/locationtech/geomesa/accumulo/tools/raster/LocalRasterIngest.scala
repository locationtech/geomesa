/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.raster

import java.io.{File, FilenameFilter}
import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import org.geotools.coverage.grid.GridCoverage2D
import org.locationtech.geomesa.accumulo.tools.ingest.IngestRasterCommand
import org.locationtech.geomesa.raster.data.Raster
import org.locationtech.geomesa.raster.index.RasterEntryDecoder.DecodedIndexValue
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import org.locationtech.geomesa.utils.geohash.BoundingBox

import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Try

class LocalRasterIngest(config: Map[String, Option[String]]) extends RasterIngest {

  lazy val path = config(IngestRasterParams.FILE_PATH).get
  lazy val fileType = config(IngestRasterParams.FORMAT).get
  lazy val rasterName = config(IngestRasterParams.TABLE).get
  lazy val parLevel = config(IngestRasterParams.PARLEVEL).get.toInt
  lazy val rs = createRasterStore(config)

  val df = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  def runIngestTask() = Try {
    val fileOrDir = new File(path)

    val files =
      (if (fileOrDir.isDirectory)
         fileOrDir.listFiles(new FilenameFilter() {
           override def accept(dir: File, name: String) =
             IngestRasterCommand.getFileExtension(name).endsWith(fileType)
         })
       else Array(fileOrDir)).par

    files.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parLevel))
    files.foreach(ingestRasterFromFile)
    rs.close()
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

    val ingestTime = config(IngestRasterParams.TIME).map(ZonedDateTime.parse(_, df)).getOrElse(ZonedDateTime.now(ZoneOffset.UTC))
    val metadata = DecodedIndexValue(Raster.getRasterId(rasterName), bbox.geom, Some(Date.from(ingestTime.toInstant)), null)

    val res =  RasterUtils.sharedRasterParams(rasterGrid.getGridGeometry, envelope).suggestedQueryResolution

    val raster = Raster(rasterGrid.getRenderedImage, metadata, res)

    rs.putRaster(raster)
  }
}
