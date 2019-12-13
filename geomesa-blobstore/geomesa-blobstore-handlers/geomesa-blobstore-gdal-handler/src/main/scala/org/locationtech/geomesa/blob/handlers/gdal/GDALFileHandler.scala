/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.handlers.gdal

import java.io.File
import java.lang.Boolean
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.gdal.gdal.{Dataset, Transformer, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.locationtech.geomesa.blob.api.handlers.AbstractFileHandler

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * GdalFileHandler uses GDAL to load metadata from candidate files to determine if
  * they can be ingested into the blob store.
  *
  */
class GDALFileHandler extends AbstractFileHandler with LazyLogging {
  import GDALFileHandler._

  val geomFactory = new GeometryFactory()
  val initialized = Try(gdal.AllRegister()) match {
    case Success(v) =>
      logger.info("Successfully loaded GDAL native interface bindings")
      true
    case Failure(e) =>
      logger.error("Failed to load GDAL native libraries in GeoMesa GDAL FileHandler, " +
        "ensure that the path of the native GDAL libraries and JNI libraries are in the " +
        "LD_LIBRARY_PATH environment variable", e)
      false
  }

  override def getGeometryFromFile(file: File): Option[Geometry] = {
    if (!initialized) {
      logger.warn(s"Unable to proceed - ${this.getClass} was not properly initialized")
      None
    } else {
      val gdalFile = gdal.Open(file.getAbsolutePath, gdalconstConstants.GA_ReadOnly)
      if (gdalFile == null) {
        None
      } else {
        try {
          getImageBoundsGeometry(gdalFile)
        } finally {
          gdalFile.delete()
        }
      }
    }
  }

  override def canProcess(file: File, map: util.Map[String, String]): Boolean = {
    if (!initialized) {
      logger.warn(s"Unable to proceed - ${this.getClass} was not properly initialized")
      false
    } else {
      val potentialGdalDataSet = gdal.Open(file.getAbsolutePath, gdalconstConstants.GA_ReadOnly)
      if (potentialGdalDataSet == null) {
        false
      } else {
        potentialGdalDataSet.delete()
        true
      }
    }
  }

  override def getDateFromFile(file: File): Option[util.Date] = super.getDateFromFile(file)

  private def getImageBoundsGeometry(file: Dataset): Option[Geometry] = {
    val maxX = file.getRasterXSize - minXY
    val maxY = file.getRasterYSize - minXY
    val coordinates = Array((minXY, minXY), (maxX, minXY), (maxX, maxY), (minXY, maxY), (minXY, minXY))
    val transformer = new Transformer(file, null, opts)
    Some(geomFactory.createPolygon(coordinates.flatMap(p => applyTransformer(p._1, p._2, transformer))))
  }

  private def applyTransformer(x: Double, y: Double, tran: Transformer): Option[Coordinate] = {
    Try {
      val point = Array.ofDim[Double](3)
      // This can throw exceptions if a GDAL version mismatch occurs
      val ret = tran.TransformPoint(point, 0, x, y)
      if (ret == 0) {
        None
      } else {
        Some(new Coordinate(point(0), point(1)))
      }
    } match {
      case Failure(e) =>
        logger.error("Failed to apply GDAL Transformer in GDALFileHandler", e)
        None
      case Success(p) =>
        p
    }

  }

}

object GDALFileHandler {
  val dst_srs =
    """DST_SRS=GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",
      |SPHEROID["WGS_1984",6378137,298.257223563]],
      |PRIMEM["Greenwich",0],
      |UNIT["Degree",0.017453292519943295]]""".stripMargin
  val opts = new util.Vector[String](List(dst_srs).asJava)
  val minXY = 0.5
}
