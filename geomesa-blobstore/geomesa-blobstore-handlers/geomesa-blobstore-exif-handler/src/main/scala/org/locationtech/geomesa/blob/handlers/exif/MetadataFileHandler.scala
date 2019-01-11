/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.handlers.exif

import java.io.File
import java.lang.Boolean
import java.util
import java.util.Date

import com.drew.imaging._
import com.drew.metadata.exif.GpsDirectory
import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.blob.api.handlers.AbstractFileHandler
import org.locationtech.geomesa.utils.text.WKTUtils

import scala.collection.JavaConversions._

class MetadataFileHandler extends AbstractFileHandler {
  override def getGeometryFromFile(file: File): Option[Geometry] = {
    val gps = ImageMetadataReader.readMetadata(file).getDirectoriesOfType(classOf[GpsDirectory])
    val gl = gps.head.getGeoLocation

    Option(WKTUtils.read(s"POINT(${gl.getLongitude} ${gl.getLatitude})"))
  }

  override def canProcess(file: File, params: util.Map[String, String]): Boolean = {
    val gps = ImageMetadataReader.readMetadata(file).getDirectoriesOfType(classOf[GpsDirectory])
    if (gps == null) {
      false
    } else {
      val geo = gps.head.getGeoLocation
      geo != null
    }
  }

  // TODO: Implement date extraction
  // https://geomesa.atlassian.net/browse/GEOMESA-955
  override def getDateFromFile(file: File): Option[Date] = super.getDateFromFile(file)
}
