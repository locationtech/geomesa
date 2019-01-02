/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.wcs

import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.coverage.grid.io.AbstractGridFormat._
import org.geotools.factory.{GeoTools, Hints}
import org.geotools.parameter.{DefaultParameterDescriptorGroup, ParameterGroup}
import org.opengis.coverage.grid.Format
import org.opengis.parameter.GeneralParameterDescriptor

class GeoMesaCoverageFormat extends AbstractGridFormat with Format {

  mInfo = new java.util.HashMap[String, String]()
  mInfo.put("name", GeoMesaCoverageFormat.CoverageFormatName)
  mInfo.put("description", "Tile imagery in Apache Accumulo\u2122")
  mInfo.put("vendor", "GeoMesa")
  mInfo.put("docURL", "http://www.geomesa.org")
  mInfo.put("version", "1.0")

  private val parameterDescriptors = Array[GeneralParameterDescriptor](READ_GRIDGEOMETRY2D)
  private val defaultParameterGroup = new DefaultParameterDescriptorGroup(mInfo, parameterDescriptors)

  readParameters = new ParameterGroup(defaultParameterGroup)
  writeParameters = null

  override def getReader(source: AnyRef) = getReader(source, null)

  override def getReader(source: AnyRef, hints: Hints) = {
    source match {
      case path: String => new GeoMesaCoverageReader(path, hints)
      case unk =>
        throw new RuntimeException("unexpected data type for reader source: " +
            s"${Option(unk).map(_.getClass.getName).getOrElse("null")}")
    }
  }

  override def accepts(input: AnyRef) = accepts(input, null)

  override def accepts(source: AnyRef, hints: Hints) = {
    source match {
      case string: String => string.startsWith("accumulo://")
      case _ => false
    }
  }

  override def getWriter(destination: AnyRef) = throw new UnsupportedOperationException("Unsupported")

  override def getWriter(destination: AnyRef, hints: Hints) = throw new UnsupportedOperationException("Unsupported")

  override def getDefaultImageIOWriteParameters = throw new UnsupportedOperationException("Unsupported")
}

object GeoMesaCoverageFormat {
  val CoverageFormatName = "Accumulo (Geomesa Raster)"
}
