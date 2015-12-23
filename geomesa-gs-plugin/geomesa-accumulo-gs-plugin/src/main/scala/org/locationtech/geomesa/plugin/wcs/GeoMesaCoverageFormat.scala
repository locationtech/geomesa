/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.wcs

import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.coverage.grid.io.AbstractGridFormat._
import org.geotools.factory.Hints
import org.geotools.parameter.{DefaultParameterDescriptorGroup, ParameterGroup}
import org.opengis.coverage.grid.Format
import org.opengis.parameter.GeneralParameterDescriptor

class GeoMesaCoverageFormat extends AbstractGridFormat() with Format {
  mInfo = new java.util.HashMap[String, String]()
  mInfo.put("name", GeoMesaCoverageFormat.coverageFormatName)
  mInfo.put("description", "Serve tile imagery from GeoMesa tables with a specific format")
  mInfo.put("vendor", "CCRI")
  mInfo.put("docURL", "http://www.geomesa.org")
  mInfo.put("version", "1.0")

  val parameterDescriptors = Array[GeneralParameterDescriptor](READ_GRIDGEOMETRY2D)
  val defaultParameterGroup = new DefaultParameterDescriptorGroup(mInfo, parameterDescriptors)

  readParameters = new ParameterGroup(defaultParameterGroup)
  writeParameters = null

  override def getReader(source: AnyRef) = getReader(source, null)

  override def getReader(source: AnyRef, hints: Hints) = source match {
    case path: String => new GeoMesaCoverageReader(path, hints)
    case unk => throw new Exception(s"unexpected data type for reader source: ${Option(unk).map(_.getClass.getName).getOrElse("null")}")
  }

  override def accepts(input: AnyRef) = true

  override def accepts(source: AnyRef, hints: Hints) = true

  override def getWriter(destination: AnyRef) = throw new UnsupportedOperationException("Unsupported")

  override def getWriter(destination: AnyRef, hints: Hints) = throw new UnsupportedOperationException("Unsupported")

  override def getDefaultImageIOWriteParameters = throw new UnsupportedOperationException("Unsupported")
}

object GeoMesaCoverageFormat {
  val coverageFormatName = "Geomesa Coverage Format"
}
