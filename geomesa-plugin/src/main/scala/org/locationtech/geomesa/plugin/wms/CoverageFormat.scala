/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.wms

import java.io.File
import java.util.{List => JList}

import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.factory.Hints
import org.geotools.parameter.{DefaultParameterDescriptorGroup, ParameterGroup}
import org.opengis.parameter.GeneralParameterDescriptor

class CoverageFormat extends AbstractGridFormat {
  mInfo = new java.util.HashMap[String, String]()
  mInfo.put("name", "Accumulo Coverage Format")
  mInfo.put("description", "Serve tile imagery from Accumulo tables with a specific format")
  mInfo.put("vendor", "GeoMesa")
  mInfo.put("docURL", "http://www.geomesa.org")
  mInfo.put("version", "1.0")

  val parameterDescriptors =
    Array[GeneralParameterDescriptor](AbstractGridFormat.READ_GRIDGEOMETRY2D, AbstractGridFormat.TIME)

  val defaultParameterGroup = new DefaultParameterDescriptorGroup(mInfo, parameterDescriptors)

  val coverageFormatReadParameters = new ParameterGroup(defaultParameterGroup)
  writeParameters = null

  override def getReadParameters: ParameterGroup = coverageFormatReadParameters
  override def getReader(source: AnyRef) = getReader(source, null)
  override def getReader(source: AnyRef, hints: Hints) = source match {
    case file: File => new CoverageReader(file.getPath)
    case path: String => new CoverageReader(path)
    case unk => throw new Exception(s"unexpected data type for reader source: ${Option(unk).map(_.getClass.getName).getOrElse("null")}")
  }
  override def accepts(input: AnyRef) = true
  override def accepts(source: AnyRef, hints: Hints) = true
  override def getWriter(destination: AnyRef) = throw new UnsupportedOperationException("Unsupported")
  override def getWriter(destination: AnyRef, hints: Hints) = throw new UnsupportedOperationException("Unsupported")
  override def getDefaultImageIOWriteParameters = throw new UnsupportedOperationException("Unsupported")
}


