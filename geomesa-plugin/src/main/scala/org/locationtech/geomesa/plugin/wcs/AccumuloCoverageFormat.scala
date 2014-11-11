package org.locationtech.geomesa.plugin.wcs

import java.io.File

import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.coverage.grid.io.AbstractGridFormat._
import org.geotools.factory.Hints
import org.geotools.parameter.{DefaultParameterDescriptorGroup, ParameterGroup}
import org.opengis.coverage.grid.Format
import org.opengis.parameter.GeneralParameterDescriptor

class AccumuloCoverageFormat extends AbstractGridFormat() with Format {
  mInfo = new java.util.HashMap[String, String]()
  mInfo.put("name", "New Accumulo Coverage Format")
  mInfo.put("description", "Serve tile imagery from Accumulo tables with a specific format")
  mInfo.put("vendor", "CCRI")
  mInfo.put("docURL", "http://www.ccri.com")
  mInfo.put("version", "1.0")
  val parameterDescriptors = Array[GeneralParameterDescriptor](READ_GRIDGEOMETRY2D, TIME)
  val defaultParameterGroup = new DefaultParameterDescriptorGroup(mInfo, parameterDescriptors)

  readParameters = new ParameterGroup(defaultParameterGroup)
  writeParameters = null

  override def getReader(source: AnyRef) = getReader(source, null)
  override def getReader(source: AnyRef, hints: Hints) = source match {
    case file: File => new AccumuloCoverageReader(file.getPath, hints)
    case path: String => new AccumuloCoverageReader(path, hints)
    case unk => throw new Exception(s"unexpected data type for reader source: ${Option(unk).map(_.getClass.getName).getOrElse("null")}")
  }
  override def accepts(input: AnyRef) = true
  override def accepts(source: AnyRef, hints: Hints) = true
  override def getWriter(destination: AnyRef) = throw new UnsupportedOperationException("Unsupported")
  override def getWriter(destination: AnyRef, hints: Hints) = throw new UnsupportedOperationException("Unsupported")
  override def getDefaultImageIOWriteParameters = throw new UnsupportedOperationException("Unsupported")
}
