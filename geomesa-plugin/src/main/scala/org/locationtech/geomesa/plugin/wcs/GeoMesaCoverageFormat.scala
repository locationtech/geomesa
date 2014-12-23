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

package org.locationtech.geomesa.plugin.wcs

import java.io.File

import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.coverage.grid.io.AbstractGridFormat._
import org.geotools.factory.Hints
import org.geotools.parameter.{DefaultParameterDescriptor, DefaultParameterDescriptorGroup, ParameterGroup}
import org.locationtech.geomesa.plugin.wcs.GeoMesaCoverageFormat._
import org.locationtech.geomesa.raster.ingest.GeoserverClientService
import org.opengis.coverage.grid.Format
import org.opengis.parameter.GeneralParameterDescriptor

object GeoMesaCoverageFormat {
  val RESOLUTION = new DefaultParameterDescriptor[String]("RESOLUTION", classOf[String], null, "1.0")
}

class GeoMesaCoverageFormat extends AbstractGridFormat() with Format {
  mInfo = new java.util.HashMap[String, String]()
  mInfo.put("name", GeoserverClientService.coverageFormatName)
  mInfo.put("description", "Serve tile imagery from GeoMesa tables with a specific format")
  mInfo.put("vendor", "CCRI")
  mInfo.put("docURL", "http://www.geomesa.org")
  mInfo.put("version", "1.0")

  val parameterDescriptors = Array[GeneralParameterDescriptor](READ_GRIDGEOMETRY2D, RESOLUTION)
  val defaultParameterGroup = new DefaultParameterDescriptorGroup(mInfo, parameterDescriptors)

  readParameters = new ParameterGroup(defaultParameterGroup)
  writeParameters = null

  override def getReader(source: AnyRef) = getReader(source, null)
  override def getReader(source: AnyRef, hints: Hints) = source match {
    case file: File => new GeoMesaCoverageReader(file.getPath, hints)
    case path: String => new GeoMesaCoverageReader(path, hints)
    case unk => throw new Exception(s"unexpected data type for reader source: ${Option(unk).map(_.getClass.getName).getOrElse("null")}")
  }
  override def accepts(input: AnyRef) = true
  override def accepts(source: AnyRef, hints: Hints) = true
  override def getWriter(destination: AnyRef) = throw new UnsupportedOperationException("Unsupported")
  override def getWriter(destination: AnyRef, hints: Hints) = throw new UnsupportedOperationException("Unsupported")
  override def getDefaultImageIOWriteParameters = throw new UnsupportedOperationException("Unsupported")
}
