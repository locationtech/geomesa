/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import java.util.Locale

import org.apache.commons.compress.compressors.bzip2.BZip2Utils
import org.apache.commons.compress.compressors.gzip.GzipUtils
import org.apache.commons.compress.compressors.xz.XZUtils
import org.apache.commons.io.FilenameUtils

object DataFormats extends Enumeration {

  type DataFormat = Value
  val Arrow, Avro, Bin, Csv, Gml, Json, Leaflet , Null, Shp, Tsv, Xml = Value

  /**
    * Returns either the format, or the extension as a string if it doesn't match
    *
    * @param name filename
    * @return
    */
  def fromFileName(name: String): Either[String, DataFormat] = {
    val filename = name match {
      case _ if GzipUtils.isCompressedFilename(name)  => GzipUtils.getUncompressedFilename(name)
      case _ if BZip2Utils.isCompressedFilename(name) => BZip2Utils.getUncompressedFilename(name)
      case _ if XZUtils.isCompressedFilename(name)    => XZUtils.getUncompressedFilename(name)
      case _ => name
    }

    val extension = FilenameUtils.getExtension(filename).toLowerCase(Locale.US)
    values.find(_.toString.equalsIgnoreCase(extension)) match {
      case Some(f) => Right(f)
      case None if extension == "html" =>  Right(Leaflet)
      case _ => Left(extension)
    }
  }
}
