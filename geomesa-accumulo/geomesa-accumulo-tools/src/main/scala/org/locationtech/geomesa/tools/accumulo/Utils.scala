/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo

import java.io.File
import java.util.Locale

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.compress.compressors.bzip2.BZip2Utils
import org.apache.commons.compress.compressors.gzip.GzipUtils
import org.apache.commons.compress.compressors.xz.XZUtils
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.data.Query

import scala.collection.JavaConverters._

object Utils extends LazyLogging {

  object Formats extends Enumeration {
    type Formats = Value
    val CSV     = Value("csv")
    val TSV     = Value("tsv")
    val SHP     = Value("shp")
    val JSON    = Value("json")
    val GeoJson = Value("geojson")
    val GML     = Value("gml")
    val BIN     = Value("bin")
    val AVRO    = Value("avro")
    val XML     = Value("xml")
    val NULL    = Value("null")
    val Other   = Value("other")

    def getFileExtension(name: String): String = {
      val filename = name match {
        case _ if GzipUtils.isCompressedFilename(name)  => GzipUtils.getUncompressedFilename(name)
        case _ if BZip2Utils.isCompressedFilename(name) => BZip2Utils.getUncompressedFilename(name)
        case _ if XZUtils.isCompressedFilename(name)    => XZUtils.getUncompressedFilename(name)
        case _ => name
      }

      FilenameUtils.getExtension(filename).toLowerCase(Locale.US)
    }
  }

  object Modes {
    val Local = "local"
    val Hdfs = "hdfs"

    def getJobMode(filename: String) = if (filename.toLowerCase.trim.startsWith("hdfs://")) Hdfs else Local
    def getModeFlag(filename: String) = "--" + getJobMode(filename)
  }

  // Recursively delete a local directory and its children
  def deleteLocalDirectory(pathStr: String): Unit = FileUtils.deleteDirectory(new File(pathStr))

  // Recursively delete a HDFS directory and its children
  def deleteHdfsDirectory(pathStr: String) {
    val fs = FileSystem.get(new Configuration)
    val path = new Path(pathStr)
    fs.delete(path, true)
  }

  // If there are override attributes given as an arg or via command line params
  // split attributes by "," meanwhile allowing to escape it by "\,".
  def setOverrideAttributes(q: Query, overrideAttributes: Option[java.util.List[String]] = None) = {
    for ( list <- overrideAttributes;
        attributes: String <- asScalaBufferConverter(list).asScala.toSeq ){
      val splitAttrs = attributes.split("""(?<!\\),""").map(_.trim.replace("\\,", ","))
      logger.debug("Attributes used for query transform: " + splitAttrs.mkString("|"))
      q.setPropertyNames(splitAttrs)
    }
  }

}
