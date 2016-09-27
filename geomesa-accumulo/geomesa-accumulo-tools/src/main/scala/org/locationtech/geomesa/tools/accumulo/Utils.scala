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

import org.apache.commons.compress.compressors.bzip2.BZip2Utils
import org.apache.commons.compress.compressors.gzip.GzipUtils
import org.apache.commons.compress.compressors.xz.XZUtils
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Utils {

  object IngestParams {
    val ACCUMULO_INSTANCE   = "geomesa.tools.ingest.instance"
    val ZOOKEEPERS          = "geomesa.tools.ingest.zookeepers"
    val ACCUMULO_MOCK       = "geomesa.tools.ingest.use-mock"
    val ACCUMULO_USER       = "geomesa.tools.ingest.user"
    val ACCUMULO_PASSWORD   = "geomesa.tools.ingest.password"
    val AUTHORIZATIONS      = "geomesa.tools.ingest.authorizations"
    val VISIBILITIES        = "geomesa.tools.ingest.visibilities"
    val SHARDS              = "geomesa.tools.ingest.shards"
    val INDEX_SCHEMA_FMT    = "geomesa.tools.ingest.index-schema-format"
    val FILE_PATH           = "geomesa.tools.ingest.path"
    val FEATURE_NAME        = "geomesa.tools.feature.name"
    val CATALOG_TABLE       = "geomesa.tools.feature.tables.catalog"
    val SFT_SPEC            = "geomesa.tools.feature.sft-spec"
    val IS_TEST_INGEST      = "geomesa.tools.ingest.is-test-ingest"
    val CONVERTER_CONFIG    = "geomesa.tools.ingest.converter-config"
  }

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

}
