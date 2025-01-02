/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.exporters

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.api.FileSystemContext
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemWriter
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage.ParquetFileSystemWriter
import org.locationtech.geomesa.utils.io.PathUtils

/**
  * Export to a FileSystem data store format
  */
abstract class FileSystemExporter extends FeatureExporter {

  private var writer: FileSystemWriter = _

  protected def createWriter(sft: SimpleFeatureType): FileSystemWriter

  override def start(sft: SimpleFeatureType): Unit = {
    if (writer != null) {
      writer.close()
    }
    writer = createWriter(sft)
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    require(writer != null, "Must call 'start' before 'export'")
    var i = 0L
    features.foreach { f => writer.write(f); i += 1 }
    Some(i)
  }

  override def close(): Unit = {
    if (writer != null) {
      writer.close()
    }
  }
}

object FileSystemExporter extends LazyLogging {

  class ParquetFileSystemExporter(path: String) extends FileSystemExporter {
    override protected def createWriter(sft: SimpleFeatureType): FileSystemWriter = {
      // use PathUtils.getUrl to handle local files, otherwise default can be in hdfs
      val file = new Path(PathUtils.getUrl(path).toURI)
      val conf = new Configuration()
      try { Class.forName("org.xerial.snappy.Snappy") } catch {
        case _: ClassNotFoundException =>
          logger.warn("SNAPPY compression is not available on the classpath - falling back to GZIP")
          conf.set("parquet.compression", "GZIP")
      }
      new ParquetFileSystemWriter(sft, FileSystemContext(file, conf), file)
    }
  }

  class OrcFileSystemExporter(path: String) extends FileSystemExporter {
    override protected def createWriter(sft: SimpleFeatureType): FileSystemWriter = {
      // use PathUtils.getUrl to handle local files, otherwise default can be in hdfs
      val file = new Path(PathUtils.getUrl(path).toURI)
      new OrcFileSystemWriter(sft, FileSystemContext(file, new Configuration()), file)
    }
  }
}
