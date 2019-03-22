/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemWriter
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage.ParquetFileSystemWriter
import org.locationtech.geomesa.utils.io.PathUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
      val conf = new Configuration()
      StorageConfiguration.setSft(conf, sft)
      try { Class.forName("org.xerial.snappy.Snappy") } catch {
        case _: ClassNotFoundException =>
          logger.warn("SNAPPY compression is not available on the classpath - falling back to GZIP")
          conf.set("parquet.compression", "GZIP")
      }
      // use PathUtils.getUrl to handle local files, otherwise default can be in hdfs
      new ParquetFileSystemWriter(sft, new Path(PathUtils.getUrl(path).toURI), conf)
    }
  }

  class OrcFileSystemExporter(path: String) extends FileSystemExporter {
    override protected def createWriter(sft: SimpleFeatureType): FileSystemWriter = {
      // use PathUtils.getUrl to handle local files, otherwise default can be in hdfs
      new OrcFileSystemWriter(sft, new Configuration(), new Path(PathUtils.getUrl(path).toURI))
    }
  }
}
