/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.features.exporters

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.conf.HadoopParquetConfiguration
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.parquet.io.{ParquetFileSystemWriter, SimpleFeatureParquetSchema}
import org.locationtech.geomesa.utils.io.PathUtils

/**
  * Export to a FileSystem data store format
  */
class GeoParquetExporter(path: String) extends FeatureExporter with LazyLogging {

  private var writer: ParquetFileSystemWriter = _

  override def start(sft: SimpleFeatureType): Unit = {
    if (writer != null) {
      writer.close()
      writer = null
    }
    val conf = new Configuration()
    try { Class.forName("org.xerial.snappy.Snappy") } catch {
      case _: ClassNotFoundException =>
        logger.warn("SNAPPY compression is not available on the classpath - falling back to GZIP")
        conf.set("parquet.compression", "GZIP")
    }
    SimpleFeatureParquetSchema.setSft(conf, sft)
    // use PathUtils.getUrl to handle local files, otherwise default can be in hdfs
    writer = new ParquetFileSystemWriter(PathUtils.getUrl(path).toURI, new HadoopParquetConfiguration(conf))
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    require(writer != null, "Must call 'start' before 'export'")
    var i = 0L
    features.foreach { f => writer.write(f); i += 1 }
    Some(i)
  }

  override def bytes: Long = if (writer == null) { 0 } else { writer.getDataSize }

  override def close(): Unit = {
    if (writer != null) {
      writer.close()
    }
  }
}
