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
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.core.fs.{LocalObjectStore, ObjectStore, S3ObjectStore}
import org.locationtech.geomesa.fs.storage.parquet.io.ParquetFileSystemWriter
import org.locationtech.geomesa.utils.io.{CloseWithLogging, PathUtils}

/**
  * Export to a FileSystem data store format
  */
class GeoParquetExporter(path: String) extends FeatureExporter with LazyLogging {

  import scala.collection.JavaConverters._

  private var writer: ParquetFileSystemWriter = _
  private var fs: ObjectStore = _

  override def start(sft: SimpleFeatureType): Unit = {
    if (writer != null) {
      writer.close()
      writer = null
    }
    // TODO make it easier to configure this in a manner consistent with the file system data store
    val hadoopConf = new Configuration()
    try { Class.forName("org.xerial.snappy.Snappy") } catch {
      case _: ClassNotFoundException =>
        logger.warn("SNAPPY compression is not available on the classpath - falling back to GZIP")
        hadoopConf.set("parquet.compression", "GZIP")
    }
    val conf = hadoopConf.iterator().asScala.map(e => e.getKey -> e.getValue).toMap

    // use PathUtils.getUrl to handle local file paths (without a scheme)
    val uri = PathUtils.getUrl(path).toURI
    fs = uri.getScheme match {
      case "file" => LocalObjectStore
      case "s3" | "s3a" => S3ObjectStore(conf)
      case s => throw new UnsupportedOperationException(s"No implementation available for writing to scheme: $s")
    }

    writer = new ParquetFileSystemWriter(fs, conf, sft, uri)
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    require(writer != null, "Must call 'start' before 'export'")
    var i = 0L
    features.foreach { f => writer.write(f); i += 1 }
    Some(i)
  }

  override def bytes: Long = if (writer == null) { 0 } else { writer.getDataSize }

  override def close(): Unit = CloseWithLogging(Seq(writer, fs).filter(_ != null))
}
