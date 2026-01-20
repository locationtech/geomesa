/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.{FileSystemPathReader, FileSystemWriter}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserverFactory.CompositeObserver
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.{AbstractFileSystemStorage, FileValidationEnabled}
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage.FileValidationObserver
import org.locationtech.geomesa.fs.storage.parquet.io.{ParquetFileSystemReader, ParquetFileSystemWriter, SimpleFeatureParquetSchema}
import org.locationtech.geomesa.security.{AuthProviderParam, AuthUtils, AuthorizationsProvider, AuthsParam, VisibilityUtils}
import org.locationtech.geomesa.utils.io.WithClose

import scala.util.control.NonFatal

/**
 * ParquetFileSystemStorage
 *
 * @param context file system context
 * @param metadata metadata
 */
class ParquetFileSystemStorage(context: FileSystemContext, metadata: StorageMetadata)
    extends AbstractFileSystemStorage(context, metadata, ParquetFileSystemStorage.FileExtension) {

  import scala.collection.JavaConverters._

  private val authProvider: AuthorizationsProvider =
    AuthUtils.getProvider(
      Option(context.conf.get(AuthProviderParam.key)).map(p => AuthProviderParam.key -> p).toMap.asJava,
      context.conf.get(AuthsParam.key, "").split(",").toSeq.filter(_.nonEmpty)
    )

  override protected def createWriter(file: Path, observer: FileSystemObserver): FileSystemWriter = {
    val conf = new Configuration(context.conf)
    SimpleFeatureParquetSchema.setSft(conf, metadata.sft)
    val observers =
      if (FileValidationEnabled.toBoolean.get) {
        CompositeObserver(Seq(observer, FileValidationObserver(file)))
      } else {
        observer
      }
    new ParquetFileSystemWriter(file, conf, observers)
  }

  override protected def createReader(
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader = {
    // readSft has all the fields needed for filtering and return
    val ReadSchema(readSft, readTransform) = ReadSchema(metadata.sft, filter, transform)
    val ReadFilter(fc, residualFilter) = ReadFilter(readSft, filter)
    val parquetFilter = fc.map(FilterCompat.get).getOrElse(FilterCompat.NOOP)
    val gtFilter = residualFilter.map(FastFilterFactory.optimize(readSft, _))
    val visFilter = VisibilityUtils.visible(authProvider)

    logger.debug(
      s"Parquet filter: ${parquetFilter match { case f: FilterPredicateCompat => f.getFilterPredicate; case f => f }} " +
        s"and modified gt filter: ${gtFilter.getOrElse(Filter.INCLUDE)}")

    // WARNING it is important to create a new conf per query
    // because we communicate the transform SFT set here
    // with the init() method on SimpleFeatureReadSupport via
    // the parquet api. Thus we need to deep copy conf objects
    val conf = new Configuration(context.conf)
    StorageConfiguration.setSft(conf, readSft)

    new ParquetFileSystemReader(conf, readSft, parquetFilter, gtFilter, visFilter, readTransform)
  }
}

object ParquetFileSystemStorage extends LazyLogging {

  val Encoding = "parquet"
  val FileExtension = "parquet"

  val ParquetCompressionOpt = "parquet.compression"

  /**
   * Validate a file by reading it back
   *
   * @param file file to validate
   */
  case class FileValidationObserver(file: Path) extends FileSystemObserver {
    override def apply(feature: SimpleFeature): Unit = {}
    override def flush(): Unit = {}
    override def close(): Unit = {
      try {
        WithClose(ParquetReader.builder(new GroupReadSupport(), file).build()) { reader =>
          var record = reader.read()
          while (record != null) {
            // Process the record
            record = reader.read()
          }
          logger.trace(s"$file is a valid Parquet file")
        }
      } catch {
        case NonFatal(e) => throw new RuntimeException(s"File appears to be corrupted: $file", e)
      }
    }
  }
}
