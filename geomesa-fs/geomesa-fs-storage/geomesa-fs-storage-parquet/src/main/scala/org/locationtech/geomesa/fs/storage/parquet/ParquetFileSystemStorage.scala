/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserverFactory.NoOpObserver
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.fs.storage.common.{AbstractFileSystemStorage, FileValidationEnabled}
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage.ParquetFileSystemWriter
import org.locationtech.geomesa.security.{AuthUtils, AuthorizationsProvider, AuthsParam, VisibilityUtils}
import org.locationtech.geomesa.utils.io.CloseQuietly

import java.util.Collections
import scala.util.control.NonFatal

/**
  *
  * @param context file system context
  * @param metadata metadata
  */
class ParquetFileSystemStorage(context: FileSystemContext, metadata: StorageMetadata)
    extends AbstractFileSystemStorage(context, metadata, ParquetFileSystemStorage.FileExtension) {

  private val authProvider: AuthorizationsProvider =
    AuthUtils.getProvider(
      Collections.emptyMap[String, String](),
      context.conf.get(AuthsParam.key, "").split(",").toSeq.filter(_.nonEmpty)
    )

  override protected def createWriter(file: Path, observer: FileSystemObserver): FileSystemWriter =
    new ParquetFileSystemWriter(metadata.sft, context, file, observer)

  override protected def createReader(
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader = {
    // readSft has all the fields needed for filtering and return
    val ReadSchema(readSft, readTransform) = ReadSchema(metadata.sft, filter, transform)
    val ReadFilter(fc, residualFilter) = ReadFilter(readSft, filter)
    val parquetFilter = fc.map(FilterCompat.get).getOrElse(FilterCompat.NOOP)
    val gtFilter = residualFilter.map(FastFilterFactory.optimize(readSft, _))
    val visFilter = VisibilityUtils.visible(Some(authProvider))

    logger.debug(
      s"Parquet filter: ${parquetFilter match { case f: FilterPredicateCompat => f.getFilterPredicate; case f => f }} " +
        s"and modified gt filter: ${gtFilter.getOrElse(Filter.INCLUDE)}")

    // WARNING it is important to create a new conf per query
    // because we communicate the transform SFT set here
    // with the init() method on SimpleFeatureReadSupport via
    // the parquet api. Thus we need to deep copy conf objects
    val conf = new Configuration(context.conf)
    StorageConfiguration.setSft(conf, readSft)

    new ParquetPathReader(conf, readSft, parquetFilter, gtFilter, visFilter, readTransform)
  }
}

object ParquetFileSystemStorage extends LazyLogging {

  val Encoding = "parquet"
  val FileExtension = "parquet"

  val ParquetCompressionOpt = "parquet.compression"

  class ParquetFileSystemWriter(
      sft: SimpleFeatureType,
      context: FileSystemContext,
      file: Path,
      observer: FileSystemObserver = NoOpObserver
    ) extends FileSystemWriter {

    private val conf = {
      val conf = new Configuration(context.conf)
      StorageConfiguration.setSft(conf, sft)
      conf
    }

    private val writer = SimpleFeatureParquetWriter.builder(file, conf).build()

    override def write(f: SimpleFeature): Unit = {
      writer.write(f)
      observer.write(f)
    }
    override def flush(): Unit = observer.flush()
    override def close(): Unit = {
      CloseQuietly(Seq(writer, observer)).foreach(e => throw e)
      PathCache.register(context.fs, file)
      if (FileValidationEnabled.toBoolean.get) {
        validateParquetFile(file)
      }
    }
  }

  /**
   * Validate a file by reading it back
   *
   * @param file file to validate
   */
  def validateParquetFile(file: Path): Unit = {
    var reader: ParquetReader[Group] = null
    try {
      // read Parquet file content
      reader = ParquetReader.builder(new GroupReadSupport(), file).build()
      var record = reader.read()
      while (record != null) {
        // Process the record
        record = reader.read()
      }
      logger.trace(s"$file is a valid Parquet file")
    } catch {
      case NonFatal(e) => throw new RuntimeException(s"Unable to validate $file: File may be corrupted", e)
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }
}
