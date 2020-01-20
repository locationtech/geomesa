/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserverFactory.NoOpObserver
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage._
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  *
  * @param context file system context
  * @param metadata metadata
  */
class ParquetFileSystemStorage(context: FileSystemContext, metadata: StorageMetadata)
    extends AbstractFileSystemStorage(context, metadata, ParquetFileSystemStorage.FileExtension) {

  override protected def createWriter(file: Path, observer: FileSystemObserver): FileSystemWriter = {
    val sftConf = new Configuration(context.conf)
    StorageConfiguration.setSft(sftConf, metadata.sft)
    new ParquetFileSystemWriter(metadata.sft, file, sftConf, observer)
  }

  override protected def createReader(
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader = {
    // readSft has all the fields needed for filtering and return
    val ReadSchema(readSft, readTransform) = ReadSchema(metadata.sft, filter, transform)
    val ReadFilter(fc, residualFilter) = ReadFilter(readSft, filter)
    val parquetFilter = fc.map(FilterCompat.get).getOrElse(FilterCompat.NOOP)
    val gtFilter = residualFilter.map(FastFilterFactory.optimize(readSft, _))

    logger.debug(s"Parquet filter: $parquetFilter and modified gt filter: ${gtFilter.getOrElse(Filter.INCLUDE)}")

    // WARNING it is important to create a new conf per query
    // because we communicate the transform SFT set here
    // with the init() method on SimpleFeatureReadSupport via
    // the parquet api. Thus we need to deep copy conf objects
    val conf = new Configuration(context.conf)
    StorageConfiguration.setSft(conf, readSft)

    new ParquetPathReader(conf, readSft, parquetFilter, gtFilter, readTransform)
  }
}

object ParquetFileSystemStorage {

  val Encoding = "parquet"
  val FileExtension = "parquet"

  val ParquetCompressionOpt = "parquet.compression"

  class ParquetFileSystemWriter(
      sft: SimpleFeatureType,
      file: Path,
      conf: Configuration,
      observer: FileSystemObserver = NoOpObserver
    ) extends FileSystemWriter {

    private val writer = SimpleFeatureParquetWriter.builder(file, conf).build()

    override def write(f: SimpleFeature): Unit = {
      writer.write(f)
      observer.write(f)
    }
    override def flush(): Unit = observer.flush()
    override def close(): Unit = CloseQuietly(Seq(writer, observer)).foreach(e => throw e)
  }
}
