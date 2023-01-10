/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.{AbstractFileSystemStorage, FileValidationEnabled}
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserverFactory.NoOpObserver
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage.ParquetFileSystemWriter
import org.locationtech.geomesa.utils.io.CloseQuietly

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

object ParquetFileSystemStorage extends LazyLogging {

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
    override def close(): Unit = {
      CloseQuietly(Seq(writer, observer)).foreach(e => throw e)
      if (FileValidationEnabled.get.toBoolean) {
        validateParquetFile(file)
      }
    }
  }

  def validateParquetFile(file: Path): Unit = {
    val reader = ParquetReader.builder(new GroupReadSupport(), file).build()

    try {
      // Read Parquet file content
      var record = reader.read()
      while (record != null) {
        // Process the record
        record = reader.read()
      }
      logger.debug(s"${file} is a valid Parquet file")
    } catch {
      case e: Exception => throw new RuntimeException(s"Unable to validate ${file}: File may be corrupted", e)
    } finally {
      reader.close()
    }
  }
}
