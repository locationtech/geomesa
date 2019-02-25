/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.{FileSystemPathReader, MetadataObservingFileSystemWriter, WriterCallback}
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
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

  override protected def createWriter(file: Path, cb: WriterCallback): FileSystemWriter = {
    val sftConf = new Configuration(context.conf)
    StorageConfiguration.setSft(sftConf, metadata.sft)
    new ParquetFileSystemWriter(metadata.sft, file, sftConf) with MetadataObservingFileSystemWriter {
      override def callback: WriterCallback = cb
    }
  }

  override protected def createReader(
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader = {
    import scala.collection.JavaConversions._

    val filt = filter.getOrElse(Filter.INCLUDE)

    // parquetSft has all the fields needed for filtering and return
    val parquetSft = transform.map { case (_, tsft) =>
      val transforms = tsft.getAttributeDescriptors.map(_.getLocalName)
      val filters = FilterHelper.propertyNames(filt, metadata.sft).filterNot(transforms.contains)
      if (filters.isEmpty) { tsft } else {
        val builder = new SimpleFeatureTypeBuilder()
        builder.init(tsft)
        filters.foreach(f => builder.add(metadata.sft.getDescriptor(f)))
        val psft = builder.buildFeatureType()
        psft.getUserData.putAll(metadata.sft.getUserData)
        psft
      }
    }.getOrElse(metadata.sft)

    // TODO GEOMESA-1954 move this filter conversion higher up in the chain
    val (fc, residualFilter) = new FilterConverter(parquetSft).convert(filt)
    val parquetFilter = fc.map(FilterCompat.get).getOrElse(FilterCompat.NOOP)
    val gtFilter = FastFilterFactory.optimize(parquetSft, residualFilter)

    logger.debug(s"Parquet filter: $parquetFilter and modified gt filter: $gtFilter")

    new FilteringReader(context.conf, parquetSft, parquetFilter, gtFilter, transform)
  }
}

object ParquetFileSystemStorage {

  val Encoding  = "parquet"
  val FileExtension    = "parquet"

  val ParquetCompressionOpt = "parquet.compression"

  class ParquetFileSystemWriter(sft: SimpleFeatureType, file: Path, conf: Configuration) extends FileSystemWriter {

    private val writer = SimpleFeatureParquetWriter.builder(file, conf).build()

    override def write(f: SimpleFeature): Unit = writer.write(f)
    override def flush(): Unit = {}
    override def close(): Unit = CloseQuietly(writer)
  }
}
