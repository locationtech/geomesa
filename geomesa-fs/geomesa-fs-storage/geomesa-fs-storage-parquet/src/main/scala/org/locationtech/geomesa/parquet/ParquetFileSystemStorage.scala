/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.{FileSystemPathReader, MetadataFileSystemStorage}
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage._
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  *
  * @param conf conf
  * @param metadata metadata
  */
class ParquetFileSystemStorage(conf: Configuration, metadata: FileMetadata)
    extends MetadataFileSystemStorage(conf, metadata) {

  override protected val extension: String = FileExtension

  override protected def createWriter(sft: SimpleFeatureType, file: Path): FileSystemWriter = {
    new FileSystemWriter {
      private val sftConf = new Configuration(conf)
      StorageConfiguration.setSft(sftConf, sft)

      private val writer = SimpleFeatureParquetWriter.builder(file, sftConf).build()

      override def write(f: SimpleFeature): Unit = writer.write(f)
      override def flush(): Unit = {}
      override def close(): Unit = CloseQuietly(writer)
    }
  }

  override protected def createReader(sft: SimpleFeatureType,
                                      filter: Option[Filter],
                                      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader = {
    import scala.collection.JavaConversions._

    val filt = filter.getOrElse(Filter.INCLUDE)

    // parquetSft has all the fields needed for filtering and return
    val parquetSft = transform.map { case (_, tsft) =>
      val transforms = tsft.getAttributeDescriptors.map(_.getLocalName)
      val filters = FilterHelper.propertyNames(filt, sft).filterNot(transforms.contains)
      if (filters.isEmpty) { tsft } else {
        val builder = new SimpleFeatureTypeBuilder()
        builder.init(tsft)
        filters.foreach(f => builder.add(sft.getDescriptor(f)))
        val psft = builder.buildFeatureType()
        psft.getUserData.putAll(sft.getUserData)
        psft
      }
    }.getOrElse(sft)

    // TODO GEOMESA-1954 move this filter conversion higher up in the chain
    val (fc, residualFilter) = new FilterConverter(parquetSft).convert(filt)
    val parquetFilter = fc.map(FilterCompat.get).getOrElse(FilterCompat.NOOP)

    logger.debug(s"Parquet filter: $parquetFilter and modified gt filter: $residualFilter")

    new FilteringReader(conf, parquetSft, parquetFilter, residualFilter, transform)
  }
}

object ParquetFileSystemStorage {

  val ParquetEncoding  = "parquet"
  val FileExtension    = "parquet"

  val ParquetCompressionOpt = "parquet.compression"
}
