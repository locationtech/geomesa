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
import org.geotools.process.vector.TransformProcess
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
import org.opengis.filter.expression.{Expression, PropertyName}

/**
  *
  * @param context file system context
  * @param metadata metadata
  */
class ParquetFileSystemStorage(context: FileSystemContext, metadata: StorageMetadata)
    extends AbstractFileSystemStorage(context, metadata, ParquetFileSystemStorage.FileExtension) {

  import scala.collection.JavaConverters._

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

    val filt = filter.getOrElse(Filter.INCLUDE)

    // readSft has all the fields needed for filtering and return
    val (readSft, readTransform) = transform match {
      case None => (metadata.sft, None)

      case Some((tdefs, _)) =>
        // if we have to return a different feature than we read, we need to apply a secondary transform
        // otherwise, we can just do the transform on read and skip the secondary transform
        var secondary = false

        // note: update `secondary` in our .flatMap to avoid a second pass over the expressions
        val transformCols = TransformProcess.toDefinition(tdefs).asScala.map(_.expression).flatMap {
          case p: PropertyName => Seq(p.getPropertyName)
          case e: Expression   => secondary = true; FilterHelper.propertyNames(e, metadata.sft)
        }.distinct
        val filterCols = FilterHelper.propertyNames(filt, metadata.sft).filterNot(transformCols.contains)
        secondary = secondary || filterCols.nonEmpty

        val projectedSft = {
          val builder = new SimpleFeatureTypeBuilder()
          builder.setName(metadata.sft.getName)
          transformCols.foreach(a => builder.add(metadata.sft.getDescriptor(a)))
          filterCols.foreach(a => builder.add(metadata.sft.getDescriptor(a)))
          builder.buildFeatureType()
        }
        projectedSft.getUserData.putAll(metadata.sft.getUserData)

        (projectedSft, if (secondary) { transform } else { None })
    }

    val (fc, residualFilter) = FilterConverter.convert(readSft, filt)
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

  class ParquetFileSystemWriter(sft: SimpleFeatureType, file: Path, conf: Configuration) extends FileSystemWriter {

    private val writer = SimpleFeatureParquetWriter.builder(file, conf).build()

    override def write(f: SimpleFeature): Unit = writer.write(f)
    override def flush(): Unit = {}
    override def close(): Unit = CloseQuietly(writer)
  }
}
