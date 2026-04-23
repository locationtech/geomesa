/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

import com.typesafe.scalalogging.LazyLogging
import org.apache.parquet.conf.PlainParquetConfiguration
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.io.{InputFile, LocalInputFile}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemContext
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.fs.storage.core.fs.{LocalObjectStore, ObjectStore, S3ObjectStore}
import org.locationtech.geomesa.fs.storage.parquet.io.s3.S3InputFile
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.Transform.Transforms

import java.net.URI
import java.nio.file.Path
import scala.annotation.tailrec
import scala.util.control.NonFatal

class ParquetFileSystemReader(
    fs: ObjectStore,
    context: FileSystemContext,
    readSft: SimpleFeatureType,
    parquetFilter: FilterCompat.Filter,
    gtFilter: Option[org.geotools.api.filter.Filter],
    visFilter: SimpleFeature => Boolean,
    transform: Option[(String, SimpleFeatureType)]
  ) extends FileSystemPathReader with LazyLogging {

  import scala.collection.JavaConverters._

  private val gtf = gtFilter.orNull

  private val transformFeature: SimpleFeature => SimpleFeature = transform match {
    case None => null
    case Some((tdefs, tsft)) =>
      val definitions = Transforms(readSft, tdefs).toArray
      f => new TransformSimpleFeature(tsft, definitions, f)
  }

  private val conf = new PlainParquetConfiguration(context.conf.asJava)
  SimpleFeatureParquetSchema.setSft(conf, readSft)

  override def root: URI = context.root

  override def read(file: URI): CloseableIterator[SimpleFeature] = {
    // TODO we can examine file bounds and simplify/eliminate the gtFilter
    logger.debug(s"Opening reader for path $file")
    new ParquetFileIterator(file)
  }

  private class ParquetFileIterator(path: URI) extends CloseableIterator[SimpleFeature] {

    private val reader: ParquetReader[SimpleFeature] =
      ParquetFileSystemReader.builder(fs, path).withConf(conf).withFilter(parquetFilter).build()

    private var staged: SimpleFeature = _

    override def close(): Unit = {
      logger.debug(s"Closing reader for path $path")
      reader.close()
    }

    override def next(): SimpleFeature = {
      val res = staged
      staged = null
      res
    }

    @tailrec
    override final def hasNext: Boolean = {
      if (staged != null) { true } else {
        val read = try { reader.read() } catch {
          case NonFatal(e) => logger.error(s"Error reading file '$path'", e); null
        }
        if (read == null) {
          false
        } else if (visFilter(read) && (gtf == null || gtf.evaluate(read))) {
          staged = if (transformFeature == null) { read } else { transformFeature(read) }
          true
        } else {
          hasNext
        }
      }
    }
  }
}

object ParquetFileSystemReader {

  def builder(fs: ObjectStore, path: URI): Builder = {
    val file = fs match {
      case _: LocalObjectStore => new LocalInputFile(Path.of(path))
      case s3: S3ObjectStore   => new S3InputFile(s3, path)
      case _ => throw new UnsupportedOperationException(s"No file implementation for scheme ${fs.scheme}")
    }
    new Builder(file)
  }

  class Builder(file: InputFile) extends ParquetReader.Builder[SimpleFeature](file) {
    override protected def getReadSupport: ReadSupport[SimpleFeature] = new SimpleFeatureReadSupport()
  }
}
