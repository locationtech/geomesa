/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.io.FileIO
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.ParquetProperties.WriterVersion
import org.apache.parquet.conf.{ParquetConfiguration, PlainParquetConfiguration}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.io.{LocalOutputFile, OutputFile, PositionOutputStream}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.core.fs.{LocalObjectStore, ObjectStore, S3ObjectStore}
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory.NoOpObserver
import org.locationtech.geomesa.fs.storage.core.parquet.io.ParquetFileSystemWriter.{FileOutput, IcebergOutput, ObjectStoreOutput}
import org.locationtech.geomesa.fs.storage.core.parquet.s3.S3OutputFile
import org.locationtech.geomesa.fs.storage.core.parquet.schema.SimpleFeatureParquetSchema
import org.locationtech.geomesa.utils.io.CloseQuietly

import java.net.URI
import java.nio.file.Path

/**
 * Parquet writer
 *
 * @param sft simple feature type
 * @param conf configuration
 * @param output file to write
 * @param observer any observers
 */
class ParquetFileSystemWriter(
    sft: SimpleFeatureType,
    conf: Map[String, String],
    output: FileOutput,
    observer: FileSystemObserver
  ) extends FileSystemWriter {

  import scala.collection.JavaConverters._

  // TODO consolidate this on FileIO instead of ObjectStore
  def this(sft: SimpleFeatureType, conf: Map[String, String], fs: ObjectStore, file: URI, observer: FileSystemObserver) =
    this(sft, conf, ObjectStoreOutput(fs, file), observer)

  def this(sft: SimpleFeatureType, conf: Map[String, String], fs: ObjectStore, file: URI) =
    this(sft, conf, fs, file, NoOpObserver)

  def this(sft: SimpleFeatureType, conf: Map[String, String], io: FileIO, file: URI, observer: FileSystemObserver) =
    this(sft, conf, IcebergOutput(io, file), observer)

  def this(sft: SimpleFeatureType, conf: Map[String, String], io: FileIO, file: URI) =
    this(sft, conf, io, file, NoOpObserver)

  private val parquetConf = new PlainParquetConfiguration(conf.asJava)
  SimpleFeatureParquetSchema.setSft(parquetConf, sft)

  private val writer = ParquetFileSystemWriter.builder(output.file, parquetConf).build()

  @volatile
  private var closed = false

  override def size: Long = if (closed) { output.size }  else { writer.getDataSize }

  override def write(f: SimpleFeature): Unit = {
    writer.write(f)
    observer(f)
  }

  override def flush(): Unit = observer.flush()

  override def close(): Unit = {
    closed = true
    CloseQuietly(Seq(writer, observer)).foreach(e => throw e)
  }
}

object ParquetFileSystemWriter extends LazyLogging {

  /**
   * Create a new configurable writer
   *
   * @param file file to write
   * @param conf write configuration
   * @return
   */
  def builder(file: OutputFile, conf: ParquetConfiguration): Builder = {
    val version = WriterVersion.fromString(conf.get("parquet.writer.version", WriterVersion.PARQUET_2_0.name()))
    val codec = CompressionCodecName.fromConf(conf.get("parquet.compression", "ZSTD"))
    logger.debug(s"Using Parquet file version $version with compression ${codec.name()}")
    new Builder(file)
      .withConf(conf)
      .withCompressionCodec(codec)
      .withDictionaryEncoding(true)
      .withDictionaryPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
      .withMaxPaddingSize(ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
      .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
      .withValidation(false)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withWriterVersion(version)
      .withRowGroupSize(8L*1024*1024)
  }

  private sealed trait FileOutput {

    /**
     * The output file
     *
     * @return
     */
    def file: OutputFile

    /**
     * Gets the size of the file
     *
     * @return
     */
    def size: Long
  }

  private case class ObjectStoreOutput(fs: ObjectStore, path: URI) extends FileOutput {

    override val file: OutputFile = fs match {
      case _: LocalObjectStore => new LocalOutputFileWithParent(Path.of(path))
      case s3: S3ObjectStore => new S3OutputFile(s3, path)
      case _ => throw new UnsupportedOperationException(s"No file implementation for scheme ${fs.scheme}")
    }

    override def size: Long = fs.size(path)
  }

  private case class IcebergOutput(io: FileIO, path: URI) extends FileOutput {
    override val file: IcebergOutputFile = new IcebergOutputFile(io.newOutputFile(path.toString))
    override def size: Long = file.original.toInputFile.getLength
  }

  class Builder(file: OutputFile) extends ParquetWriter.Builder[SimpleFeature, Builder](file) {
    override def self(): Builder = this
    override protected def getWriteSupport(conf: Configuration): WriteSupport[SimpleFeature] =
      new SimpleFeatureWriteSupport()
    override protected def getWriteSupport(conf: ParquetConfiguration): WriteSupport[SimpleFeature] =
      new SimpleFeatureWriteSupport()
  }

  private class LocalOutputFileWithParent(file: Path) extends LocalOutputFile(file) {
    override def create(blockSize: Long): PositionOutputStream = {
      Option(file.toFile.getParentFile).foreach(_.mkdirs())
      super.create(blockSize)
    }
    override def createOrOverwrite(blockSize: Long): PositionOutputStream = {
      Option(file.toFile.getParentFile).foreach(_.mkdirs())
      super.createOrOverwrite(blockSize)
    }
  }
}
