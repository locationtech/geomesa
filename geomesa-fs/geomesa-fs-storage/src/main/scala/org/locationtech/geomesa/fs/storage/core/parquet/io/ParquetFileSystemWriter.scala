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
import org.apache.iceberg.mapping.{MappingUtil, NameMappingParser}
import org.apache.parquet.column.ParquetProperties.WriterVersion
import org.apache.parquet.conf.{ParquetConfiguration, PlainParquetConfiguration}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.io.{LocalOutputFile, OutputFile, PositionOutputStream}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.{FileSystemWriter, ParquetCompressionOpt}
import org.locationtech.geomesa.fs.storage.core.fs.{LocalObjectStore, ObjectStore, S3ObjectStore}
import org.locationtech.geomesa.fs.storage.core.iceberg.SimpleFeatureIcebergSchema
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory.NoOpObserver
import org.locationtech.geomesa.fs.storage.core.parquet.io.ParquetFileSystemWriter.FileOutput
import org.locationtech.geomesa.fs.storage.core.parquet.s3.S3OutputFile
import org.locationtech.geomesa.fs.storage.core.parquet.schema.SimpleFeatureParquetSchema
import org.locationtech.geomesa.fs.storage.core.schema.SimpleFeatureSchema
import org.locationtech.geomesa.utils.io.CloseQuietly

import java.net.URI
import java.nio.file.Path

/**
 * Parquet writer
 *
 * @param conf configuration
 * @param output file to write
 * @param observer any observers
 */
class ParquetFileSystemWriter private (conf: ParquetConfiguration, output: FileOutput, observer: FileSystemObserver)
    extends FileSystemWriter {

  @volatile
  private var closed = false

  private val writer = ParquetFileSystemWriter.builder(output.file, conf).build()

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

  import scala.collection.JavaConverters._

  /**
   * Primary iceberg constructor - when writing to iceberg, needs to use this constructor in order to ensure field ids align
   *
   * @param schema iceberg schema
   * @param conf conf
   * @param io file io
   * @param file file to write
   * @param observer observer
   */
  def apply(
      schema: SimpleFeatureIcebergSchema,
      conf: Map[String, String],
      io: FileIO,
      file: String,
      observer: FileSystemObserver): ParquetFileSystemWriter = {
    // stamp the written parquet files with the table's iceberg field ids (by name) so reads resolve by id
    val nameMapping = Map(SimpleFeatureSchema.IcebergNameMappingKey -> NameMappingParser.toJson(MappingUtil.create(schema.schema)))
    val sft = SimpleFeatureParquetSchema.sftConf(schema.sft)
    apply(conf ++ nameMapping ++ sft, IcebergOutput(io, file), observer)
  }

  /**
   * Secondary constructor - for non-iceberg exports. Note: *will not* align with Iceberg field ids
   *
   * TODO consolidate this on FileIO instead of ObjectStore
   *
   * @param sft simple feature type
   * @param conf configuration options
   * @param fs object store
   * @param file output file path
   * @return
   */
  def apply(sft: SimpleFeatureType, conf: Map[String, String], fs: ObjectStore, file: String): ParquetFileSystemWriter = {
    val sftConf = SimpleFeatureParquetSchema.sftConf(sft)
    apply(conf ++ sftConf, ObjectStoreOutput(fs, URI.create(file)), NoOpObserver)
  }

  /**
   * Constructor - requires the sft to be encoded in the conf
   *
   * @param conf configuration options
   * @param output file output
   * @param observer observer
   * @return
   */
  private def apply(conf: Map[String, String], output: FileOutput, observer: FileSystemObserver): ParquetFileSystemWriter = {
    val compression = Option(System.getProperty(ParquetCompressionOpt)).map(ParquetCompressionOpt -> _).toMap
    val parquetConf = new PlainParquetConfiguration((compression ++ conf).asJava)
    new ParquetFileSystemWriter(parquetConf, output, observer)
  }

  /**
   * Create a new configurable writer
   *
   * @param file file to write
   * @param conf write configuration
   * @return
   */
  private def builder(file: OutputFile, conf: ParquetConfiguration): Builder = {
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

  private case class IcebergOutput(io: FileIO, path: String) extends FileOutput {
    override val file: IcebergOutputFile = new IcebergOutputFile(io.newOutputFile(path))
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
