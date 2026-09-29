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
import org.apache.iceberg.TableProperties
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.mapping.{MappingUtil, NameMappingParser}
import org.apache.parquet.column.ParquetProperties.WriterVersion
import org.apache.parquet.conf.{ParquetConfiguration, PlainParquetConfiguration}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetOutputFormat, ParquetWriter}
import org.apache.parquet.io.{LocalOutputFile, OutputFile, PositionOutputStream}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.{FileSystemWriter, ParquetCompressionOpt, ParquetRowGroupSizeOpt}
import org.locationtech.geomesa.fs.storage.core.fs.{LocalObjectStore, ObjectStore, S3ObjectStore}
import org.locationtech.geomesa.fs.storage.core.iceberg.SimpleFeatureIcebergSchema
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserver
import org.locationtech.geomesa.fs.storage.core.observer.FileSystemObserverFactory.NoOpObserver
import org.locationtech.geomesa.fs.storage.core.parquet.io.ParquetFileSystemWriter.FileOutput
import org.locationtech.geomesa.fs.storage.core.parquet.s3.S3OutputFile
import org.locationtech.geomesa.fs.storage.core.parquet.schema.SimpleFeatureParquetSchema
import org.locationtech.geomesa.fs.storage.core.schema.SimpleFeatureSchema
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.locationtech.geomesa.utils.text.Suffixes

import java.net.URI
import java.nio.file.Path
import java.util.Locale
import scala.util.Try

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
    // system properties provide defaults, which the storage configuration overrides
    val sysProps = Seq(ParquetCompressionOpt, ParquetRowGroupSizeOpt).flatMap(k => Option(System.getProperty(k)).map(k -> _)).toMap
    val parquetConf = new PlainParquetConfiguration((sysProps ++ conf).asJava)
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
    val rowGroupSize = this.rowGroupSize(conf)
    logger.debug(s"Using Parquet file version $version with compression ${codec.name()} and row group size $rowGroupSize")

    val builder =
      new Builder(file)
        .withConf(conf)
        .withCompressionCodec(codec)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withWriterVersion(version)
        .withRowGroupSize(rowGroupSize)
    configureBloomFilters(builder, conf)
  }

  /**
   * Applies any bloom filter options to the writer. The builder does not read these from its configuration
   * (only `ParquetOutputFormat` does), so they have to be passed through explicitly. Options use the standard
   * parquet keys, either globally (e.g. `parquet.bloom.filter.enabled`) or per column, by appending
   * `#<column path>` (e.g. `parquet.bloom.filter.enabled#name`)
   *
   * @param builder writer builder
   * @param conf write configuration
   * @return the builder
   */
  private[io] def configureBloomFilters(builder: Builder, conf: ParquetConfiguration): Builder = {
    import ParquetOutputFormat._

    def option[T](key: String, value: String)(parse: String => Option[T])(apply: T => Unit): Unit = {
      parse(value.trim) match {
        case Some(v) => apply(v)
        case None => logger.warn(s"Ignoring invalid value for $key: '$value'")
      }
    }
    def bool(v: String): Option[Boolean] = v.toLowerCase(Locale.US) match {
      case "true" => Some(true)
      case "false" => Some(false)
      case _ => None
    }
    def long(v: String): Option[Long] = Try(v.toLong).toOption.filter(_ > 0)
    def fpp(v: String): Option[Double] = Try(v.toDouble).toOption.filter(d => d > 0 && d < 1)
    def bytes(v: String): Option[Int] = Suffixes.Memory.bytes(v).toOption.filter(b => b > 0 && b <= Int.MaxValue).map(_.toInt)

    conf.iterator().asScala.foreach { entry =>
      val key = entry.getKey
      if (key.startsWith("parquet.bloom.filter.")) {
        val value = entry.getValue
        key.split("#", 2) match {
          case Array(BLOOM_FILTER_ENABLED)                     => option(key, value)(bool)(builder.withBloomFilterEnabled(_))
          case Array(BLOOM_FILTER_ENABLED, col)                => option(key, value)(bool)(builder.withBloomFilterEnabled(col, _))
          case Array(BLOOM_FILTER_EXPECTED_NDV, col)           => option(key, value)(long)(builder.withBloomFilterNDV(col, _))
          case Array(BLOOM_FILTER_FPP, col)                    => option(key, value)(fpp)(builder.withBloomFilterFPP(col, _))
          case Array(BLOOM_FILTER_CANDIDATES_NUMBER, col)      => option(key, value)(long)(n => builder.withBloomFilterCandidateNumber(col, n.toInt))
          case Array(BLOOM_FILTER_MAX_BYTES)                   => option(key, value)(bytes)(builder.withMaxBloomFilterBytes(_))
          case Array(ADAPTIVE_BLOOM_FILTER_ENABLED)            => option(key, value)(bool)(builder.withAdaptiveBloomFilterEnabled(_))
          case _ => logger.warn(s"Ignoring unrecognized bloom filter option: $key")
        }
      }
    }
    builder
  }

  /**
   * Translates the parquet bloom filter properties of an iceberg table (as set by e.g. Trino's
   * `parquet_bloom_filter_columns`, and honored by other iceberg writers such as compaction) into the
   * equivalent parquet write options
   *
   * @param properties iceberg table properties
   * @return parquet write options
   */
  def icebergBloomFilterConf(properties: java.util.Map[String, String]): Map[String, String] = {
    import ParquetOutputFormat._
    import TableProperties._

    val prefixes = Seq(
      PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX -> BLOOM_FILTER_ENABLED,
      PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX     -> BLOOM_FILTER_EXPECTED_NDV,
      PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX     -> BLOOM_FILTER_FPP,
    )
    properties.asScala.toMap.flatMap {
      case (PARQUET_BLOOM_FILTER_MAX_BYTES, v) => Some(BLOOM_FILTER_MAX_BYTES -> v)
      case (k, v) =>
        prefixes.collectFirst { case (prefix, key) if k.startsWith(prefix) && k.length > prefix.length =>
          s"$key#${k.substring(prefix.length)}" -> v
        }
    }
  }

  /**
   * The configured row group size, or parquet's default (`ParquetWriter.DEFAULT_BLOCK_SIZE`) if absent or invalid
   *
   * @param conf write configuration
   * @return row group size, in bytes
   */
  private[io] def rowGroupSize(conf: ParquetConfiguration): Long = {
    Option(conf.get(ParquetRowGroupSizeOpt)).map(_.trim).filter(_.nonEmpty) match {
      case None => ParquetWriter.DEFAULT_BLOCK_SIZE
      case Some(size) =>
        Suffixes.Memory.bytes(size).getOrElse {
          logger.warn(s"Invalid $ParquetRowGroupSizeOpt '$size', using the default of ${ParquetWriter.DEFAULT_BLOCK_SIZE} bytes")
          ParquetWriter.DEFAULT_BLOCK_SIZE
        }
    }
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
