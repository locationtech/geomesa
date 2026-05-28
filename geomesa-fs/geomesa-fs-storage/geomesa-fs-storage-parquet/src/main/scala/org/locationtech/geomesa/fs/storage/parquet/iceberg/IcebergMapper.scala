/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.iceberg

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg._
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.calrissian.mango.types.{LexiTypeEncoders, TypeEncoder}
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.{StorageFile, XZ2Encoder, Z2Encoder}
import org.locationtech.geomesa.fs.storage.core.schemes.AttributeScheme.{IntegralBucketing, WidthBucketing}
import org.locationtech.geomesa.fs.storage.core.schemes._
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, Partition, PartitionScheme}
import org.locationtech.geomesa.fs.storage.parquet.iceberg.IcebergMapper.SchemeMapper
import org.locationtech.geomesa.fs.storage.parquet.io.geometry.ZValues.ZValueField
import org.locationtech.geomesa.fs.storage.parquet.io.{ParquetFileSystemReader, SimpleFeatureParquetSchema}
import org.locationtech.geomesa.utils.io.WithClose

import java.net.URI
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap

/**
 * Maps geomesa storage to iceberg
 */
class IcebergMapper(storage: FileSystemStorage) extends LazyLogging {

  import scala.collection.JavaConverters._

  private val metricsConfigs = new ConcurrentHashMap[String, MetricsConfig]()

  // need to be in attribute order
  private val mappers = storage.metadata.sft.getAttributeDescriptors.asScala.toSeq.flatMap { d =>
    storage.metadata.schemes.find(_.attribute == d.getLocalName).flatMap { scheme =>
      val mapper = SchemeMapper(scheme, d.getType.getBinding)
      if (mapper.isEmpty) {
        logger.warn(s"Partition scheme '${scheme.name}' is not supported by iceberg and will not be available for query filtering")
      }
      mapper
    }
  }

  val schema: Schema = SimpleFeatureParquetSchema(storage.metadata.sft, storage.context.conf).iceberg

  /**
   * The partition scheme being mapped
   *
   * @return
   */
  val spec: PartitionSpec = mappers.foldLeft(PartitionSpec.builderFor(schema))((b, m) => m.spec(b)).build()

  /**
   * Convert geomesa file metadata to iceberg file metadata
   *
   * @param table iceberg table
   * @param file file
   * @return
   */
  def toDataFile(table: Table, file: StorageFile): DataFile = {
    val uri = storage.context.root.resolve(file.file).toString
    logger.whenDebugEnabled {
      val inputFile = ParquetFileSystemReader.inputFile(storage.fs, new URI(uri))
      val footer = WithClose(inputFile.newStream())(ParquetFileReader.readFooter(inputFile, ParquetReadOptions.builder().build(), _))
      logger.debug(s"Parquet schema for ${file.file}: ${ParquetMetadata.toJSON(footer)}")
    }
    val inputFile = table.io().newInputFile(uri)
    val metrics = ParquetUtil.fileMetrics(inputFile, metricsConfigs.computeIfAbsent(table.name(), _ => MetricsConfig.forTable(table)), null)
    val partitions = partitionValues(file.partition).asJava
    // TODO withSort(f.sort)
    DataFiles.builder(table.spec())
      .withPath(inputFile.location())
      .withFormat(FileFormat.PARQUET)
      .withFileSizeInBytes(inputFile.getLength)
      .withMetrics(metrics)
      .withPartitionValues(partitions)
      .withRecordCount(file.count)
      .build()
  }

  /**
   * Gets the iceberg partition values that correspond to a given partition
   *
   * @param partition partition
   * @return
   */
  def partitionValues(partition: Partition): Seq[String] = {
    mappers.map { m =>
      val key = partition.values.find(_.name == m.scheme.name).getOrElse {
        throw new IllegalArgumentException(
          s"Could not find associated partition: ${m.scheme.name} out of ${partition.values.mkString(", ")}")
      }
      m.toIceberg(key.value)
    }
  }
}

object IcebergMapper {

  /**
   * Maps a partition scheme to iceberg
   */
  private trait SchemeMapper {

    /**
     * The partition scheme being mapped
     *
     * @return
     */
    def scheme: PartitionScheme

    /**
     * Creates the iceberg partition spec
     *
     * @param b spec builder
     * @return
     */
    def spec(b: PartitionSpec.Builder): PartitionSpec.Builder

    /**
     * Gets the iceberg partition value for a given geomesa partition value
     *
     * @param partitionValue geomesa partition value
     * @return iceberg partition value
     */
    def toIceberg(partitionValue: String): String
  }

  private object SchemeMapper {

    /**
     * Maps a partition scheme to iceberg
     *
     * @param scheme the scheme to map
     * @return a mapping, if the scheme is supported by iceberg
     */
    def apply(scheme: PartitionScheme, binding: Class[_]): Option[SchemeMapper] = scheme match {
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.HOURS => Some(HourMapper(s))
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.DAYS => Some(DayMapper(s))
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.MONTHS => Some(MonthMapper(s))
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.YEARS => Some(YearMapper(s))

      case s: Z2Scheme if s.bits % 4 == 0 => Some(Z2Mapper(s))
      case s: XZ2Scheme if s.bits % 4 == 0 => Some(XZ2Mapper(s))

      case s: HashScheme[_] => Some(HashMapper(s))

      case s: AttributeScheme[_] if classOf[String].isAssignableFrom(binding) =>
        s.bucketing match {
          case None => Some(IdentityMapper(s, LexiTypeEncoders.stringEncoder()))
          case Some(w: WidthBucketing) => Some(TruncateMapper(s, LexiTypeEncoders.stringEncoder(), w.max))
        }

      case s: AttributeScheme[_] if classOf[Integer].isAssignableFrom(binding) =>
        s.bucketing match {
          case None => Some(IdentityMapper(s, LexiTypeEncoders.integerEncoder()))
          case Some(i: IntegralBucketing[Int]) => Some(TruncateMapper(s, LexiTypeEncoders.integerEncoder(), i.divisor))
        }

      case s: AttributeScheme[_] if classOf[java.lang.Long].isAssignableFrom(binding) =>
        s.bucketing match {
          case None => Some(IdentityMapper(s, LexiTypeEncoders.longEncoder()))
          case Some(i: IntegralBucketing[Long]) => Some(TruncateMapper(s, LexiTypeEncoders.longEncoder(), i.divisor.toInt))
        }

      case _ => None
    }
  }

  private case class HourMapper(scheme: DateTimeScheme) extends SchemeMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.hour(scheme.attribute)
    override def toIceberg(key: String): String = LexiTypeEncoders.integerEncoder().decode(key).toString
  }

  private case class DayMapper(scheme: DateTimeScheme) extends SchemeMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.day(scheme.attribute)
    // note: days are handled differently from other types, and expect an ISO_LOCAL_DATE formatted string
    override def toIceberg(key: String): String = {
      val days = LexiTypeEncoders.integerEncoder().decode(key)
      DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDate.EPOCH.plusDays(days.longValue()))
    }
  }

  private case class MonthMapper(scheme: DateTimeScheme) extends SchemeMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.month(scheme.attribute)
    override def toIceberg(key: String): String = LexiTypeEncoders.integerEncoder().decode(key).toString
  }

  private case class YearMapper(scheme: DateTimeScheme) extends SchemeMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.year(scheme.attribute)
    override def toIceberg(key: String): String = LexiTypeEncoders.integerEncoder().decode(key).toString
  }

  private case class Z2Mapper(scheme: Z2Scheme) extends SchemeMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder =
      b.truncate(ZValueField.z2(scheme.attribute).zValue, scheme.bits / 4)
    override def toIceberg(partitionValue: String): String = Z2Encoder.encodePartition(partitionValue, scheme.bits)
  }

  private case class XZ2Mapper(scheme: XZ2Scheme) extends SchemeMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder =
      b.truncate(ZValueField.xz2(scheme.attribute).zValue, scheme.bits / 4)
    override def toIceberg(partitionValue: String): String = partitionValue
  }

  private case class HashMapper(scheme: HashScheme[_]) extends SchemeMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.bucket(scheme.attribute, scheme.buckets)
    override def toIceberg(key: String): String = key
  }

  private case class IdentityMapper(scheme: PartitionScheme, lexicoder: TypeEncoder[_, String]) extends SchemeMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.identity(scheme.attribute)
    override def toIceberg(key: String): String = lexicoder.decode(key).toString
  }

  private case class TruncateMapper(scheme: PartitionScheme, lexicoder: TypeEncoder[_, String], width: Int) extends SchemeMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.truncate(scheme.attribute, width)
    override def toIceberg(key: String): String = lexicoder.decode(key).toString
  }
}
