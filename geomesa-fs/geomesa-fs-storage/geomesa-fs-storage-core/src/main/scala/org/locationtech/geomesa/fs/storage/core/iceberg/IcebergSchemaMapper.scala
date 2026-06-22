/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg._
import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.parquet.ParquetUtil
import org.calrissian.mango.types.LexiTypeEncoders
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.iceberg.IcebergSchemaMapper.PartitionMapper
import org.locationtech.geomesa.fs.storage.core.parquet.schema.SimpleFeatureParquetSchema
import org.locationtech.geomesa.fs.storage.core.schemes.AttributeScheme.{IntegralBucketing, WidthBucketing}
import org.locationtech.geomesa.fs.storage.core.schemes.SpatialScheme.ZValueField
import org.locationtech.geomesa.fs.storage.core.schemes._
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, Partition, PartitionKey, PartitionScheme}
import org.locationtech.geomesa.utils.text.DateParsing

import java.net.URI
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap

/**
 * Maps geomesa storage to iceberg
 */
case class IcebergSchemaMapper(sft: SimpleFeatureType, schemes: Seq[PartitionScheme], context: FileSystemContext)
    extends LazyLogging {

  import scala.collection.JavaConverters._

  private val metricsConfigs = new ConcurrentHashMap[String, MetricsConfig]()

  private val mappers = schemes.map { scheme =>
    PartitionMapper(scheme, sft.getDescriptor(scheme.attribute).getType.getBinding) match {
      case Right(m) => m
        // TODO list supported schemes? link to iceberg docs?
      case Left(m) => throw new UnsupportedOperationException(m)
    }
  }

  val parquetSchema: SimpleFeatureParquetSchema = SimpleFeatureParquetSchema(sft, context.conf)

  val schema: Schema = parquetSchema.iceberg

  /**
   * The partition scheme being mapped
   *
   * @return
   */
  val spec: PartitionSpec = mappers.foldLeft(PartitionSpec.builderFor(schema))((b, m) => m.spec(b)).build()

  // TODO create a partition struct directly?
  def partitionValues(partition: Partition): Seq[String] = {
    mappers.map { m =>
      val key = partition.values.find(_.name == m.scheme.name).getOrElse {
        throw new IllegalArgumentException(
          s"Could not find associated partition: ${m.scheme.name} out of ${partition.values.mkString(", ")}")
      }
      m.toIceberg(key.value)
    }
  }

  /**
   * Create a DataFile from a file path and partition
   *
   * @param table iceberg table
   * @param filePath file path relative to root
   * @param partition partition
   * @param content file content type (DATA, EQUALITY_DELETES, POSITION_DELETES)
   * @return
   */
  def createDataFile(
      table: Table,
      filePath: String,
      partition: Partition,
      content: org.apache.iceberg.FileContent = org.apache.iceberg.FileContent.DATA): DataFile = {
    val uri = context.root.resolve(filePath).toString
    val inputFile = table.io().newInputFile(uri)
    val metrics = ParquetUtil.fileMetrics(inputFile, metricsConfigs.computeIfAbsent(table.name(), _ => MetricsConfig.forTable(table)), null)
    val partitionValues = mappers.map { m =>
      val key = partition.values.find(_.name == m.scheme.name).getOrElse {
        throw new IllegalArgumentException(
          s"Could not find associated partition: ${m.scheme.name} out of ${partition.values.mkString(", ")}")
      }
      m.toIceberg(key.value)
    }
    // TODO withSort(f.sort)
    // Note: Iceberg determines content type by builder - DataFiles for DATA, DeleteFiles for deletes
    // For now, we create DATA files and rely on filename/metadata for delete semantics
    // TODO: Refactor to use DeleteFiles.builder() for true EQUALITY_DELETES
    DataFiles.builder(table.spec())
      .withPath(inputFile.location())
      .withFormat(FileFormat.PARQUET)
      .withFileSizeInBytes(inputFile.getLength)
      .withMetrics(metrics)
      .withPartitionValues(partitionValues.asJava)
      //TODO ?.withRecordCount(file.count)
      .build()
  }

  /**
   * Extract partition information from a DataFile
   *
   * @param file data file
   * @return
   */
  def partition(file: DataFile): Partition = {
    val partitions = mappers.zipWithIndex.map { case (m, i) => PartitionKey(m.scheme.name, m.fromIceberg(file.partition(), i)) }
    Partition(partitions.toSet)
  }

  /**
   * Get the relative file path from a DataFile
   *
   * @param file data file
   * @return
   */
  def filePath(file: DataFile): String = {
    context.root.relativize(URI.create(file.location())).toString
  }

  def expression(partition: Partition): Expression = {
    val clauses = partition.values.toSeq.map { key =>
      val mapper = mappers.find(_.scheme.name == key.name).getOrElse {
        throw new IllegalArgumentException(
          s"Could not find associated partition: ${key.name} out of ${partition.values.mkString(", ")}")
      }
      mapper.expression(key.value)
    }
    clauses.reduce(Expressions.and)
  }
}

object IcebergSchemaMapper {

  /**
   * Maps a partition scheme to iceberg
   */
  private trait PartitionMapper {

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
     * @param key geomesa partition value
     * @return iceberg partition value
     */
    def toIceberg(key: String): String

    /**
     * Gets the geomesa partition value for a given iceberg partition value
     *
     * @param partition iceberg partition struct
     * @param i offest into the partition struct
     * @return geomesa partition value
     */
    def fromIceberg(partition: StructLike, i: Int): String

    /**
     * Get an iceberg filter that covers a given partition
     *
     * @param key partition value
     * @return
     */
    def expression(key: String): Expression
  }

  private object PartitionMapper {

    /**
     * Maps a partition scheme to iceberg
     *
     * @param scheme the scheme to map
     * @return a mapping, if the scheme is supported by iceberg
     */
    def apply(scheme: PartitionScheme, binding: Class[_]): Either[String, PartitionMapper] = scheme match {
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.HOURS => Right(HourMapper(s))
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.DAYS => Right(DayMapper(s))
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.MONTHS => Right(MonthMapper(s))
      case s: DateTimeScheme if s.step == 1 && s.unit == ChronoUnit.YEARS => Right(YearMapper(s))

      case s: DateTimeScheme if s.unit == ChronoUnit.WEEKS => Left(s"Iceberg does not support week-based partitioning: ${s.name}")
      case s: DateTimeScheme if s.step != 1                => Left(s"Iceberg does not support date partitioning step-units other than 1: ${s.name}")

      case s: Z2Scheme if s.bits % 4 == 0  => Right(Z2Mapper(s))
      case s: XZ2Scheme if s.bits % 4 == 0 => Right(XZ2Mapper(s))

      case s: Z2Scheme  => Left(s"Iceberg spatial bit partitioning must be a multiple of 4: ${s.name}")
      case s: XZ2Scheme => Left(s"Iceberg spatial bit partitioning must be a multiple of 4: ${s.name}")

      case s: HashScheme[_] => Right(HashMapper(s))

      case s: AttributeScheme[_] if classOf[String].isAssignableFrom(binding) =>
        s.bucketing match {
          case None => Right(IdentityStringMapper(s))
          case Some(w: WidthBucketing) => Right(TruncateStringMapper(s, w.max))
        }

      case s: AttributeScheme[_] if classOf[Integer].isAssignableFrom(binding) =>
        s.bucketing match {
          case None => Right(IdentityIntMapper(s))
          case Some(i: IntegralBucketing[Int]) => Right(TruncateIntMapper(s, i.divisor))
        }

      case s: AttributeScheme[_] if classOf[java.lang.Long].isAssignableFrom(binding) =>
        s.bucketing match {
          case None => Right(IdentityLongMapper(s))
          case Some(i: IntegralBucketing[Long]) => Right(TruncateLongMapper(s, i.divisor.toInt))
        }

      case s: AttributeScheme[_] =>
        Left(s"Iceberg does not support partitioning for attributes of type ${binding.getName}: ${s.name}")

      case s => Left(s"Iceberg mapping not implemented for scheme: ${s.name}")
    }
  }

  private case class HourMapper(scheme: DateTimeScheme) extends PartitionMapper {
    private val lexicoder = LexiTypeEncoders.integerEncoder()
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.hour(scheme.attribute)
    override def toIceberg(key: String): String = lexicoder.decode(key).toString
    override def fromIceberg(partition: StructLike, i: Int): String = lexicoder.encode(partition.get(i, classOf[Integer]))
    override def expression(key: String): Expression = Expressions.equal(Expressions.hour[Integer](scheme.attribute), lexicoder.decode(key))
  }

  private case class DayMapper(scheme: DateTimeScheme) extends PartitionMapper {
    private val lexicoder = LexiTypeEncoders.integerEncoder()
    private val dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.day(scheme.attribute)
    // note: days are handled differently from other types, and expect an ISO_LOCAL_DATE formatted string
    override def toIceberg(key: String): String =
      dateTimeFormatter.format(LocalDate.EPOCH.plusDays(lexicoder.decode(key).longValue()))
    override def fromIceberg(partition: StructLike, i: Int): String = {
      val date = DateParsing.parse(partition.get(i, classOf[String]), dateTimeFormatter)
      lexicoder.encode(ChronoUnit.DAYS.between(DateTimeScheme.Epoch, date).toInt)
    }
    override def expression(key: String): Expression =
      Expressions.equal(Expressions.day[Integer](scheme.attribute), lexicoder.decode(key))
  }

  private case class MonthMapper(scheme: DateTimeScheme) extends PartitionMapper {
    private val lexicoder = LexiTypeEncoders.integerEncoder()
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.month(scheme.attribute)
    override def toIceberg(key: String): String = lexicoder.decode(key).toString
    override def fromIceberg(partition: StructLike, i: Int): String = lexicoder.encode(partition.get(i, classOf[Integer]))
    override def expression(key: String): Expression =
      Expressions.equal(Expressions.month[Integer](scheme.attribute), lexicoder.decode(key))
  }

  private case class YearMapper(scheme: DateTimeScheme) extends PartitionMapper {
    private val lexicoder = LexiTypeEncoders.integerEncoder()
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.year(scheme.attribute)
    override def toIceberg(key: String): String = lexicoder.decode(key).toString
    override def fromIceberg(partition: StructLike, i: Int): String = lexicoder.encode(partition.get(i, classOf[Integer]))
    override def expression(key: String): Expression =
      Expressions.equal(Expressions.year[Integer](scheme.attribute), lexicoder.decode(key))
  }

  private case class Z2Mapper(scheme: Z2Scheme) extends PartitionMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder =
      b.truncate(ZValueField.z2(scheme.attribute).zValue, scheme.bits / 4)
    override def toIceberg(key: String): String = key
    override def fromIceberg(partition: StructLike, i: Int): String = partition.get(i, classOf[String])
    override def expression(key: String): Expression =
      Expressions.equal[String](Expressions.truncate[String](ZValueField.z2(scheme.attribute).zValue, scheme.digits), key)
  }

  private case class XZ2Mapper(scheme: XZ2Scheme) extends PartitionMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder =
      b.truncate(ZValueField.xz2(scheme.attribute).zValue, scheme.bits / 4)
    override def toIceberg(key: String): String = key
    override def fromIceberg(partition: StructLike, i: Int): String = partition.get(i, classOf[String])
    override def expression(key: String): Expression =
      Expressions.equal[String](Expressions.truncate[String](ZValueField.xz2(scheme.attribute).zValue, scheme.digits), key)
  }

  private case class HashMapper(scheme: HashScheme[_]) extends PartitionMapper {
    private val format = s"%0${(scheme.buckets - 1).toString.length}d"
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.bucket(scheme.attribute, scheme.buckets)
    override def toIceberg(key: String): String = key
    override def fromIceberg(partition: StructLike, i: Int): String = format.format(partition.get(i, classOf[Integer]))
    override def expression(key: String): Expression =
      Expressions.equal(Expressions.bucket[Integer](scheme.attribute, scheme.buckets), Integer.valueOf(key))
  }

  private case class IdentityStringMapper(scheme: PartitionScheme) extends PartitionMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.identity(scheme.attribute)
    override def toIceberg(key: String): String = key
    override def fromIceberg(partition: StructLike, i: Int): String = partition.get(i, classOf[String])
    override def expression(key: String): Expression = Expressions.equal[String](scheme.attribute, key)
  }

  private case class IdentityIntMapper(scheme: PartitionScheme) extends PartitionMapper {
    private val lexicoder = LexiTypeEncoders.integerEncoder()
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.identity(scheme.attribute)
    override def toIceberg(key: String): String = lexicoder.decode(key).toString
    override def fromIceberg(partition: StructLike, i: Int): String = lexicoder.encode(partition.get(i, classOf[Integer]))
    override def expression(key: String): Expression = Expressions.equal[Integer](scheme.attribute, lexicoder.decode(key))
  }

  private case class IdentityLongMapper(scheme: PartitionScheme) extends PartitionMapper {
    private val lexicoder = LexiTypeEncoders.longEncoder()
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.identity(scheme.attribute)
    override def toIceberg(key: String): String = lexicoder.decode(key).toString
    override def fromIceberg(partition: StructLike, i: Int): String = lexicoder.encode(partition.get(i, classOf[java.lang.Long]))
    override def expression(key: String): Expression = Expressions.equal[java.lang.Long](scheme.attribute, lexicoder.decode(key))
  }

  private case class TruncateStringMapper(scheme: PartitionScheme, width: Int) extends PartitionMapper {
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.truncate(scheme.attribute, width)
    override def toIceberg(key: String): String = key
    override def fromIceberg(partition: StructLike, i: Int): String = partition.get(i, classOf[String])
    override def expression(key: String): Expression =
      Expressions.equal(Expressions.truncate[String](scheme.attribute, width), key)
  }

  private case class TruncateIntMapper(scheme: PartitionScheme, divisor: Int) extends PartitionMapper {
    private val lexicoder = LexiTypeEncoders.integerEncoder()
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.truncate(scheme.attribute, divisor)
    override def toIceberg(key: String): String = lexicoder.decode(key).toString
    override def fromIceberg(partition: StructLike, i: Int): String = lexicoder.encode(partition.get(i, classOf[Integer]))
    override def expression(key: String): Expression =
      Expressions.equal(Expressions.truncate[Integer](scheme.attribute, divisor), lexicoder.decode(key))
  }

  private case class TruncateLongMapper(scheme: PartitionScheme, divisor: Int) extends PartitionMapper {
    private val lexicoder = LexiTypeEncoders.longEncoder()
    override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.truncate(scheme.attribute, divisor)
    override def toIceberg(key: String): String = lexicoder.decode(key).toString
    override def fromIceberg(partition: StructLike, i: Int): String = lexicoder.encode(partition.get(i, classOf[java.lang.Long]))
    override def expression(key: String): Expression =
      Expressions.equal(Expressions.truncate[java.lang.Long](scheme.attribute, divisor), lexicoder.decode(key))
  }
}
