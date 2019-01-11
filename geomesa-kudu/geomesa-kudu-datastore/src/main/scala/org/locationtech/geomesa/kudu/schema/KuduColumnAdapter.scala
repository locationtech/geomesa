/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.schema

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import org.locationtech.jts.geom.{Coordinate, Geometry, Point}
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.KuduPredicate.ComparisonOp
import org.apache.kudu.client.{KuduPredicate, PartialRow, RowResult}
import org.apache.kudu.{ColumnSchema, Type}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.kryo.impl.{KryoFeatureDeserialization, KryoFeatureSerialization}
import org.locationtech.geomesa.features.kryo.serialization.KryoGeometrySerialization
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema.KuduFilter
import org.locationtech.geomesa.kudu.utils.ColumnConfiguration
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.reflect.ClassTag

/**
  * Helper class for operating on RowResult and PartialRow instances
  *
  * @tparam T underlying value binding
  */
trait KuduColumnAdapter[T] {
  def name: String
  def columns: Seq[ColumnSchema]
  def writeColumns: Seq[ColumnSchema] = columns
  def readFromRow(row: RowResult): T
  def writeToRow(row: PartialRow, value: T): Unit
  def transfer(from: RowResult, to: PartialRow): Unit = writeToRow(to, readFromRow(from))
  // note: scans don't fully support ORs, so this is somewhat limited in what it can represent
  def predicate(filter: Filter): KuduFilter
}

object KuduColumnAdapter {

  import KuduPredicate.newComparisonPredicate

  import scala.collection.JavaConverters._

  private val gf = JTSFactoryFinder.getGeometryFactory

  /**
    * Create a binding for a simple feature attribute
    *
    * Note: we add '_sft' to the attribute name to ensure columns don't conflict with index key columns
    *
    * @param sft simple feature type
    * @param descriptor attribute descriptor
    * @return
    */
  def apply(sft: SimpleFeatureType, descriptor: AttributeDescriptor): KuduColumnAdapter[_ <: AnyRef] = {
    val name = descriptor.getLocalName
    val bindings = ObjectType.selectType(descriptor)
    val config = ColumnConfiguration(bindings.head, descriptor.getUserData.asScala)
    bindings.head match {
      case ObjectType.STRING   => StringColumnAdapter(name, config)
      case ObjectType.INT      => IntColumnAdapter(name, config)
      case ObjectType.LONG     => LongColumnAdapter(name, config)
      case ObjectType.FLOAT    => FloatColumnAdapter(name, config)
      case ObjectType.DOUBLE   => DoubleColumnAdapter(name, config)
      case ObjectType.BOOLEAN  => BooleanColumnAdapter(name, config)
      case ObjectType.DATE     => DateColumnAdapter(name, config)
      case ObjectType.UUID     => UuidColumnAdapter(name, config)
      case ObjectType.BYTES    => BytesColumnAdapter(name, config)
      case ObjectType.JSON     => StringColumnAdapter(name, config)
      case ObjectType.LIST     => KryoColumnAdapter(name, bindings, descriptor, config)
      case ObjectType.MAP      => KryoColumnAdapter(name, bindings, descriptor, config)
      case ObjectType.GEOMETRY =>
        if (bindings(1) == ObjectType.POINT) {
          PointColumnAdapter(sft, name, config, descriptor != sft.getGeometryDescriptor)
        } else {
          GeometryColumnAdapter(name, config, descriptor != sft.getGeometryDescriptor)
        }
      case _ => throw new NotImplementedError(s"Can't handle attribute '$name' of type ${descriptor.getType.getBinding}")
    }
  }

  case class StringColumnAdapter(name: String, config: ColumnConfiguration)
      extends SimpleColumnAdapter[String](Type.STRING, config) {

    override def readFromRow(row: RowResult): String =
      if (row.isNull(column)) { null } else { row.getString(column) }

    override def writeToRow(row: PartialRow, value: String): Unit =
      if (value == null) { row.setNull(column) } else { row.addString(column, value) }

    override protected def predicate(op: ComparisonOp, value: String): KuduPredicate =
      KuduPredicate.newComparisonPredicate(columns.head, op, value)

    override protected def inList(values: Seq[String]): KuduPredicate =
      KuduPredicate.newInListPredicate(columns.head, values.asJava)
  }

  case class IntColumnAdapter(name: String, config: ColumnConfiguration)
      extends SimpleColumnAdapter[Integer](Type.INT32, config) {

    override def readFromRow(row: RowResult): Integer =
      if (row.isNull(column)) { null } else { Int.box(row.getInt(column)) }

    override def writeToRow(row: PartialRow, value: Integer): Unit =
      if (value == null) { row.setNull(column) } else { row.addInt(column, value.intValue()) }

    override protected def predicate(op: ComparisonOp, value: Integer): KuduPredicate =
      KuduPredicate.newComparisonPredicate(columns.head, op, value.intValue())

    override protected def inList(values: Seq[Integer]): KuduPredicate =
      KuduPredicate.newInListPredicate(columns.head, values.map(_.intValue()).asJava)
  }

  case class LongColumnAdapter(name: String, config: ColumnConfiguration)
      extends SimpleColumnAdapter[java.lang.Long](Type.INT64, config) {

    override def readFromRow(row: RowResult): java.lang.Long =
      if (row.isNull(column)) { null } else { Long.box(row.getLong(column)) }

    override def writeToRow(row: PartialRow, value: java.lang.Long): Unit =
      if (value == null) { row.setNull(column) } else { row.addLong(column, value.longValue()) }

    override protected def predicate(op: ComparisonOp, value: java.lang.Long): KuduPredicate =
      KuduPredicate.newComparisonPredicate(columns.head, op, value.longValue())

    override protected def inList(values: Seq[java.lang.Long]): KuduPredicate =
      KuduPredicate.newInListPredicate(columns.head, values.map(_.longValue()).asJava)
  }

  case class FloatColumnAdapter(name: String, config: ColumnConfiguration)
      extends SimpleColumnAdapter[java.lang.Float](Type.FLOAT, config) {

    override def readFromRow(row: RowResult): java.lang.Float =
      if (row.isNull(column)) { null } else { Float.box(row.getFloat(column)) }

    override def writeToRow(row: PartialRow, value: java.lang.Float): Unit =
      if (value == null) { row.setNull(column) } else { row.addFloat(column, value.floatValue()) }

    override protected def predicate(op: ComparisonOp, value: java.lang.Float): KuduPredicate =
      KuduPredicate.newComparisonPredicate(columns.head, op, value.floatValue())

    override protected def inList(values: Seq[java.lang.Float]): KuduPredicate =
      KuduPredicate.newInListPredicate(columns.head, values.map(_.floatValue()).asJava)
  }

  case class DoubleColumnAdapter(name: String, config: ColumnConfiguration)
      extends SimpleColumnAdapter[java.lang.Double](Type.DOUBLE, config) {

    override def readFromRow(row: RowResult): java.lang.Double =
      if (row.isNull(column)) { null } else { Double.box(row.getDouble(column)) }

    override def writeToRow(row: PartialRow, value: java.lang.Double): Unit =
      if (value == null) { row.setNull(column) } else { row.addDouble(column, value.doubleValue()) }

    override protected def predicate(op: ComparisonOp, value: java.lang.Double): KuduPredicate =
      KuduPredicate.newComparisonPredicate(columns.head, op, value.doubleValue())

    override protected def inList(values: Seq[java.lang.Double]): KuduPredicate =
      KuduPredicate.newInListPredicate(columns.head, values.map(_.doubleValue()).asJava)
  }

  case class BooleanColumnAdapter(name: String, config: ColumnConfiguration)
      extends SimpleColumnAdapter[java.lang.Boolean](Type.BOOL, config) {

    override def readFromRow(row: RowResult): java.lang.Boolean =
      if (row.isNull(column)) { null } else { Boolean.box(row.getBoolean(column)) }

    override def writeToRow(row: PartialRow, value: java.lang.Boolean): Unit =
      if (value == null) { row.setNull(column) } else { row.addBoolean(column, value.booleanValue()) }

    override protected def predicate(op: ComparisonOp, value: java.lang.Boolean): KuduPredicate =
      KuduPredicate.newComparisonPredicate(columns.head, op, value.booleanValue())

    override protected def inList(values: Seq[java.lang.Boolean]): KuduPredicate =
      KuduPredicate.newInListPredicate(columns.head, values.map(_.booleanValue()).asJava)
  }

  case class BytesColumnAdapter(name: String, config: ColumnConfiguration)
      extends SimpleColumnAdapter[Array[Byte]](Type.BINARY, config) {

    override def readFromRow(row: RowResult): Array[Byte] =
      if (row.isNull(column)) { null } else { row.getBinaryCopy(column) }

    override def writeToRow(row: PartialRow, value: Array[Byte]): Unit =
      if (value == null) { row.setNull(column) } else { row.addBinary(column, value) }

    override protected def predicate(op: ComparisonOp, value: Array[Byte]): KuduPredicate =
      KuduPredicate.newComparisonPredicate(columns.head, op, value)

    override protected def inList(values: Seq[Array[Byte]]): KuduPredicate =
      KuduPredicate.newInListPredicate(columns.head, values.asJava)
  }

  case class DateColumnAdapter(name: String, config: ColumnConfiguration)
      extends SimpleColumnAdapter[Date](Type.UNIXTIME_MICROS, config) {

    override def readFromRow(row: RowResult): Date =
      if (row.isNull(column)) { null } else { new Date(row.getLong(column) / 1000) }

    override def writeToRow(row: PartialRow, value: Date): Unit =
      if (value == null) { row.setNull(column) } else { row.addLong(column, value.getTime * 1000) }

    override protected def predicate(op: ComparisonOp, value: Date): KuduPredicate =
      KuduPredicate.newComparisonPredicate(columns.head, op, value.getTime * 1000L)

    override protected def inList(values: Seq[Date]): KuduPredicate =
      KuduPredicate.newInListPredicate(columns.head, values.map(_.getTime * 1000L).asJava)
  }

  case class UuidColumnAdapter(name: String, config: ColumnConfiguration)
      extends SimpleColumnAdapter[UUID](Type.BINARY, config) {

    override def readFromRow(row: RowResult): UUID =
      if (row.isNull(column)) { null } else {
        val bytes = row.getBinary(column)
        new UUID(bytes.getLong(), bytes.getLong())
      }

    override def writeToRow(row: PartialRow, value: UUID): Unit =
      if (value == null) { row.setNull(column) } else {
        val bb = ByteBuffer.allocate(16)
        bb.putLong(value.getMostSignificantBits)
        bb.putLong(value.getLeastSignificantBits)
        bb.flip()
        row.addBinary(column, bb)
      }

    override protected def predicate(op: ComparisonOp, value: UUID): KuduPredicate =
      KuduPredicate.newComparisonPredicate(columns.head, op, toBytes(value))

    override protected def inList(values: Seq[UUID]): KuduPredicate =
      KuduPredicate.newInListPredicate(columns.head, values.map(toBytes).asJava)

    private def toBytes(uuid: UUID): Array[Byte] = {
      val bytes = Array.ofDim[Byte](16)
      val bb = ByteBuffer.wrap(bytes)
      bb.putLong(uuid.getMostSignificantBits)
      bb.putLong(uuid.getLeastSignificantBits)
      bytes
    }
  }

  case class PointColumnAdapter(sft: SimpleFeatureType, name: String, config: ColumnConfiguration, nullable: Boolean)
      extends KuduColumnAdapter[Point] {

    // note: we add a modifier to ensure the name doesn't conflict with our index columns or any other columns
    private val colX = s"${name}_sft_x"
    private val colY = s"${name}_sft_y"

    override val columns: Seq[ColumnSchema] = Seq(colX, colY).map { n =>
      config(new ColumnSchemaBuilder(n, Type.DOUBLE)).nullable(nullable).build()
    }

    override def readFromRow(row: RowResult): Point =
      if (row.isNull(colX)) { null } else { gf.createPoint(new Coordinate(row.getDouble(colX), row.getDouble(colY))) }

    override def writeToRow(row: PartialRow, value: Point): Unit = {
      if (value == null) {
        row.setNull(colX)
        row.setNull(colY)
      } else {
        row.addDouble(colX, value.getX)
        row.addDouble(colY, value.getY)
      }
    }

    override def predicate(filter: Filter): KuduFilter = {
      val geometries = FilterHelper.extractGeometries(filter, name)
      if (geometries.isEmpty) { KuduFilter(Seq.empty, Some(filter)) } else {
        var xmin, ymin = Double.MaxValue
        var xmax, ymax = Double.MinValue
        geometries.foreach { geom =>
          val env = geom.getEnvelopeInternal
          xmin = math.min(xmin, env.getMinX)
          ymin = math.min(ymin, env.getMinY)
          xmax = math.max(xmax, env.getMaxX)
          ymax = math.max(ymax, env.getMaxY)
        }
        val predicates = Seq(
          newComparisonPredicate(columns.head, ComparisonOp.GREATER_EQUAL, xmin),
          newComparisonPredicate(columns.head, ComparisonOp.LESS_EQUAL, xmax),
          newComparisonPredicate(columns.last, ComparisonOp.GREATER_EQUAL, ymin),
          newComparisonPredicate(columns.last, ComparisonOp.LESS_EQUAL, ymax)
        )

        val covered = geometries.values.lengthCompare(2) < 0 && geometries.forall(GeometryUtils.isRectangular)
        val remainingFilter = if (!covered) { Some(filter) } else {
          // TODO verify bounds cover the entire extracted filter
          val (extracted, remaining) = org.locationtech.geomesa.filter.partitionSubFilters(filter,
            (f) => FilterHelper.propertyNames(f, sft) == Seq(name))
          if (extracted.isEmpty) { Some(filter) } else {
            org.locationtech.geomesa.filter.andOption(remaining)
          }
        }

        KuduFilter(predicates, remainingFilter)
      }
    }
  }

  case class GeometryColumnAdapter(name: String, config: ColumnConfiguration, nullable: Boolean)
      extends KuduColumnAdapter[Geometry] {

    // store the binary geometry, and the bounds for push-down predicates

    // note: we add a modifier to ensure the name doesn't conflict with our index columns
    private val bytes = config(new ColumnSchemaBuilder(s"${name}_sft", Type.BINARY)).nullable(nullable).build()

    // note: we add a modifier to ensure the name doesn't conflict with our index columns or any other columns
    private val Seq(xmin, ymin, xmax, ymax) =
      Seq(s"${name}_sft_xmin", s"${name}_sft_ymin", s"${name}_sft_xmax", s"${name}_sft_ymax").map { n =>
        new ColumnSchemaBuilder(n, Type.DOUBLE).nullable(nullable).build()
      }

    // we only read the binary bytes column, the bounds are used for predicate push-down but never returned
    override val columns: Seq[ColumnSchema] = Seq(bytes)

    override val writeColumns: Seq[ColumnSchema] = Seq(bytes, xmin, ymin, xmax, ymax)

    override def readFromRow(row: RowResult): Geometry = {
      import org.locationtech.geomesa.utils.io.ByteBuffers.RichByteBuffer
      if (row.isNull(bytes.getName)) { null } else {
        val buffer = row.getBinary(bytes.getName)
        val in = KryoFeatureDeserialization.getInput(buffer.toInputStream)
        KryoGeometrySerialization.deserialize(in)
      }
    }

    override def writeToRow(row: PartialRow, value: Geometry): Unit = {
      if (value == null) {
        row.setNull(bytes.getName)
        row.setNull(xmin.getName)
        row.setNull(ymin.getName)
        row.setNull(xmax.getName)
        row.setNull(ymax.getName)
      } else {
        val stream = new ByteArrayOutputStream
        val out = KryoFeatureSerialization.getOutput(stream)
        KryoGeometrySerialization.serialize(out, value)
        out.flush()
        row.addBinary(bytes.getName, stream.toByteArray)
        val env = value.getEnvelopeInternal
        row.addDouble(xmin.getName, env.getMinX)
        row.addDouble(ymin.getName, env.getMinY)
        row.addDouble(xmax.getName, env.getMaxX)
        row.addDouble(ymax.getName, env.getMaxY)
      }
    }

    override def predicate(filter: Filter): KuduFilter = {
      val geometries = FilterHelper.extractGeometries(filter, name)
      if (geometries.isEmpty) { KuduFilter(Seq.empty, Some(filter)) } else {
        var min_x, min_y = Double.MaxValue
        var max_x, max_y = Double.MinValue
        geometries.foreach { geom =>
          val env = geom.getEnvelopeInternal
          min_x = math.min(min_x, env.getMinX)
          min_y = math.min(min_y, env.getMinY)
          max_x = math.max(max_x, env.getMaxX)
          max_y = math.max(max_y, env.getMaxY)
        }
        val predicates = Seq(
          newComparisonPredicate(xmax, ComparisonOp.GREATER_EQUAL, min_x),
          newComparisonPredicate(xmin, ComparisonOp.LESS_EQUAL,    max_x),
          newComparisonPredicate(ymax, ComparisonOp.GREATER_EQUAL, min_y),
          newComparisonPredicate(ymin, ComparisonOp.LESS_EQUAL,    max_y)
        )
        // don't exclude the filter - our predicate only covers the bounding box
        KuduFilter(predicates, Some(filter))
      }
    }
  }

  /**
    * Encodes variable-size types (lists and maps) for storing in a fixed number of columns (i.e. one)
    */
  case class KryoColumnAdapter(name: String,
                               bindings: Seq[ObjectType],
                               descriptor: AttributeDescriptor,
                               config: ColumnConfiguration) extends KuduColumnAdapter[AnyRef] {

    // note: reader and writer handle null values
    private val writer = KryoFeatureSerialization.matchWriter(bindings, descriptor)
    private val reader = KryoFeatureDeserialization.matchReader(bindings)

    private val column = s"${name}_sft"
    override val columns: Seq[ColumnSchema] = Seq(config(new ColumnSchemaBuilder(column, Type.BINARY)).build())

    override def readFromRow(row: RowResult): AnyRef = {
      import org.locationtech.geomesa.utils.io.ByteBuffers.RichByteBuffer
      reader.apply(KryoFeatureDeserialization.getInput(row.getBinary(column).toInputStream))
    }

    override def writeToRow(row: PartialRow, value: AnyRef): Unit = {
      val stream = new ByteArrayOutputStream()
      val out = KryoFeatureSerialization.getOutput(stream)
      writer.apply(out, value)
      out.flush()
      row.addBinary(column, stream.toByteArray)
    }

    override def predicate(filter: Filter): KuduFilter = KuduFilter(Seq.empty, Some(filter))
  }

  abstract class SimpleColumnAdapter[T] private [KuduColumnAdapter]
        (typed: Type, config: ColumnConfiguration)
        (implicit ct: ClassTag[T])
      extends KuduColumnAdapter[T] {

    // note: we add a modifier to ensure the name doesn't conflict with our index columns
    protected val column = s"${name}_sft"

    override val columns: Seq[ColumnSchema] =
      Seq(config(new ColumnSchemaBuilder(column, typed)).nullable(true).build())

    override def predicate(filter: Filter): KuduFilter = {
      val bounds = FilterHelper.extractAttributeBounds(filter, name, ct.runtimeClass.asInstanceOf[Class[T]])
      if (bounds.isEmpty || !bounds.forall(_.isBounded)) {
        KuduFilter(Seq.empty, Some(filter))
      } else if (bounds.forall(_.isEquals)) {
        if (bounds.values.lengthCompare(1) == 0) {
          KuduFilter(Seq(predicate(ComparisonOp.EQUAL, bounds.values.head.lower.value.get)), remainingFilter(filter))
        } else {
          KuduFilter(Seq(inList(bounds.values.map(_.lower.value.get))), remainingFilter(filter))
        }
      } else {
        // range filter
        // kudu doesn't support ORs so take the min/max bounds
        var lower = bounds.values.head.lower
        var upper = bounds.values.head.upper
        bounds.values.tail.foreach { bound =>
          lower = Bounds.smallerLowerBound(lower, bound.lower)
          upper = Bounds.largerUpperBound(lower, bound.lower)
        }

        val loOp = if (lower.inclusive) { ComparisonOp.GREATER_EQUAL } else { ComparisonOp.GREATER }
        val hiOp = if (upper.inclusive) { ComparisonOp.LESS_EQUAL } else { ComparisonOp.LESS }
        val predicates = (lower.value, upper.value) match {
          case (Some(lo), Some(hi)) => Seq(predicate(loOp, lo), predicate(hiOp, hi))
          case (Some(lo), None)     => Seq(predicate(loOp, lo))
          case (None, Some(hi))     => Seq(predicate(hiOp, hi))
        }

        // if multiple bounds (OR), we haven't fully captured the predicate
        val remaining = if (bounds.values.lengthCompare(1) > 0) { Some(filter) } else { remainingFilter(filter) }

        KuduFilter(predicates, remaining)
      }
    }

    protected def predicate(op: ComparisonOp, value: T): KuduPredicate
    protected def inList(values: Seq[T]): KuduPredicate

    private def remainingFilter(filter: Filter): Option[Filter] = {
      // TODO verify bounds cover the entire extracted filter
      val (extracted, remaining) = org.locationtech.geomesa.filter.partitionSubFilters(filter,
        (f) => FilterHelper.propertyNames(f, null) == Seq(name))
      if (extracted.isEmpty) { Some(filter) } else {
        org.locationtech.geomesa.filter.andOption(remaining)
      }
    }
  }
}
