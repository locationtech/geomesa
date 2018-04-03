/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.schema

import java.io.ByteArrayOutputStream
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point}
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.KuduPredicate.ComparisonOp
import org.apache.kudu.client.{KuduPredicate, PartialRow, RowResult}
import org.apache.kudu.{ColumnSchema, Type}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.kryo.impl.{KryoFeatureDeserialization, KryoFeatureSerialization}
import org.locationtech.geomesa.features.kryo.serialization.KryoGeometrySerialization
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.filter.{Bounds, FilterHelper, checkOrder, isSpatialFilter}
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema.KuduFilter
import org.locationtech.geomesa.kudu.utils.ColumnConfiguration
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.io.ByteBufferInputStream
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.spatial.BinarySpatialOperator

import scala.reflect.ClassTag

/**
  * Helper class for operating on RowResult and PartialRow instances
  *
  * @tparam T underlying value binding
  */
trait KuduColumnAdapter[T] {
  def name: String
  def columns: Seq[ColumnSchema]
  def readFromRow(row: RowResult): T
  def writeToRow(row: PartialRow, value: T): Unit
  def transfer(from: RowResult, to: PartialRow): Unit = writeToRow(to, readFromRow(from))
  // note: scans don't support ORs, so this is somewhat limited in what it can represent
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
      case ObjectType.LIST     => KryoColumnAdapter(name, bindings, config)
      case ObjectType.MAP      => KryoColumnAdapter(name, bindings, config)
      case ObjectType.GEOMETRY =>
        if (bindings(1) == ObjectType.POINT) {
          PointColumnAdapter(name, config, descriptor != sft.getGeometryDescriptor)
        } else {
          GeometryColumnAdapter(name, config, descriptor != sft.getGeometryDescriptor)
        }
      case _ => throw new NotImplementedError(s"Can't handle attribute '$name' of type ${descriptor.getType.getBinding}")
    }
  }

  case class StringColumnAdapter(name: String, config: ColumnConfiguration) extends KuduColumnAdapter[String] {

    private val column = s"${name}_sft"
    override val columns: Seq[ColumnSchema] =
      Seq(config(new ColumnSchemaBuilder(column, Type.STRING)).nullable(true).build())

    override def readFromRow(row: RowResult): String = row.getString(column)

    override def writeToRow(row: PartialRow, value: String): Unit =
      if (value == null) { row.setNull(column) } else { row.addString(column, value) }

    override def predicate(filter: Filter): KuduFilter =
      KuduColumnAdapter.predicate[String](filter, name, newComparisonPredicate(columns.head, _, _))
  }

  case class IntColumnAdapter(name: String, config: ColumnConfiguration) extends KuduColumnAdapter[Integer] {

    private val column = s"${name}_sft"
    override val columns: Seq[ColumnSchema] =
      Seq(config(new ColumnSchemaBuilder(column, Type.INT32)).nullable(true).build())

    override def readFromRow(row: RowResult): Integer =
      if (row.isNull(column)) { null } else { Int.box(row.getInt(column)) }

    override def writeToRow(row: PartialRow, value: Integer): Unit =
      if (value == null) { row.setNull(column) } else { row.addInt(column, value.intValue()) }

    override def predicate(filter: Filter): KuduFilter =
      KuduColumnAdapter.predicate[Int](filter, name, newComparisonPredicate(columns.head, _, _))
  }

  case class LongColumnAdapter(name: String, config: ColumnConfiguration) extends KuduColumnAdapter[java.lang.Long] {

    private val column = s"${name}_sft"
    override val columns: Seq[ColumnSchema] =
      Seq(config(new ColumnSchemaBuilder(column, Type.INT64)).nullable(true).build())

    override def readFromRow(row: RowResult): java.lang.Long =
      if (row.isNull(column)) { null } else { Long.box(row.getLong(column)) }

    override def writeToRow(row: PartialRow, value: java.lang.Long): Unit =
      if (value == null) { row.setNull(column) } else { row.addLong(column, value.longValue()) }

    override def predicate(filter: Filter): KuduFilter =
      KuduColumnAdapter.predicate[Long](filter, name, newComparisonPredicate(columns.head, _, _))
  }

  case class FloatColumnAdapter(name: String, config: ColumnConfiguration) extends KuduColumnAdapter[java.lang.Float] {

    private val column = s"${name}_sft"
    override val columns: Seq[ColumnSchema] =
      Seq(config(new ColumnSchemaBuilder(column, Type.FLOAT)).nullable(true).build())

    override def readFromRow(row: RowResult): java.lang.Float =
      if (row.isNull(column)) { null } else { Float.box(row.getFloat(column)) }

    override def writeToRow(row: PartialRow, value: java.lang.Float): Unit =
      if (value == null) { row.setNull(column) } else { row.addFloat(column, value.floatValue()) }

    override def predicate(filter: Filter): KuduFilter =
      KuduColumnAdapter.predicate[Float](filter, name, newComparisonPredicate(columns.head, _, _))
  }

  case class DoubleColumnAdapter(name: String, config: ColumnConfiguration) extends KuduColumnAdapter[java.lang.Double] {

    private val column = s"${name}_sft"
    override val columns: Seq[ColumnSchema] =
      Seq(config(new ColumnSchemaBuilder(column, Type.DOUBLE)).nullable(true).build())

    override def readFromRow(row: RowResult): java.lang.Double =
      if (row.isNull(column)) { null } else { Double.box(row.getDouble(column)) }

    override def writeToRow(row: PartialRow, value: java.lang.Double): Unit =
      if (value == null) { row.setNull(column) } else { row.addDouble(column, value.doubleValue()) }

    override def predicate(filter: Filter): KuduFilter =
      KuduColumnAdapter.predicate[Double](filter, name, newComparisonPredicate(columns.head, _, _))
  }

  case class BooleanColumnAdapter(name: String, config: ColumnConfiguration) extends KuduColumnAdapter[java.lang.Boolean] {

    private val column = s"${name}_sft"
    override val columns: Seq[ColumnSchema] =
      Seq(config(new ColumnSchemaBuilder(column, Type.BOOL)).nullable(true).build())

    override def readFromRow(row: RowResult): java.lang.Boolean =
      if (row.isNull(column)) { null } else { Boolean.box(row.getBoolean(column)) }

    override def writeToRow(row: PartialRow, value: java.lang.Boolean): Unit =
      if (value == null) { row.setNull(column) } else { row.addBoolean(column, value.booleanValue()) }

    override def predicate(filter: Filter): KuduFilter =
      KuduColumnAdapter.predicate[Boolean](filter, name, newComparisonPredicate(columns.head, _, _))
  }

  case class BytesColumnAdapter(name: String, config: ColumnConfiguration) extends KuduColumnAdapter[Array[Byte]] {

    private val column = s"${name}_sft"
    override val columns: Seq[ColumnSchema] =
      Seq(config(new ColumnSchemaBuilder(column, Type.BINARY)).nullable(true).build())

    override def readFromRow(row: RowResult): Array[Byte] =
      if (row.isNull(column)) { null } else { row.getBinaryCopy(column) }

    override def writeToRow(row: PartialRow, value: Array[Byte]): Unit =
      if (value == null) { row.setNull(column) } else { row.addBinary(column, value) }

    override def predicate(filter: Filter): KuduFilter =
      KuduColumnAdapter.predicate[Array[Byte]](filter, name, newComparisonPredicate(columns.head, _, _))
  }

  case class DateColumnAdapter(name: String, config: ColumnConfiguration) extends KuduColumnAdapter[Date] {

    private val column = s"${name}_sft"
    override val columns: Seq[ColumnSchema] =
      Seq(config(new ColumnSchemaBuilder(column, Type.UNIXTIME_MICROS)).nullable(true).build())

    override def readFromRow(row: RowResult): Date =
      if (row.isNull(column)) { null } else { new Date(row.getLong(column) / 1000) }

    override def writeToRow(row: PartialRow, value: Date): Unit =
      if (value == null) { row.setNull(column) } else { row.addLong(column, value.getTime * 1000) }

    override def predicate(filter: Filter): KuduFilter =
      KuduColumnAdapter.predicate[Date](filter, name,
        (op, date) => newComparisonPredicate(columns.head, op, date.getTime * 1000))
  }

  case class UuidColumnAdapter(name: String, config: ColumnConfiguration) extends KuduColumnAdapter[UUID] {

    private val column = s"${name}_sft"
    override val columns: Seq[ColumnSchema] =
      Seq(config(new ColumnSchemaBuilder(column, Type.STRING)).nullable(true).build())

    override def readFromRow(row: RowResult): UUID =
      if (row.isNull(column)) { null } else { UUID.fromString(row.getString(column)) }

    override def writeToRow(row: PartialRow, value: UUID): Unit =
      if (value == null) { row.setNull(column) } else { row.addString(column, value.toString) }

    override def predicate(filter: Filter): KuduFilter =
      KuduColumnAdapter.predicate[String](filter, name, newComparisonPredicate(columns.head, _, _))
  }

  case class PointColumnAdapter(name: String, config: ColumnConfiguration, nullable: Boolean)
      extends KuduColumnAdapter[Point] {

    private val (colX, colY) = (s"${name}_sft_x", s"${name}_sft_y")
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
          // TODO it's possible that this will not match the extracted geoms...
          def matches(f: Filter): Boolean = {
            isSpatialFilter(f) && Option(f).collect { case f: BinarySpatialOperator =>
              val exp = checkOrder(f.getExpression1, f.getExpression2)
              // nullable cols are not the primary geom - primary geoms can be referred to with a null name
              if (nullable) { exp.exists(_.name == name) } else { exp.exists(e => e.name == name || e.name == null) }
            }.getOrElse(false)
          }
          val (extracted, remaining) = org.locationtech.geomesa.filter.partitionSubFilters(filter, matches)
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

    private val bytes = new ColumnSchemaBuilder(s"${name}_sft", Type.BINARY)
        .compressionAlgorithm(ColumnConfiguration.compression()).nullable(nullable).build()
    private val Seq(xmin, ymin, xmax, ymax) =
      Seq(s"${name}_sft_xmin", s"${name}_sft_ymin", s"${name}_sft_xmax", s"${name}_sft_ymax").map { n =>
        config(new ColumnSchemaBuilder(n, Type.DOUBLE)).nullable(nullable).build()
      }

    override val columns: Seq[ColumnSchema] = Seq(bytes, xmin, ymin, xmax, ymax)

    override def readFromRow(row: RowResult): Geometry = {
      if (row.isNull(bytes.getName)) { null } else {
        val buffer = row.getBinary(bytes.getName)
        val in = KryoFeatureDeserialization.getInput(new ByteBufferInputStream(buffer))
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
          newComparisonPredicate(xmin, ComparisonOp.LESS_EQUAL, max_x),
          newComparisonPredicate(ymax, ComparisonOp.GREATER_EQUAL, min_y),
          newComparisonPredicate(ymin, ComparisonOp.LESS_EQUAL, max_y)
        )
        // don't exclude the filter - our predicate only covers the bounding box
        KuduFilter(predicates, Some(filter))
      }
    }
  }

  case class KryoColumnAdapter(name: String, bindings: Seq[ObjectType], config: ColumnConfiguration)
      extends KuduColumnAdapter[AnyRef] {

    // note: reader and writer handle null values
    private val writer = KryoFeatureSerialization.matchWriter(bindings)
    private val reader = KryoFeatureDeserialization.matchReader(bindings)

    private val column = s"${name}_sft"
    override val columns: Seq[ColumnSchema] = Seq(config(new ColumnSchemaBuilder(column, Type.BINARY)).build())

    override def readFromRow(row: RowResult): AnyRef = {
      val is = new ByteBufferInputStream(row.getBinary(column))
      reader.apply(KryoFeatureDeserialization.getInput(is))
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

  private def predicate[T](filter: Filter,
                           name: String,
                           toPredicate: (ComparisonOp, T) => KuduPredicate)
                          (implicit ct: ClassTag[T]): KuduFilter = {
    val bounds = FilterHelper.extractAttributeBounds(filter, name, ct.runtimeClass.asInstanceOf[Class[T]])
    if (bounds.isEmpty || !bounds.forall(_.isBounded)) { KuduFilter(Seq.empty, Some(filter)) } else {
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
        case (Some(lo), Some(hi)) if lo == hi => Seq(toPredicate(ComparisonOp.EQUAL, lo))
        case (Some(lo), Some(hi)) => Seq(toPredicate(loOp, lo), toPredicate(hiOp, hi))
        case (Some(lo), None) => Seq(toPredicate(loOp, lo))
        case (None, Some(hi)) => Seq(toPredicate(hiOp, hi))
      }

      val remainingFilter = {
        // if multiple bounds (OR), we haven't fully captured the predicate
        if (bounds.values.lengthCompare(1) > 0) { Some(filter) } else {
          // TODO verify bounds cover the entire extracted filter
          val (extracted, remaining) = org.locationtech.geomesa.filter.partitionSubFilters(filter,
            (f) => org.locationtech.geomesa.filter.getAttributeProperty(f).contains(name))
          if (extracted.isEmpty) { Some(filter) } else {
            org.locationtech.geomesa.filter.andOption(remaining)
          }
        }
      }

      KuduFilter(predicates, remainingFilter)
    }
  }
}
