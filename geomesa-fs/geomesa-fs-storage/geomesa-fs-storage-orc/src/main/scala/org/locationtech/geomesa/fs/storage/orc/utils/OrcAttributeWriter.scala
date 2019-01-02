/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc.utils

import java.nio.charset.StandardCharsets
import java.util.UUID

import org.locationtech.jts.geom._
import org.apache.orc.storage.ql.exec.vector._
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Writes a simple feature to a given Orc row
  */
trait OrcAttributeWriter {
  def apply(sf: SimpleFeature, row: Int): Unit
}

object OrcAttributeWriter {

  /**
    * Create a writer for simple feature to the given vector batch
    *
    * @param sft simple feature type
    * @param batch vector batch
    * @param fid write feature id or not
    * @return
    */
  def apply(sft: SimpleFeatureType, batch: VectorizedRowBatch, fid: Boolean = true): OrcAttributeWriter = {
    require(batch.cols.length == OrcFileSystemStorage.fieldCount(sft, fid),
      s"ORC schema does not match SimpleFeatureType: ${batch.cols.map(_.getClass.getName).mkString("\n\t", "\n\t", "")}")

    val builder = Seq.newBuilder[OrcAttributeWriter]
    builder.sizeHint(sft.getAttributeCount + (if (fid) { 1 } else { 0 }))

    var i = 0
    var col = 0
    while (i < sft.getAttributeCount) {
      val bindings = ObjectType.selectType(sft.getDescriptor(i))
      val writer = bindings.head match {
        case ObjectType.GEOMETRY => col += 1; createGeometryWriter(bindings(1), batch.cols(col - 1), batch.cols(col), i)
        case ObjectType.DATE     => new DateWriter(batch.cols(col).asInstanceOf[TimestampColumnVector], i)
        case ObjectType.STRING   => new StringWriter(batch.cols(col).asInstanceOf[BytesColumnVector], i)
        case ObjectType.INT      => new IntWriter(batch.cols(col).asInstanceOf[LongColumnVector], i)
        case ObjectType.LONG     => new LongWriter(batch.cols(col).asInstanceOf[LongColumnVector], i)
        case ObjectType.FLOAT    => new FloatWriter(batch.cols(col).asInstanceOf[DoubleColumnVector], i)
        case ObjectType.DOUBLE   => new DoubleWriter(batch.cols(col).asInstanceOf[DoubleColumnVector], i)
        case ObjectType.BOOLEAN  => new BooleanWriter(batch.cols(col).asInstanceOf[LongColumnVector], i)
        case ObjectType.BYTES    => new BytesWriter(batch.cols(col).asInstanceOf[BytesColumnVector], i)
        case ObjectType.JSON     => new StringWriter(batch.cols(col).asInstanceOf[BytesColumnVector], i)
        case ObjectType.UUID     => new UuidWriter(batch.cols(col).asInstanceOf[BytesColumnVector], i)
        case ObjectType.LIST     => new ListWriter(batch.cols(col).asInstanceOf[ListColumnVector], i, bindings(1))
        case ObjectType.MAP      => new MapWriter(batch.cols(col).asInstanceOf[MapColumnVector], i, bindings(1), bindings(2))
        case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
      }
      builder += writer
      i += 1
      col += 1
    }

    if (fid) {
      builder += new FidWriter(batch.cols(col).asInstanceOf[BytesColumnVector])
    }

    new SequenceWriter(builder.result)
  }

  // noinspection LanguageFeature
  private def createGeometryWriter(binding: ObjectType, x: ColumnVector, y: ColumnVector, i: Int): OrcAttributeWriter = {
    implicit def toDoubleColumnVector(vec: ColumnVector): DoubleColumnVector = vec.asInstanceOf[DoubleColumnVector]
    implicit def toListColumnVector(vec: ColumnVector): ListColumnVector = vec.asInstanceOf[ListColumnVector]

    binding match {
      case ObjectType.POINT           => new PointWriter(x, y, i)
      case ObjectType.LINESTRING      => new LineStringWriter(x, y, i)
      case ObjectType.MULTIPOINT      => new MultiPointWriter(x, y, i)
      case ObjectType.POLYGON         => new PolygonWriter(x, y, i)
      case ObjectType.MULTILINESTRING => new MultiLineStringWriter(x, y, i)
      case ObjectType.MULTIPOLYGON    => new MultiPolygonWriter(x, y, i)
      case _ => throw new IllegalArgumentException(s"Unexpected object type $binding")
    }
  }

  // invokes a sequence of writers in a single call
  class SequenceWriter(writers: Seq[OrcAttributeWriter]) extends OrcAttributeWriter {
    override def apply(sf: SimpleFeature, row: Int): Unit = writers.foreach(_.apply(sf, row))
  }

  // writes a feature ID to a vector
  class FidWriter(val vector: BytesColumnVector) extends OrcAttributeWriter with SetVectorString {
    override def apply(sf: SimpleFeature, row: Int): Unit = setValue(sf.getID, row)
  }

  // writes a string attribute to a vector
  class StringWriter(val vector: BytesColumnVector, val attribute: Int)
      extends VectorWriterAdapter[String, BytesColumnVector] with SetVectorString

  // writes a date attribute to a vector
  class DateWriter(val vector: TimestampColumnVector, val attribute: Int)
      extends VectorWriterAdapter[java.util.Date, TimestampColumnVector] with SetVectorDate

  // writes an int attribute to a vector
  class IntWriter(val vector: LongColumnVector, val attribute: Int)
      extends VectorWriterAdapter[java.lang.Integer, LongColumnVector] with SetVectorInt

  // writes a long attribute to a vector
  class LongWriter(val vector: LongColumnVector, val attribute: Int)
      extends VectorWriterAdapter[java.lang.Long, LongColumnVector] with SetVectorLong

  // writes a float attribute to a vector
  class FloatWriter(val vector: DoubleColumnVector, val attribute: Int)
      extends VectorWriterAdapter[java.lang.Float, DoubleColumnVector] with SetVectorFloat

  // writes a double attribute to a vector
  class DoubleWriter(val vector: DoubleColumnVector, val attribute: Int)
      extends VectorWriterAdapter[java.lang.Double, DoubleColumnVector] with SetVectorDouble

  // writes a boolean attribute to a vector
  class BooleanWriter(val vector: LongColumnVector, val attribute: Int)
      extends VectorWriterAdapter[java.lang.Boolean, LongColumnVector] with SetVectorBoolean

  // writes a byte array attribute to a vector
  class BytesWriter(val vector: BytesColumnVector, val attribute: Int)
      extends VectorWriterAdapter[Array[Byte], BytesColumnVector] with SetVectorBytes

  // writes a UUID attribute to a vector
  class UuidWriter(val vector: BytesColumnVector, val attribute: Int)
      extends OrcAttributeWriter with SetVectorString {
    override def apply(sf: SimpleFeature, row: Int): Unit =
      setValue(Option(sf.getAttribute(attribute).asInstanceOf[UUID]).map(_.toString).orNull, row)
  }

  /**
    * Writes a point attribute to a vector
    *
    * @param x x coordinates
    * @param y y coordinates
    * @param attribute simple feature attribute index
    */
  class PointWriter(x: DoubleColumnVector, y: DoubleColumnVector, attribute: Int) extends OrcAttributeWriter {
    override def apply(sf: SimpleFeature, row: Int): Unit = {
      val value = sf.getAttribute(attribute).asInstanceOf[Point]
      if (value != null) {
        x.vector(row) = value.getX
        y.vector(row) = value.getY
      } else {
        x.noNulls = false
        y.noNulls = false
        x.isNull(row) = true
        y.isNull(row) = true
      }
    }
  }

  /**
    * Writes a linestring attribute to a vector. A linestring is modeled as a list of points.
    *
    * @see PointWriter
    *
    * @param xx outer list vector for x coordinates, containing a double vector for points
    * @param yy outer list vector for y coordinates, containing a double vector for points
    * @param attribute simple feature attribute index
    */
  class LineStringWriter(xx: ListColumnVector, yy: ListColumnVector, attribute: Int) extends OrcAttributeWriter {
    private val x = xx.child.asInstanceOf[DoubleColumnVector]
    private val y = yy.child.asInstanceOf[DoubleColumnVector]

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      val value = sf.getAttribute(attribute).asInstanceOf[LineString]
      if (value != null) {
        val length = value.getNumPoints
        Seq(xx, yy).foreach { vector =>
          vector.child.ensureSize(vector.childCount + length, true)
          vector.offsets(row) = vector.childCount
          vector.lengths(row) = length
        }
        var i = 0
        while (i < length) {
          val pt = value.getCoordinateN(i)
          x.vector(xx.childCount + i) = pt.x
          y.vector(yy.childCount + i) = pt.y
          i += 1
        }
        xx.childCount += length
        yy.childCount += length
      } else {
        xx.noNulls = false
        yy.noNulls = false
        xx.isNull(row) = true
        yy.isNull(row) = true
      }
    }
  }

  /**
    * Writes a multi-point attribute to a vector. A multi-point is modeled as a list of points.
    *
    * @see PointWriter
    *
    * @param xx outer list vector for x coordinates, containing a double vector for points
    * @param yy outer list vector for y coordinates, containing a double vector for points
    * @param attribute simple feature attribute index
    */
  class MultiPointWriter(xx: ListColumnVector, yy: ListColumnVector, attribute: Int) extends OrcAttributeWriter {
    private val x = xx.child.asInstanceOf[DoubleColumnVector]
    private val y = yy.child.asInstanceOf[DoubleColumnVector]

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      val value = sf.getAttribute(attribute).asInstanceOf[MultiPoint]
      if (value != null) {
        val length = value.getNumPoints
        Seq(xx, yy).foreach { vector =>
          vector.child.ensureSize(vector.childCount + length, true)
          vector.offsets(row) = vector.childCount
          vector.lengths(row) = length
        }
        var i = 0
        while (i < length) {
          val pt = value.getGeometryN(i).asInstanceOf[Point]
          x.vector(xx.childCount + i) = pt.getX
          y.vector(yy.childCount + i) = pt.getY
          i += 1
        }
        xx.childCount += length
        yy.childCount += length
      } else {
        xx.noNulls = false
        yy.noNulls = false
        xx.isNull(row) = true
        yy.isNull(row) = true
      }
    }
  }

  /**
    * Writes a polygon attribute to a vector. A polygon is modeled as a list of lines, with the first
    * value being the shell, and any subsequent values being interior holes.
    *
    * @see LineStringWriter
    *
    * @param xxx outer list vector for x coordinates, containing a list vector for individual lines
    * @param yyy outer list vector for y coordinates, containing a list vector for individual lines
    * @param attribute simple feature attribute index
    */
  class PolygonWriter(xxx: ListColumnVector, yyy: ListColumnVector, attribute: Int) extends OrcAttributeWriter {
    // list of points for each line
    private val xx = xxx.child.asInstanceOf[ListColumnVector]
    private val yy = yyy.child.asInstanceOf[ListColumnVector]
    // points
    private val x = xx.child.asInstanceOf[DoubleColumnVector]
    private val y = yy.child.asInstanceOf[DoubleColumnVector]

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      val polygon = sf.getAttribute(attribute).asInstanceOf[Polygon]
      if (polygon != null) {
        val lineCount = polygon.getNumInteriorRing + 1
        Seq(xxx, yyy).foreach { vector =>
          vector.child.ensureSize(vector.childCount + lineCount, true)
          vector.offsets(row) = vector.childCount
          vector.lengths(row) = lineCount
        }
        var j = 0
        while (j < lineCount) {
          val line = if (j == 0) { polygon.getExteriorRing } else { polygon.getInteriorRingN(j - 1) }
          val length = line.getNumPoints
          Seq(xx, yy).foreach { vector =>
            vector.child.ensureSize(vector.childCount + length, true)
            vector.offsets(xxx.childCount + j) = vector.childCount
            vector.lengths(xxx.childCount + j) = length
          }
          var i = 0
          while (i < length) {
            val pt = line.getCoordinateN(i)
            x.vector(xx.childCount + i) = pt.x
            y.vector(yy.childCount + i) = pt.y
            i += 1
          }
          xx.childCount += length
          yy.childCount += length
          j += 1
        }
        xxx.childCount += lineCount
        yyy.childCount += lineCount
      } else {
        xxx.noNulls = false
        yyy.noNulls = false
        xxx.isNull(row) = true
        yyy.isNull(row) = true
      }
    }
  }

  /**
    * Writes a multi-linestring attribute to a vector. A multi-linestring is modeled as a list of lines.
    *
    * @see LineStringWriter
    *
    * @param xxx outer list vector for x coordinates, containing a list vector for individual lines
    * @param yyy outer list vector for y coordinates, containing a list vector for individual lines
    * @param attribute simple feature attribute index
    */
  class MultiLineStringWriter(xxx: ListColumnVector, yyy: ListColumnVector, attribute: Int) extends OrcAttributeWriter {
    // list of points for each line
    private val xx = xxx.child.asInstanceOf[ListColumnVector]
    private val yy = yyy.child.asInstanceOf[ListColumnVector]
    // points
    private val x = xx.child.asInstanceOf[DoubleColumnVector]
    private val y = yy.child.asInstanceOf[DoubleColumnVector]

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      val value = sf.getAttribute(attribute).asInstanceOf[MultiLineString]
      if (value != null) {
        val lineCount = value.getNumGeometries
        Seq(xxx, yyy).foreach { vector =>
          vector.child.ensureSize(vector.childCount + lineCount, true)
          vector.offsets(row) = vector.childCount
          vector.lengths(row) = lineCount
        }
        var j = 0
        while (j < lineCount) {
          val line = value.getGeometryN(j).asInstanceOf[LineString]
          val length = line.getNumPoints
          Seq(xx, yy).foreach { vector =>
            vector.child.ensureSize(vector.childCount + length, true)
            vector.offsets(xxx.childCount + j) = vector.childCount
            vector.lengths(xxx.childCount + j) = length
          }
          var i = 0
          while (i < length) {
            val pt = line.getCoordinateN(i)
            x.vector(xx.childCount + i) = pt.x
            y.vector(yy.childCount + i) = pt.y
            i += 1
          }
          xx.childCount += length
          yy.childCount += length
          j += 1
        }
        xxx.childCount += lineCount
        yyy.childCount += lineCount
      } else {
        xxx.noNulls = false
        yyy.noNulls = false
        xxx.isNull(row) = true
        yyy.isNull(row) = true
      }
    }
  }

  /**
    * Writes a multi-polygon attribute to a vector. A multi-polygon is modeled as a list of polygons.
    *
    * @see PolygonWriter
    *
    * @param xxxx outer list vector for x coordinates, containing a list vector for individual polygons
    * @param yyyy outer list vector for y coordinates, containing a list vector for individual polygons
    * @param attribute simple feature attribute index
    */
  class MultiPolygonWriter(xxxx: ListColumnVector, yyyy: ListColumnVector, attribute: Int) extends OrcAttributeWriter {
    // list of lines for each polygon
    private val xxx = xxxx.child.asInstanceOf[ListColumnVector]
    private val yyy = yyyy.child.asInstanceOf[ListColumnVector]
    // list of points for each line
    private val xx = xxx.child.asInstanceOf[ListColumnVector]
    private val yy = yyy.child.asInstanceOf[ListColumnVector]
    // points
    private val x = xx.child.asInstanceOf[DoubleColumnVector]
    private val y = yy.child.asInstanceOf[DoubleColumnVector]

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      val multiPolygon = sf.getAttribute(attribute).asInstanceOf[MultiPolygon]
      if (multiPolygon != null) {
        val polygonCount = multiPolygon.getNumGeometries
        Seq(xxxx, yyyy).foreach { vector =>
          vector.child.ensureSize(vector.childCount + polygonCount, true)
          vector.offsets(row) = vector.childCount
          vector.lengths(row) = polygonCount
        }
        var k = 0
        while (k < polygonCount) {
          val polygon = multiPolygon.getGeometryN(k).asInstanceOf[Polygon]
          val lineCount = polygon.getNumInteriorRing + 1
          Seq(xxx, yyy).foreach { vector =>
            vector.child.ensureSize(vector.childCount + lineCount, true)
            vector.offsets(xxxx.childCount + k) = vector.childCount
            vector.lengths(xxxx.childCount + k) = lineCount
          }
          var j = 0
          while (j < lineCount) {
            val line = if (j == 0) { polygon.getExteriorRing } else { polygon.getInteriorRingN(j - 1) }
            val length = line.getNumPoints
            Seq(xx, yy).foreach { vector =>
              vector.child.ensureSize(vector.childCount + length, true)
              vector.offsets(xxx.childCount + j) = vector.childCount
              vector.lengths(xxx.childCount + j) = length
            }
            var i = 0
            while (i < length) {
              val pt = line.getCoordinateN(i)
              x.vector(xx.childCount + i) = pt.x
              y.vector(yy.childCount + i) = pt.y
              i += 1
            }
            xx.childCount += length
            yy.childCount += length
            j += 1
          }
          xxx.childCount += lineCount
          yyy.childCount += lineCount
          k += 1
        }
        xxxx.childCount += polygonCount
        yyyy.childCount += polygonCount
      } else {
        xxxx.noNulls = false
        yyyy.noNulls = false
        xxxx.isNull(row) = true
        yyyy.isNull(row) = true
      }
    }
  }

  /**
    * Writes a java.util.List attribute to a vector
    *
    * @param vector vector
    * @param attribute simple feature attribute index
    * @param binding list value type
    */
  class ListWriter(vector: ListColumnVector, attribute: Int, binding: ObjectType) extends OrcAttributeWriter {

    private val writer = getInnerWriter(binding, vector.child)

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      val value = sf.getAttribute(attribute).asInstanceOf[java.util.List[AnyRef]]
      if (value != null) {
        val length = value.size
        vector.child.ensureSize(vector.childCount + length, true)
        vector.offsets(row) = vector.childCount
        vector.lengths(row) = length
        var i = 0
        while (i < length) {
          writer.setValue(value.get(i), vector.childCount + i)
          i += 1
        }
        vector.childCount += length
      } else {
        vector.noNulls = false
        vector.isNull(row) = true
      }
    }
  }

  /**
    * Writes a java.util.Map attribute to a vector
    *
    * @param vector vector
    * @param attribute simple feature attribute index
    * @param keyBinding map key type
    * @param valueBinding map value type
    */
  class MapWriter(vector: MapColumnVector, attribute: Int, keyBinding: ObjectType, valueBinding: ObjectType)
      extends OrcAttributeWriter {

    private val keyWriter = getInnerWriter(keyBinding, vector.keys)
    private val valueWriter = getInnerWriter(valueBinding, vector.values)

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      import scala.collection.JavaConversions._

      val value = sf.getAttribute(attribute).asInstanceOf[java.util.Map[AnyRef, AnyRef]]
      if (value != null) {
        val length = value.size
        vector.keys.ensureSize(vector.childCount + length, true)
        vector.values.ensureSize(vector.childCount + length, true)
        vector.offsets(row) = vector.childCount
        vector.lengths(row) = length
        var i = 0
        value.foreach { case (k, v) =>
          keyWriter.setValue(k, vector.childCount + i)
          valueWriter.setValue(v, vector.childCount + i)
          i += 1
        }
        vector.childCount += length
      } else {
        vector.noNulls = false
        vector.isNull(row) = true
      }
    }
  }

  // writes a typed value to a vector
  trait SetVectorValue[T <: AnyRef, U <: ColumnVector] {
    protected val vector: U
    def setValue(value: T, row: Int): Unit
  }

  // gets a value from a simple feature and writes it to a vector
  trait VectorWriterAdapter[T <: AnyRef, U <: ColumnVector] extends OrcAttributeWriter with SetVectorValue[T, U] {
    def attribute: Int
    override def apply(sf: SimpleFeature, row: Int): Unit = setValue(sf.getAttribute(attribute).asInstanceOf[T], row)
  }

  // writes a string to a vector
  trait SetVectorString extends SetVectorValue[String, BytesColumnVector] {
    override def setValue(value: String, row: Int): Unit = {
      if (value != null) {
        val bytes = value.getBytes(StandardCharsets.UTF_8)
        vector.setRef(row, bytes, 0, bytes.length)
      } else {
        vector.noNulls = false
        vector.isNull(row) = true
      }
    }
  }

  // writes a date to a vector
  trait SetVectorDate extends SetVectorValue[java.util.Date, TimestampColumnVector] {
    override def setValue(value: java.util.Date, row: Int): Unit = {
      if (value != null) {
        vector.time(row) = value.getTime
      } else {
        vector.noNulls = false
        vector.isNull(row) = true
      }
    }
  }

  // writes an int to a vector
  trait SetVectorInt extends SetVectorValue[java.lang.Integer, LongColumnVector] {
    override def setValue(value: java.lang.Integer, row: Int): Unit = {
      if (value != null) {
        vector.vector(row) = value.longValue
      } else {
        vector.noNulls = false
        vector.isNull(row) = true
      }
    }
  }

  // writes a long to a vector
  trait SetVectorLong extends SetVectorValue[java.lang.Long, LongColumnVector] {
    override def setValue(value: java.lang.Long, row: Int): Unit = {
      if (value != null) {
        vector.vector(row) = value.longValue
      } else {
        vector.noNulls = false
        vector.isNull(row) = true
      }
    }
  }

  // writes a float to a vector
  trait SetVectorFloat extends SetVectorValue[java.lang.Float, DoubleColumnVector] {
    override def setValue(value: java.lang.Float, row: Int): Unit = {
      if (value != null) {
        vector.vector(row) = value.doubleValue
      } else {
        vector.noNulls = false
        vector.isNull(row) = true
      }
    }
  }

  trait SetVectorDouble extends SetVectorValue[java.lang.Double, DoubleColumnVector] {
    override def setValue(value: java.lang.Double, row: Int): Unit = {
      if (value != null) {
        vector.vector(row) = value.doubleValue
      } else {
        vector.noNulls = false
        vector.isNull(row) = true
      }
    }
  }

  // writes a boolean to a vector
  trait SetVectorBoolean extends SetVectorValue[java.lang.Boolean, LongColumnVector] {
    override def setValue(value: java.lang.Boolean, row: Int): Unit = {
      if (value != null) {
        vector.vector(row) = if (value) { 1L } else { 0L }
      } else {
        vector.noNulls = false
        vector.isNull(row) = true
      }
    }
  }

  // writes a byte array to a vector
  trait SetVectorBytes extends SetVectorValue[Array[Byte], BytesColumnVector] {
    override def setValue(value: Array[Byte], row: Int): Unit = {
      if (value != null) {
        vector.setRef(row, value, 0, value.length)
      } else {
        vector.noNulls = false
        vector.isNull(row) = true
      }
    }
  }

  // writes a UUID to a vector
  trait SetVectorUuid extends SetVectorValue[UUID, BytesColumnVector] {
    override def setValue(value: UUID, row: Int): Unit = {
      if (value != null) {
        val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
        vector.setRef(row, bytes, 0, bytes.length)
      } else {
        vector.noNulls = false
        vector.isNull(row) = true
      }
    }
  }

  /**
    * Gets a writer for setting a value directly into a vector
    *
    * @param binding binding
    * @param vec vector
    * @return
    */
  private def getInnerWriter(binding: ObjectType, vec: ColumnVector): SetVectorValue[AnyRef, ColumnVector] = {
    val writer = binding match {
      case ObjectType.DATE     => new SetVectorDate { override val vector: TimestampColumnVector = vec.asInstanceOf[TimestampColumnVector] }
      case ObjectType.STRING   => new SetVectorString { override val vector: BytesColumnVector = vec.asInstanceOf[BytesColumnVector] }
      case ObjectType.INT      => new SetVectorInt { override val vector: LongColumnVector = vec.asInstanceOf[LongColumnVector] }
      case ObjectType.LONG     => new SetVectorLong { override val vector: LongColumnVector = vec.asInstanceOf[LongColumnVector] }
      case ObjectType.FLOAT    => new SetVectorFloat { override val vector: DoubleColumnVector = vec.asInstanceOf[DoubleColumnVector] }
      case ObjectType.DOUBLE   => new SetVectorDouble { override val vector: DoubleColumnVector = vec.asInstanceOf[DoubleColumnVector] }
      case ObjectType.BOOLEAN  => new SetVectorBoolean { override val vector: LongColumnVector = vec.asInstanceOf[LongColumnVector] }
      case ObjectType.BYTES    => new SetVectorBytes { override val vector: BytesColumnVector = vec.asInstanceOf[BytesColumnVector] }
      case ObjectType.JSON     => new SetVectorString { override val vector: BytesColumnVector = vec.asInstanceOf[BytesColumnVector] }
      case ObjectType.UUID     => new SetVectorUuid { override val vector: BytesColumnVector = vec.asInstanceOf[BytesColumnVector] }
      case _ => throw new IllegalArgumentException(s"Unexpected object type $binding")
    }
    writer.asInstanceOf[SetVectorValue[AnyRef, ColumnVector]]
  }
}
