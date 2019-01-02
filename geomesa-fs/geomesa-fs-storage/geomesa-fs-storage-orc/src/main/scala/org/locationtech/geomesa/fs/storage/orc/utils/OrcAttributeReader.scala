/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc.utils

import java.util.UUID

import org.locationtech.jts.geom._
import org.apache.orc.storage.ql.exec.vector._
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Populates a simple feature from a given Orc row
  */
trait OrcAttributeReader {
  def apply(sf: SimpleFeature, row: Int): Unit
}

object OrcAttributeReader {

  private val gf = JTSFactoryFinder.getGeometryFactory

  /**
    * Create a reader for an ORC batch
    *
    * @param sft simple feature type
    * @param batch row batch
    * @param columns columns to read, corresponding to simple feature attributes
    * @param fid read feature id or not
    * @return
    */
  def apply(sft: SimpleFeatureType,
            batch: VectorizedRowBatch,
            columns: Option[Set[Int]] = None,
            fid: Boolean = true): OrcAttributeReader = {
    val builder = Seq.newBuilder[OrcAttributeReader]
    builder.sizeHint(columns.map(_.size).getOrElse(sft.getAttributeCount) + (if (fid) { 1 } else { 0 }))

    var i = 0
    var col = 0
    while (i < sft.getAttributeCount) {
      val bindings = ObjectType.selectType(sft.getDescriptor(i))
      if (columns.forall(_.contains(i))) {
        val reader = bindings.head match {
          case ObjectType.GEOMETRY => createGeometryReader(bindings(1), batch.cols(col), batch.cols(col + 1), i)
          case ObjectType.DATE     => new DateReader(batch.cols(col).asInstanceOf[TimestampColumnVector], i)
          case ObjectType.STRING   => new StringReader(batch.cols(col).asInstanceOf[BytesColumnVector], i)
          case ObjectType.INT      => new IntReader(batch.cols(col).asInstanceOf[LongColumnVector], i)
          case ObjectType.LONG     => new LongReader(batch.cols(col).asInstanceOf[LongColumnVector], i)
          case ObjectType.FLOAT    => new FloatReader(batch.cols(col).asInstanceOf[DoubleColumnVector], i)
          case ObjectType.DOUBLE   => new DoubleReader(batch.cols(col).asInstanceOf[DoubleColumnVector], i)
          case ObjectType.BOOLEAN  => new BooleanReader(batch.cols(col).asInstanceOf[LongColumnVector], i)
          case ObjectType.BYTES    => new BytesReader(batch.cols(col).asInstanceOf[BytesColumnVector], i)
          case ObjectType.JSON     => new StringReader(batch.cols(col).asInstanceOf[BytesColumnVector], i)
          case ObjectType.UUID     => new UuidReader(batch.cols(col).asInstanceOf[BytesColumnVector], i)
          case ObjectType.LIST     => new ListReader(batch.cols(col).asInstanceOf[ListColumnVector], i, bindings(1))
          case ObjectType.MAP      => new MapReader(batch.cols(col).asInstanceOf[MapColumnVector], i, bindings(1), bindings(2))
          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }
        builder += reader
      }
      i += 1
      if (bindings.head == ObjectType.GEOMETRY) {
        col += 2
      } else {
        col += 1
      }
    }

    if (fid) {
      builder += new FidReader(batch.cols(col).asInstanceOf[BytesColumnVector])
    }

    new SequenceReader(builder.result)
  }

  // noinspection LanguageFeature
  private def createGeometryReader(binding: ObjectType, x: ColumnVector, y: ColumnVector, i: Int): OrcAttributeReader = {
    implicit def toDoubleColumnVector(vec: ColumnVector): DoubleColumnVector = vec.asInstanceOf[DoubleColumnVector]
    implicit def toListColumnVector(vec: ColumnVector): ListColumnVector = vec.asInstanceOf[ListColumnVector]

    binding match {
      case ObjectType.POINT           => new PointReader(x, y, i)
      case ObjectType.LINESTRING      => new LineStringReader(x, y, i)
      case ObjectType.MULTIPOINT      => new MultiPointReader(x, y, i)
      case ObjectType.POLYGON         => new PolygonReader(x, y, i)
      case ObjectType.MULTILINESTRING => new MultiLineStringReader(x, y, i)
      case ObjectType.MULTIPOLYGON    => new MultiPolygonReader(x, y, i)
      case _ => throw new IllegalArgumentException(s"Unexpected object type $binding")
    }
  }

  // invokes a sequence of readers in a single call
  class SequenceReader(readers: Seq[OrcAttributeReader]) extends OrcAttributeReader {
    override def apply(sf: SimpleFeature, row: Int): Unit = readers.foreach(_.apply(sf, row))
  }

  // reads a feature ID from a vector and sets it in a simple feature
  class FidReader(vector: BytesColumnVector) extends OrcAttributeReader {
    override def apply(sf: SimpleFeature, row: Int): Unit =
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(vector.toString(row))
  }

  // reads a date from a vector and sets it in a simple feature
  class DateReader(val vector: TimestampColumnVector, val attribute: Int)
      extends VectorReaderAdapter[TimestampColumnVector] with GetVectorDate

  // reads a string from a vector and sets it in a simple feature
  class StringReader(val vector: BytesColumnVector, val attribute: Int)
      extends VectorReaderAdapter[BytesColumnVector] with GetVectorString

  // reads an int from a vector and sets it in a simple feature
  class IntReader(val vector: LongColumnVector, val attribute: Int)
      extends VectorReaderAdapter[LongColumnVector] with GetVectorInt

  // reads a long from a vector and sets it in a simple feature
  class LongReader(val vector: LongColumnVector, val attribute: Int)
      extends VectorReaderAdapter[LongColumnVector] with GetVectorLong

  // reads a float from a vector and sets it in a simple feature
  class FloatReader(val vector: DoubleColumnVector, val attribute: Int)
      extends VectorReaderAdapter[DoubleColumnVector] with GetVectorFloat

  // reads a double from a vector and sets it in a simple feature
  class DoubleReader(val vector: DoubleColumnVector, val attribute: Int)
      extends VectorReaderAdapter[DoubleColumnVector] with GetVectorDouble

  // reads a boolean from a vector and sets it in a simple feature
  class BooleanReader(val vector: LongColumnVector, val attribute: Int)
      extends VectorReaderAdapter[LongColumnVector] with GetVectorBoolean

  // reads a byte array from a vector and sets it in a simple feature
  class BytesReader(val vector: BytesColumnVector, val attribute: Int)
      extends VectorReaderAdapter[BytesColumnVector] with GetVectorBytes

  // reads a UUID from a vector and sets it in a simple feature
  class UuidReader(val vector: BytesColumnVector, val attribute: Int)
      extends VectorReaderAdapter[BytesColumnVector] with GetVectorUuid

  /**
    * Reads a point attribute from a vector and sets it in a simple feature
    *
    * @param x x coordinates
    * @param y y coordinates
    * @param attribute simple feature attribute index
    */
  class PointReader(x: DoubleColumnVector, y: DoubleColumnVector, attribute: Int) extends OrcAttributeReader {
    override def apply(sf: SimpleFeature, row: Int): Unit = {
      if (x.noNulls || !x.isNull(row)) {
        sf.setAttribute(attribute, gf.createPoint(new Coordinate(x.vector(row), y.vector(row))))
      } else {
        sf.setAttribute(attribute, null)
      }
    }
  }

  /**
    * Reads a linestring attribute from a vector and sets it in a simple feature.
    * A linestring is modeled as a list of points.
    *
    * @see PointReader
    *
    * @param xx outer list vector for x coordinates, containing a double vector for points
    * @param yy outer list vector for y coordinates, containing a double vector for points
    * @param attribute simple feature attribute index
    */
  class LineStringReader(xx: ListColumnVector, yy: ListColumnVector, attribute: Int) extends OrcAttributeReader {
    private val x = xx.child.asInstanceOf[DoubleColumnVector]
    private val y = yy.child.asInstanceOf[DoubleColumnVector]

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      if (xx.noNulls || !xx.isNull(row)) {
        val offset = xx.offsets(row).toInt
        val length = xx.lengths(row).toInt
        val coordinates = Array.ofDim[Coordinate](length)
        var i = 0
        while (i < length) {
          coordinates(i) = new Coordinate(x.vector(offset + i), y.vector(offset + i))
          i += 1
        }
        sf.setAttribute(attribute, gf.createLineString(coordinates))
      } else {
        sf.setAttribute(attribute, null)
      }
    }
  }

  /**
    * Reads a multi-point attribute from a vector and sets it in a simple feature.
    * A multi-point is modeled as a list of points.
    *
    * @see PointReader
    *
    * @param xx outer list vector for x coordinates, containing a double vector for points
    * @param yy outer list vector for y coordinates, containing a double vector for points
    * @param attribute simple feature attribute index
    */
  class MultiPointReader(xx: ListColumnVector, yy: ListColumnVector, attribute: Int) extends OrcAttributeReader {
    private val x = xx.child.asInstanceOf[DoubleColumnVector]
    private val y = yy.child.asInstanceOf[DoubleColumnVector]

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      if (xx.noNulls || !xx.isNull(row)) {
        val offset = xx.offsets(row).toInt
        val length = xx.lengths(row).toInt
        val coordinates = Array.ofDim[Coordinate](length)
        var i = 0
        while (i < length) {
          coordinates(i) = new Coordinate(x.vector(offset + i), y.vector(offset + i))
          i += 1
        }
        sf.setAttribute(attribute, gf.createMultiPoint(coordinates))
      } else {
        sf.setAttribute(attribute, null)
      }
    }
  }

  /**
    * Reads a polygon attribute from a vector and sets it in a simple feature.
    * A polygon is modeled as a list of lines, with the first value being the shell,
    * and any subsequent values being interior holes.
    *
    * @see LineStringReader
    *
    * @param xxx outer list vector for x coordinates, containing a list vector for individual lines
    * @param yyy outer list vector for y coordinates, containing a list vector for individual lines
    * @param attribute simple feature attribute index
    */
  class PolygonReader(xxx: ListColumnVector, yyy: ListColumnVector, attribute: Int) extends OrcAttributeReader {
    private val xx = xxx.child.asInstanceOf[ListColumnVector]
    private val yy = yyy.child.asInstanceOf[ListColumnVector]
    private val x = xx.child.asInstanceOf[DoubleColumnVector]
    private val y = yy.child.asInstanceOf[DoubleColumnVector]

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      if (xxx.noNulls || !xxx.isNull(row)) {
        val lineOffset = xxx.offsets(row).toInt
        val lineLength = xxx.lengths(row).toInt
        val lines = Array.ofDim[LinearRing](lineLength)
        var j = 0
        while (j < lineLength) {
          val offset = xx.offsets(lineOffset + j).toInt
          val length = xx.lengths(lineOffset + j).toInt
          val coordinates = Array.ofDim[Coordinate](length)
          var i = 0
          while (i < length) {
            coordinates(i) = new Coordinate(x.vector(offset + i), y.vector(offset + i))
            i += 1
          }
          lines(j) = gf.createLinearRing(coordinates)
          j += 1
        }
        val polygon = if (lineLength == 1) { gf.createPolygon(lines.head) } else { gf.createPolygon(lines.head, lines.tail) }
        sf.setAttribute(attribute, polygon)
      } else {
        sf.setAttribute(attribute, null)
      }
    }
  }

  /**
    * Reads a multi-linestring attribute from a vector and sets it in a simple feature.
    * A multi-linestring is modeled as a list of lines.
    *
    * @see LineStringReader
    *
    * @param xxx outer list vector for x coordinates, containing a list vector for individual lines
    * @param yyy outer list vector for y coordinates, containing a list vector for individual lines
    * @param attribute simple feature attribute index
    */
  class MultiLineStringReader(xxx: ListColumnVector, yyy: ListColumnVector, attribute: Int) extends OrcAttributeReader {
    private val xx = xxx.child.asInstanceOf[ListColumnVector]
    private val yy = yyy.child.asInstanceOf[ListColumnVector]
    private val x = xx.child.asInstanceOf[DoubleColumnVector]
    private val y = yy.child.asInstanceOf[DoubleColumnVector]

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      if (xxx.noNulls || !xxx.isNull(row)) {
        val lineOffset = xxx.offsets(row).toInt
        val lineLength = xxx.lengths(row).toInt
        val lines = Array.ofDim[LineString](lineLength)
        var j = 0
        while (j < lineLength) {
          val offset = xx.offsets(lineOffset + j).toInt
          val length = xx.lengths(lineOffset + j).toInt
          val coordinates = Array.ofDim[Coordinate](length)
          var i = 0
          while (i < length) {
            coordinates(i) = new Coordinate(x.vector(offset + i), y.vector(offset + i))
            i += 1
          }
          lines(j) = gf.createLineString(coordinates)
          j += 1
        }
        sf.setAttribute(attribute, gf.createMultiLineString(lines))
      } else {
        sf.setAttribute(attribute, null)
      }
    }
  }

  /**
    * Reads a multi-polygon attribute from a vector and sets it in a simple feature.
    * A multi-polygon is modeled as a list of polygons.
    *
    * @see PolygonReader
    *
    * @param xxxx outer list vector for x coordinates, containing a list vector for individual polygons
    * @param yyyy outer list vector for y coordinates, containing a list vector for individual polygons
    * @param attribute simple feature attribute index
    */
  class MultiPolygonReader(xxxx: ListColumnVector, yyyy: ListColumnVector, attribute: Int) extends OrcAttributeReader {
    private val xxx = xxxx.child.asInstanceOf[ListColumnVector]
    private val yyy = yyyy.child.asInstanceOf[ListColumnVector]
    private val xx = xxx.child.asInstanceOf[ListColumnVector]
    private val yy = yyy.child.asInstanceOf[ListColumnVector]
    private val x = xx.child.asInstanceOf[DoubleColumnVector]
    private val y = yy.child.asInstanceOf[DoubleColumnVector]

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      if (xxxx.noNulls || !xxxx.isNull(row)) {
        val polygonOffset = xxxx.offsets(row).toInt
        val polygonLength = xxxx.lengths(row).toInt
        val polygons = Array.ofDim[Polygon](polygonLength)
        var k = 0
        while (k < polygonLength) {
          val lineOffset = xxx.offsets(polygonOffset + k).toInt
          val lineLength = xxx.lengths(polygonOffset + k).toInt
          val lines = Array.ofDim[LinearRing](lineLength)
          var j = 0
          while (j < lineLength) {
            val offset = xx.offsets(lineOffset + j).toInt
            val length = xx.lengths(lineOffset + j).toInt
            val coordinates = Array.ofDim[Coordinate](length)
            var i = 0
            while (i < length) {
              coordinates(i) = new Coordinate(x.vector(offset + i), y.vector(offset + i))
              i += 1
            }
            lines(j) = gf.createLinearRing(coordinates)
            j += 1
          }
          polygons(k) = if (lineLength == 1) { gf.createPolygon(lines.head) } else { gf.createPolygon(lines.head, lines.tail)}
          k += 1
        }
        sf.setAttribute(attribute, gf.createMultiPolygon(polygons))
      } else {
        sf.setAttribute(attribute, null)
      }
    }
  }

  /**
    * Reads a java.util.List attribute from a vector
    *
    * @param vector vector
    * @param attribute simple feature attribute index
    * @param binding list value type
    */
  class ListReader(vector: ListColumnVector, attribute: Int, binding: ObjectType) extends OrcAttributeReader {

    private val reader = getInnerReader(binding, vector.child)

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      if (vector.noNulls || !vector.isNull(row)) {
        val offset = vector.offsets(row).toInt
        val length = vector.lengths(row).toInt
        val value = new java.util.ArrayList[AnyRef](length)
        var i = offset
        while (i < offset + length) {
          value.add(reader.getValue(i))
          i += 1
        }
        sf.setAttribute(attribute, value)
      } else {
        sf.setAttribute(attribute, null)
      }
    }
  }

  /**
    * Reads a java.util.Map attribute from a vector
    *
    * @param vector vector
    * @param attribute simple feature attribute index
    * @param keyBinding map key type
    * @param valueBinding map value type
    */
  class MapReader(vector: MapColumnVector, attribute: Int, keyBinding: ObjectType, valueBinding: ObjectType)
      extends OrcAttributeReader {

    private val keyReader = getInnerReader(keyBinding, vector.keys)
    private val valueReader = getInnerReader(valueBinding, vector.values)

    override def apply(sf: SimpleFeature, row: Int): Unit = {
      if (vector.noNulls || !vector.isNull(row)) {
        val offset = vector.offsets(row).toInt
        val length = vector.lengths(row).toInt
        val value = new java.util.HashMap[AnyRef, AnyRef](length)
        var i = 0
        while (i < length) {
          value.put(keyReader.getValue(i + offset), valueReader.getValue(i + offset))
          i += 1
        }
        sf.setAttribute(attribute, value)
      } else {
        sf.setAttribute(attribute, null)
      }
    }
  }

  // reads a value out of a typed vector
  trait GetVectorValue[T <: ColumnVector] {
    protected val vector: T
    def getValue(row: Int): AnyRef
  }

  // reads a value out of a vector and sets it into a simple feature
  trait VectorReaderAdapter[T <: ColumnVector] extends OrcAttributeReader with GetVectorValue[T]{
    def attribute: Int
    override def apply(sf: SimpleFeature, row: Int): Unit = sf.setAttribute(attribute, getValue(row))
  }

  // reads a date from a vector
  trait GetVectorDate extends GetVectorValue[TimestampColumnVector] {
    override def getValue(row: Int): AnyRef = {
      if (vector.noNulls || !vector.isNull(row)) {
        new java.util.Date(vector.time(row))
      } else {
        null
      }
    }
  }

  // reads a string from a vector
  trait GetVectorString extends GetVectorValue[BytesColumnVector] {
    override def getValue(row: Int): AnyRef = {
      if (vector.noNulls || !vector.isNull(row)) {
        vector.toString(row)
      } else {
        null
      }
    }
  }

  // reads an int from a vector
  trait GetVectorInt extends GetVectorValue[LongColumnVector] {
    override def getValue(row: Int): AnyRef = {
      if (vector.noNulls || !vector.isNull(row)) {
        Int.box(vector.vector(row).toInt)
      } else {
        null
      }
    }
  }

  // reads a long from a vector
  trait GetVectorLong extends GetVectorValue[LongColumnVector] {
    override def getValue(row: Int): AnyRef = {
      if (vector.noNulls || !vector.isNull(row)) {
        Long.box(vector.vector(row))
      } else {
        null
      }
    }
  }

  // reads a float from a vector
  trait GetVectorFloat extends GetVectorValue[DoubleColumnVector] {
    override def getValue(row: Int): AnyRef = {
      if (vector.noNulls || !vector.isNull(row)) {
        Float.box(vector.vector(row).toFloat)
      } else {
        null
      }
    }
  }

  // reads a double from a vector
  trait GetVectorDouble extends GetVectorValue[DoubleColumnVector] {
    override def getValue(row: Int): AnyRef = {
      if (vector.noNulls || !vector.isNull(row)) {
        Double.box(vector.vector(row))
      } else {
        null
      }
    }
  }

  // reads a boolean from a vector
  trait GetVectorBoolean extends GetVectorValue[LongColumnVector] {
    override def getValue(row: Int): AnyRef = {
      if (vector.noNulls || !vector.isNull(row)) {
        Boolean.box(vector.vector(row) > 0L)
      } else {
        null
      }
    }
  }

  // reads a byte array from a vector
  trait GetVectorBytes extends GetVectorValue[BytesColumnVector] {
    override def getValue(row: Int): AnyRef = {
      if (vector.noNulls || !vector.isNull(row)) {
        var bytes = vector.vector(row)
        if (vector.start(row) != 0 || vector.length(row) != bytes.length) {
          val tmp = Array.ofDim[Byte](vector.length(row))
          System.arraycopy(bytes, vector.start(row), tmp, 0, tmp.length)
          bytes = tmp
        }
        bytes
      } else {
        null
      }
    }
  }

  // reads a UUID from a vector
  trait GetVectorUuid extends GetVectorValue[BytesColumnVector] {
    override def getValue(row: Int): AnyRef = {
      if (vector.noNulls || !vector.isNull(row)) {
        UUID.fromString(vector.toString(row))
      } else {
        null
      }
    }
  }

  /**
    * Gets a reader for getting a value directly out of a vector
    *
    * @param binding binding
    * @param vec vector
    * @return
    */
  private def getInnerReader(binding: ObjectType, vec: ColumnVector): GetVectorValue[ColumnVector] = {
    val reader = binding match {
      case ObjectType.DATE     => new GetVectorDate { override val vector: TimestampColumnVector = vec.asInstanceOf[TimestampColumnVector] }
      case ObjectType.STRING   => new GetVectorString { override val vector: BytesColumnVector = vec.asInstanceOf[BytesColumnVector] }
      case ObjectType.INT      => new GetVectorInt { override val vector: LongColumnVector = vec.asInstanceOf[LongColumnVector] }
      case ObjectType.LONG     => new GetVectorLong { override val vector: LongColumnVector = vec.asInstanceOf[LongColumnVector] }
      case ObjectType.FLOAT    => new GetVectorFloat { override val vector: DoubleColumnVector = vec.asInstanceOf[DoubleColumnVector] }
      case ObjectType.DOUBLE   => new GetVectorDouble { override val vector: DoubleColumnVector = vec.asInstanceOf[DoubleColumnVector] }
      case ObjectType.BOOLEAN  => new GetVectorBoolean { override val vector: LongColumnVector = vec.asInstanceOf[LongColumnVector] }
      case ObjectType.BYTES    => new GetVectorBytes { override val vector: BytesColumnVector = vec.asInstanceOf[BytesColumnVector] }
      case ObjectType.JSON     => new GetVectorString { override val vector: BytesColumnVector = vec.asInstanceOf[BytesColumnVector] }
      case ObjectType.UUID     => new GetVectorUuid { override val vector: BytesColumnVector = vec.asInstanceOf[BytesColumnVector] }
      case _ => throw new IllegalArgumentException(s"Unexpected object type $binding")
    }
    reader.asInstanceOf[GetVectorValue[ColumnVector]]
  }
}
