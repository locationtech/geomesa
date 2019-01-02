/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc.utils

import java.util.UUID

import org.locationtech.jts.geom.{Coordinate, LineString, LinearRing, Polygon}
import org.apache.hadoop.io._
import org.apache.orc.mapred.{OrcList, OrcMap, OrcStruct, OrcTimestamp}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait OrcInputFormatReader {
  def apply(input: OrcStruct, sf: SimpleFeature): Unit
}

object OrcInputFormatReader {

  private val gf = JTSFactoryFinder.getGeometryFactory

  def apply(sft: SimpleFeatureType, columns: Option[Set[Int]] = None, fid: Boolean = true): OrcInputFormatReader = {
    val builder = Seq.newBuilder[OrcInputFormatReader]
    builder.sizeHint(columns.map(_.size).getOrElse(sft.getAttributeCount) + (if (fid) { 1 } else { 0 }))

    var i = 0
    var col = 0
    while (i < sft.getAttributeCount) {
      val bindings = ObjectType.selectType(sft.getDescriptor(i))
      if (columns.forall(_.contains(i))) {
        val reader = bindings.head match {
          case ObjectType.GEOMETRY => col += 1; createGeometryReader(bindings(1), col - 1, col, i)
          case ObjectType.DATE     => new DateInputFormatReader(col, i)
          case ObjectType.STRING   => new StringInputFormatReader(col, i)
          case ObjectType.INT      => new IntInputFormatReader(col, i)
          case ObjectType.LONG     => new LongInputFormatReader(col, i)
          case ObjectType.FLOAT    => new FloatInputFormatReader(col, i)
          case ObjectType.DOUBLE   => new DoubleInputFormatReader(col, i)
          case ObjectType.BOOLEAN  => new BooleanInputFormatReader(col, i)
          case ObjectType.BYTES    => new BytesInputFormatReader(col, i)
          case ObjectType.JSON     => new StringInputFormatReader(col, i)
          case ObjectType.UUID     => new UuidInputFormatReader(col, i)
          case ObjectType.LIST     => new ListInputFormatReader(col, i, bindings(1))
          case ObjectType.MAP      => new MapInputFormatReader(col, i, bindings(1), bindings(2))
          case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
        }
        builder += reader
        col += 1
      } else {
        bindings.head match {
          case ObjectType.GEOMETRY => col += 2
          case _                   => col += 1
        }
      }
      i += 1
    }

    if (fid) {
      builder += new FidInputFormatReader(col)
    }

    new SequenceInputFormatReader(builder.result)
  }

  private def createGeometryReader(binding: ObjectType, x: Int, y: Int, i: Int): OrcInputFormatReader = {
    binding match {
      case ObjectType.POINT           => new PointInputFormatReader(x, y, i)
      case ObjectType.LINESTRING      => new LineStringInputFormatReader(x, y, i)
      case ObjectType.MULTIPOINT      => new MultiPointInputFormatReader(x, y, i)
      case ObjectType.POLYGON         => new PolygonInputFormatReader(x, y, i)
      case ObjectType.MULTILINESTRING => new MultiLineStringInputFormatReader(x, y, i)
      case ObjectType.MULTIPOLYGON    => new MultiPolygonInputFormatReader(x, y, i)
      case _ => throw new IllegalArgumentException(s"Unexpected geometry type $binding")
    }
  }

  class SequenceInputFormatReader(readers: Seq[OrcInputFormatReader]) extends OrcInputFormatReader {
    override def apply(input: OrcStruct, sf: SimpleFeature): Unit = readers.foreach(_.apply(input, sf))
  }

  class FidInputFormatReader(col: Int) extends OrcInputFormatReader with ConvertInputFormatString {
    private var counter = -1L
    override def apply(input: OrcStruct, sf: SimpleFeature): Unit = {
      val field = input.getFieldValue(col)
      if (field == null) {
        counter += 1
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(s"$counter")
      } else {
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(convert(field))
      }
    }

  }

  class DateInputFormatReader(val col: Int, val attribute: Int)
      extends InputFormatReaderAdapter with ConvertInputFormatDate

  class StringInputFormatReader(val col: Int, val attribute: Int)
      extends InputFormatReaderAdapter with ConvertInputFormatString

  class IntInputFormatReader(val col: Int, val attribute: Int)
      extends InputFormatReaderAdapter with ConvertInputFormatInt

  class LongInputFormatReader(val col: Int, val attribute: Int)
      extends InputFormatReaderAdapter with ConvertInputFormatLong

  class FloatInputFormatReader(val col: Int, val attribute: Int)
      extends InputFormatReaderAdapter with ConvertInputFormatFloat

  class DoubleInputFormatReader(val col: Int, val attribute: Int)
      extends InputFormatReaderAdapter with ConvertInputFormatDouble

  class BooleanInputFormatReader(val col: Int, val attribute: Int)
      extends InputFormatReaderAdapter with ConvertInputFormatBoolean

  class BytesInputFormatReader(val col: Int, val attribute: Int)
      extends InputFormatReaderAdapter with ConvertInputFormatBytes

  class UuidInputFormatReader(val col: Int, val attribute: Int)
      extends InputFormatReaderAdapter with ConvertInputFormatUuid

  /**
    * Reads a point attribute from a vector and sets it in a simple feature
    *
    * @param xCol index of x field, containing points
    * @param yCol index of y field, containing points
    * @param attribute simple feature attribute index
    */
  class PointInputFormatReader(xCol: Int, yCol: Int, attribute: Int)
      extends OrcInputFormatReader with ConvertInputFormatDouble {
    override def apply(input: OrcStruct, sf: SimpleFeature): Unit = {
      val x = input.getFieldValue(xCol)
      val y = input.getFieldValue(yCol)
      if (x == null || y == null) {
        sf.setAttribute(attribute, null)
      } else {
        sf.setAttribute(attribute, gf.createPoint(new Coordinate(convertUnboxed(x), convertUnboxed(y))))
      }
    }
  }

  /**
    * Reads a linestring attribute from a vector and sets it in a simple feature.
    * A linestring is modeled as a list of points.
    *
    * @see PointReader
    *
    * @param xCol index of x field, containing a list of points
    * @param yCol index of y field, containing a list of points
    * @param attribute simple feature attribute index
    */
  class LineStringInputFormatReader(xCol: Int, yCol: Int, attribute: Int)
      extends OrcInputFormatReader with ConvertInputFormatDouble {
    override def apply(input: OrcStruct, sf: SimpleFeature): Unit = {
      val xList = input.getFieldValue(xCol).asInstanceOf[OrcList[_ <: WritableComparable[_]]]
      val yList = input.getFieldValue(yCol).asInstanceOf[OrcList[_ <: WritableComparable[_]]]
      if (xList == null || yList == null) {
        sf.setAttribute(attribute, null)
      } else {
        val coordinates = Array.ofDim[Coordinate](xList.size())
        var i = 0
        while (i < coordinates.length) {
          coordinates(i) = new Coordinate(convertUnboxed(xList.get(i)), convertUnboxed(yList.get(i)))
          i += 1
        }
        sf.setAttribute(attribute, gf.createLineString(coordinates))
      }
    }
  }

  /**
    * Reads a multi-point attribute from a vector and sets it in a simple feature.
    * A multi-point is modeled as a list of points.
    *
    * @see PointReader
    *
    * @param xCol index of x field, containing a list of points
    * @param yCol index of y field, containing a list of points
    * @param attribute simple feature attribute index
    */
  class MultiPointInputFormatReader(xCol: Int, yCol: Int, attribute: Int)
      extends OrcInputFormatReader with ConvertInputFormatDouble {
    override def apply(input: OrcStruct, sf: SimpleFeature): Unit = {
      val xList = input.getFieldValue(xCol).asInstanceOf[OrcList[_ <: WritableComparable[_]]]
      val yList = input.getFieldValue(yCol).asInstanceOf[OrcList[_ <: WritableComparable[_]]]
      if (xList == null || yList == null) {
        sf.setAttribute(attribute, null)
      } else {
        val coordinates = Array.ofDim[Coordinate](xList.size())
        var i = 0
        while (i < coordinates.length) {
          coordinates(i) = new Coordinate(convertUnboxed(xList.get(i)), convertUnboxed(yList.get(i)))
          i += 1
        }
        sf.setAttribute(attribute, gf.createMultiPoint(coordinates))
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
    * @param xCol index of x field, containing a list of lists of points
    * @param yCol index of y field, containing a list of lists of points
    * @param attribute simple feature attribute index
    */
  class PolygonInputFormatReader(xCol: Int, yCol: Int, attribute: Int)
      extends OrcInputFormatReader with ConvertInputFormatDouble {
    override def apply(input: OrcStruct, sf: SimpleFeature): Unit = {
      val xxList = input.getFieldValue(xCol).asInstanceOf[OrcList[OrcList[_ <: WritableComparable[_]]]]
      val yyList = input.getFieldValue(yCol).asInstanceOf[OrcList[OrcList[_ <: WritableComparable[_]]]]
      if (xxList == null || yyList == null) {
        sf.setAttribute(attribute, null)
      } else {
        val lines = Array.ofDim[LinearRing](xxList.size)
        var j = 0
        while (j < lines.length) {
          val xList = xxList.get(j)
          val yList = yyList.get(j)
          val coordinates = Array.ofDim[Coordinate](xList.size())
          var i = 0
          while (i < coordinates.length) {
            coordinates(i) = new Coordinate(convertUnboxed(xList.get(i)), convertUnboxed(yList.get(i)))
            i += 1
          }
          lines(j) = gf.createLinearRing(coordinates)
          j += 1
        }
        val polygon = if (lines.length == 1) { gf.createPolygon(lines.head) } else { gf.createPolygon(lines.head, lines.tail) }
        sf.setAttribute(attribute, polygon)
      }
    }
  }

  /**
    * Reads a multi-linestring attribute from a vector and sets it in a simple feature.
    * A multi-linestring is modeled as a list of lines.
    *
    * @see LineStringReader
    *
    * @param xCol index of x field, containing a list of lists of points
    * @param yCol index of y field, containing a list of lists of points
    * @param attribute simple feature attribute index
    */
  class MultiLineStringInputFormatReader(xCol: Int, yCol: Int, attribute: Int)
      extends OrcInputFormatReader with ConvertInputFormatDouble {
    override def apply(input: OrcStruct, sf: SimpleFeature): Unit = {
      val xxList = input.getFieldValue(xCol).asInstanceOf[OrcList[OrcList[_ <: WritableComparable[_]]]]
      val yyList = input.getFieldValue(yCol).asInstanceOf[OrcList[OrcList[_ <: WritableComparable[_]]]]
      if (xxList == null || yyList == null) {
        sf.setAttribute(attribute, null)
      } else {
        val lines = Array.ofDim[LineString](xxList.size)
        var j = 0
        while (j < lines.length) {
          val xList = xxList.get(j)
          val yList = yyList.get(j)
          val coordinates = Array.ofDim[Coordinate](xList.size())
          var i = 0
          while (i < coordinates.length) {
            coordinates(i) = new Coordinate(convertUnboxed(xList.get(i)), convertUnboxed(yList.get(i)))
            i += 1
          }
          lines(j) = gf.createLineString(coordinates)
          j += 1
        }
        sf.setAttribute(attribute, gf.createMultiLineString(lines))
      }
    }
  }

  /**
    * Reads a multi-polygon attribute from a vector and sets it in a simple feature.
    * A multi-polygon is modeled as a list of polygons.
    *
    * @see PolygonReader
    *
    * @param xCol index of x field, containing a list of lists of lists of points
    * @param yCol index of y field, containing a list of lists of lists of points
    * @param attribute simple feature attribute index
    */
  class MultiPolygonInputFormatReader(xCol: Int, yCol: Int, attribute: Int)
      extends OrcInputFormatReader with ConvertInputFormatDouble {
    override def apply(input: OrcStruct, sf: SimpleFeature): Unit = {
      val xxxList = input.getFieldValue(xCol).asInstanceOf[OrcList[OrcList[OrcList[_ <: WritableComparable[_]]]]]
      val yyyList = input.getFieldValue(yCol).asInstanceOf[OrcList[OrcList[OrcList[_ <: WritableComparable[_]]]]]
      if (xxxList == null || yyyList == null) {
        sf.setAttribute(attribute, null)
      } else {
        val polygons = Array.ofDim[Polygon](xxxList.size)
        var k = 0
        while (k < polygons.length) {
          val xxList = xxxList.get(k)
          val yyList = yyyList.get(k)
          val lines = Array.ofDim[LinearRing](xxList.size)
          var j = 0
          while (j < lines.length) {
            val xList = xxList.get(j)
            val yList = yyList.get(j)
            val coordinates = Array.ofDim[Coordinate](xList.size())
            var i = 0
            while (i < coordinates.length) {
              coordinates(i) = new Coordinate(convertUnboxed(xList.get(i)), convertUnboxed(yList.get(i)))
              i += 1
            }
            lines(j) = gf.createLinearRing(coordinates)
            j += 1
          }
          polygons(k) = if (lines.size == 1) {
            gf.createPolygon(lines(0))
          } else {
            gf.createPolygon(lines(0), lines.tail)
          }
          k += 1
        }
        sf.setAttribute(attribute, gf.createMultiPolygon(polygons))
      }
    }
  }

  class ListInputFormatReader(col: Int, attribute: Int, binding: ObjectType) extends OrcInputFormatReader {
    private val converter = getConverter(binding)

    override def apply(input: OrcStruct, sf: SimpleFeature): Unit = {
      import scala.collection.JavaConversions._
      val value = input.getFieldValue(attribute).asInstanceOf[OrcList[_ <: WritableComparable[_]]]
      if (value == null) {
        sf.setAttribute(attribute, null)
      } else {
        val list = new java.util.ArrayList[AnyRef](value.size())
        value.foreach(element => list.add(converter.convert(element)))
        sf.setAttribute(attribute, list)
      }
    }
  }

  class MapInputFormatReader(col: Int, attribute: Int, keyBinding: ObjectType, valueBinding: ObjectType)
      extends OrcInputFormatReader {
    private val keyConverter = getConverter(keyBinding)
    private val valueConverter = getConverter(valueBinding)

    override def apply(input: OrcStruct, sf: SimpleFeature): Unit = {
      import scala.collection.JavaConversions._
      val value = input.getFieldValue(attribute).asInstanceOf[OrcMap[_ <: WritableComparable[_], _ <: WritableComparable[_]]]
      if (value == null) {
        sf.setAttribute(attribute, null)
      } else {
        val map = new java.util.HashMap[AnyRef, AnyRef](value.size())
        value.foreach { case (k, v) => map.put(keyConverter.convert(k), valueConverter.convert(v)) }
        sf.setAttribute(attribute, map)
      }
    }
  }

  trait ConvertInputFormatValue {
    def convert(input: AnyRef): AnyRef
  }

  trait InputFormatReaderAdapter extends OrcInputFormatReader with ConvertInputFormatValue {
    def col: Int
    def attribute: Int
    override def apply(input: OrcStruct, sf: SimpleFeature): Unit = {
      val field = input.getFieldValue(col)
      if (field == null) {
        sf.setAttribute(attribute, null)
      } else {
        sf.setAttribute(attribute, convert(field))
      }
    }
  }

  trait ConvertInputFormatDate extends ConvertInputFormatValue {
    override def convert(input: AnyRef): java.util.Date = new java.util.Date(input.asInstanceOf[OrcTimestamp].getTime)
  }

  trait ConvertInputFormatString extends ConvertInputFormatValue {
    override def convert(input: AnyRef): String = input.asInstanceOf[Text].toString
  }

  trait ConvertInputFormatInt extends ConvertInputFormatValue {
    override def convert(input: AnyRef): java.lang.Integer = Int.box(input.asInstanceOf[IntWritable].get())
  }

  trait ConvertInputFormatLong extends ConvertInputFormatValue {
    override def convert(input: AnyRef): java.lang.Long = Long.box(input.asInstanceOf[LongWritable].get())
  }

  trait ConvertInputFormatFloat extends ConvertInputFormatValue {
    override def convert(input: AnyRef): java.lang.Float = Float.box(input.asInstanceOf[FloatWritable].get())
  }

  trait ConvertInputFormatDouble extends ConvertInputFormatValue {
    override def convert(input: AnyRef): java.lang.Double = Double.box(input.asInstanceOf[DoubleWritable].get())
    def convertUnboxed(input: AnyRef): Double = input.asInstanceOf[DoubleWritable].get()
  }

  trait ConvertInputFormatBoolean extends ConvertInputFormatValue {
    override def convert(input: AnyRef): java.lang.Boolean = Boolean.box(input.asInstanceOf[BooleanWritable].get())
  }

  trait ConvertInputFormatBytes extends ConvertInputFormatValue {
    override def convert(input: AnyRef): Array[Byte] = input.asInstanceOf[BytesWritable].copyBytes()
  }

  trait ConvertInputFormatUuid extends ConvertInputFormatValue {
    override def convert(input: AnyRef): UUID = UUID.fromString(input.asInstanceOf[Text].toString)
  }

  /**
    * Gets a converter instance
    *
    * @param binding binding
    * @return
    */
  private def getConverter(binding: ObjectType): ConvertInputFormatValue = {
    binding match {
      case ObjectType.DATE     => new ConvertInputFormatDate {}
      case ObjectType.STRING   => new ConvertInputFormatString {}
      case ObjectType.INT      => new ConvertInputFormatInt {}
      case ObjectType.LONG     => new ConvertInputFormatLong {}
      case ObjectType.FLOAT    => new ConvertInputFormatFloat {}
      case ObjectType.DOUBLE   => new ConvertInputFormatDouble {}
      case ObjectType.BOOLEAN  => new ConvertInputFormatBoolean {}
      case ObjectType.BYTES    => new ConvertInputFormatBytes {}
      case ObjectType.JSON     => new ConvertInputFormatString {}
      case ObjectType.UUID     => new ConvertInputFormatUuid {}
      case _ => throw new IllegalArgumentException(s"Unexpected object type $binding")
    }
  }
}
