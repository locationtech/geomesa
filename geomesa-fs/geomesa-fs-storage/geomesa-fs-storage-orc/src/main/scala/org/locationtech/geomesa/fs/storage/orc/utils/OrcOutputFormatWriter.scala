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
import org.apache.hadoop.io._
import org.apache.orc.TypeDescription
import org.apache.orc.mapred.{OrcList, OrcMap, OrcStruct, OrcTimestamp}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait OrcOutputFormatWriter {
  def apply(sf: SimpleFeature, output: OrcStruct): Unit
}

object OrcOutputFormatWriter {

  def apply(sft: SimpleFeatureType, description: TypeDescription, fid: Boolean = true): OrcOutputFormatWriter = {
    val builder = Seq.newBuilder[OrcOutputFormatWriter]
    builder.sizeHint(sft.getAttributeCount + (if (fid) { 1 } else { 0 }))

    var i = 0
    var col = 0
    while (i < sft.getAttributeCount) {
      val bindings = ObjectType.selectType(sft.getDescriptor(i))
      val reader = bindings.head match {
        case ObjectType.GEOMETRY => col += 1; createGeometryWriter(bindings(1), col - 1, col, i, description)
        case ObjectType.DATE     => new DateOutputFormatWriter(col, i)
        case ObjectType.STRING   => new StringOutputFormatWriter(col, i)
        case ObjectType.INT      => new IntOutputFormatWriter(col, i)
        case ObjectType.LONG     => new LongOutputFormatWriter(col, i)
        case ObjectType.FLOAT    => new FloatOutputFormatWriter(col, i)
        case ObjectType.DOUBLE   => new DoubleOutputFormatWriter(col, i)
        case ObjectType.BOOLEAN  => new BooleanOutputFormatWriter(col, i)
        case ObjectType.BYTES    => new BytesOutputFormatWriter(col, i)
        case ObjectType.JSON     => new StringOutputFormatWriter(col, i)
        case ObjectType.UUID     => new UuidOutputFormatWriter(col, i)
        case ObjectType.LIST     => new ListOutputFormatWriter(col, i, bindings(1), description)
        case ObjectType.MAP      => new MapOutputFormatWriter(col, i, bindings(1), bindings(2), description)
        case _ => throw new IllegalArgumentException(s"Unexpected object type ${bindings.head}")
      }
      builder += reader
      i += 1
      col += 1
    }

    if (fid) {
      builder += new FidOutputFormatWriter(col)
    }

    new SequenceOutputFormatWriter(builder.result)
  }

  private def createGeometryWriter(binding: ObjectType,
                                   x: Int, y: Int, i: Int,
                                   description: TypeDescription): OrcOutputFormatWriter = {
    binding match {
      case ObjectType.POINT           => new PointOutputFormatWriter(x, y, i)
      case ObjectType.LINESTRING      => new LineStringOutputFormatWriter(x, y, i, description)
      case ObjectType.MULTIPOINT      => new MultiPointOutputFormatWriter(x, y, i, description)
      case ObjectType.POLYGON         => new PolygonOutputFormatWriter(x, y, i, description)
      case ObjectType.MULTILINESTRING => new MultiLineStringOutputFormatWriter(x, y, i, description)
      case ObjectType.MULTIPOLYGON    => new MultiPolygonOutputFormatWriter(x, y, i, description)
      case _ => throw new IllegalArgumentException(s"Unexpected geometry type $binding")
    }
  }

  class SequenceOutputFormatWriter(writers: Seq[OrcOutputFormatWriter]) extends OrcOutputFormatWriter {
    override def apply(sf: SimpleFeature, output: OrcStruct): Unit = writers.foreach(_.apply(sf, output))
  }

  class FidOutputFormatWriter(col: Int) extends OrcOutputFormatWriter with SetOutputFormatString {
    override def apply(sf: SimpleFeature, output: OrcStruct): Unit =
      output.setFieldValue(col, setValue(sf.getID, output.getFieldValue(col)))
  }

  class DateOutputFormatWriter(val col: Int, val attribute: Int)
      extends OutputFormatWriterAdapter with SetOutputFormatDate

  class StringOutputFormatWriter(val col: Int, val attribute: Int)
      extends OutputFormatWriterAdapter with SetOutputFormatString

  class IntOutputFormatWriter(val col: Int, val attribute: Int)
      extends OutputFormatWriterAdapter with SetOutputFormatInt

  class LongOutputFormatWriter(val col: Int, val attribute: Int)
      extends OutputFormatWriterAdapter with SetOutputFormatLong

  class FloatOutputFormatWriter(val col: Int, val attribute: Int)
      extends OutputFormatWriterAdapter with SetOutputFormatFloat

  class DoubleOutputFormatWriter(val col: Int, val attribute: Int)
      extends OutputFormatWriterAdapter with SetOutputFormatDouble

  class BooleanOutputFormatWriter(val col: Int, val attribute: Int)
      extends OutputFormatWriterAdapter with SetOutputFormatBoolean

  class BytesOutputFormatWriter(val col: Int, val attribute: Int)
      extends OutputFormatWriterAdapter with SetOutputFormatBytes

  class UuidOutputFormatWriter(val col: Int, val attribute: Int)
      extends OutputFormatWriterAdapter with SetOutputFormatUuid

  /**
    * Reads a point attribute from a simple feature and sets it in an output format
    *
    * @param xCol index of x field, containing points
    * @param yCol index of y field, containing points
    * @param attribute simple feature attribute index
    */
  class PointOutputFormatWriter(xCol: Int, yCol: Int, attribute: Int)
      extends OrcOutputFormatWriter with SetOutputFormatDouble {
    override def apply(sf: SimpleFeature, output: OrcStruct): Unit = {
      val point = sf.getAttribute(attribute).asInstanceOf[Point]
      if (point == null) {
        output.setFieldValue(xCol, null)
        output.setFieldValue(yCol, null)
      } else {
        var x = output.getFieldValue(xCol).asInstanceOf[DoubleWritable]
        var y = output.getFieldValue(yCol).asInstanceOf[DoubleWritable]
        if (x == null) {
          x = new DoubleWritable
          output.setFieldValue(xCol, x)
        }
        if (y == null) {
          y = new DoubleWritable
          output.setFieldValue(yCol, y)
        }
        x.set(point.getX)
        y.set(point.getY)
      }
    }
  }

  /**
    * Reads a linestring attribute from a simple feature and sets it in an output format.
    * A linestring is modeled as a list of points.
    *
    * @see PointOutputFormatWriter
    *
    * @param xCol index of x field, containing a list of points
    * @param yCol index of y field, containing a list of points
    * @param attribute simple feature attribute index
    */
  class LineStringOutputFormatWriter(xCol: Int, yCol: Int, attribute: Int, description: TypeDescription)
      extends OrcOutputFormatWriter with SetOutputFormatDouble {
    override def apply(sf: SimpleFeature, output: OrcStruct): Unit = {
      val line = sf.getAttribute(attribute).asInstanceOf[LineString]
      if (line == null) {
        output.setFieldValue(xCol, null)
        output.setFieldValue(yCol, null)
      } else {
        var x = output.getFieldValue(xCol).asInstanceOf[OrcList[DoubleWritable]]
        var y = output.getFieldValue(yCol).asInstanceOf[OrcList[DoubleWritable]]
        if (x == null) {
          x = new OrcList[DoubleWritable](description.getChildren.get(xCol), line.getNumPoints)
          output.setFieldValue(xCol, x)
        } else {
          x.clear()
        }
        if (y == null) {
          y = new OrcList[DoubleWritable](description.getChildren.get(yCol), line.getNumPoints)
          output.setFieldValue(yCol, y)
        } else {
          y.clear()
        }
        var i = 0
        while (i < line.getNumPoints) {
          val pt = line.getCoordinateN(i)
          x.add(new DoubleWritable(pt.x))
          y.add(new DoubleWritable(pt.y))
          i += 1
        }
      }
    }
  }

  /**
    * Reads a multi-point attribute from a simple feature and sets it in an output format.
    * A multi-point is modeled as a list of points.
    *
    * @see PointOutputFormatWriter
    *
    * @param xCol index of x field, containing a list of points
    * @param yCol index of y field, containing a list of points
    * @param attribute simple feature attribute index
    */
  class MultiPointOutputFormatWriter(xCol: Int, yCol: Int, attribute: Int, description: TypeDescription)
      extends OrcOutputFormatWriter with SetOutputFormatDouble {
    override def apply(sf: SimpleFeature, output: OrcStruct): Unit = {
      val multiPoint = sf.getAttribute(attribute).asInstanceOf[MultiPoint]
      if (multiPoint == null) {
        output.setFieldValue(xCol, null)
        output.setFieldValue(yCol, null)
      } else {
        var x = output.getFieldValue(xCol).asInstanceOf[OrcList[DoubleWritable]]
        var y = output.getFieldValue(yCol).asInstanceOf[OrcList[DoubleWritable]]
        if (x == null) {
          x = new OrcList[DoubleWritable](description.getChildren.get(xCol), multiPoint.getNumPoints)
          output.setFieldValue(xCol, x)
        } else {
          x.clear()
        }
        if (y == null) {
          y = new OrcList[DoubleWritable](description.getChildren.get(yCol), multiPoint.getNumPoints)
          output.setFieldValue(yCol, y)
        } else {
          y.clear()
        }
        var i = 0
        while (i < multiPoint.getNumPoints) {
          val pt = multiPoint.getGeometryN(i).asInstanceOf[Point]
          x.add(new DoubleWritable(pt.getX))
          y.add(new DoubleWritable(pt.getY))
          i += 1
        }
      }
    }
  }

  /**
    * Reads a polygon attribute from a simple feature and sets it in an output format.
    * A polygon is modeled as a list of lines, with the first value being the shell,
    * and any subsequent values being interior holes.
    *
    * @see LineStringOutputFormatWriter
    *
    * @param xCol index of x field, containing a list of lists of points
    * @param yCol index of y field, containing a list of lists of points
    * @param attribute simple feature attribute index
    */
  class PolygonOutputFormatWriter(xCol: Int, yCol: Int, attribute: Int, description: TypeDescription)
      extends OrcOutputFormatWriter with SetOutputFormatDouble {
    override def apply(sf: SimpleFeature, output: OrcStruct): Unit = {
      val polygon = sf.getAttribute(attribute).asInstanceOf[Polygon]
      if (polygon == null) {
        output.setFieldValue(xCol, null)
        output.setFieldValue(yCol, null)
      } else {
        var xx = output.getFieldValue(xCol).asInstanceOf[OrcList[OrcList[DoubleWritable]]]
        var yy = output.getFieldValue(yCol).asInstanceOf[OrcList[OrcList[DoubleWritable]]]
        if (xx == null) {
          xx = new OrcList[OrcList[DoubleWritable]](description.getChildren.get(xCol), polygon.getNumInteriorRing + 1)
          output.setFieldValue(xCol, xx)
        } else {
          xx.clear()
        }
        if (yy == null) {
          yy = new OrcList[OrcList[DoubleWritable]](description.getChildren.get(yCol), polygon.getNumInteriorRing + 1)
          output.setFieldValue(yCol, yy)
        } else {
          yy.clear()
        }
        var j = 0
        while (j < polygon.getNumInteriorRing + 1) {
          val line = if (j == 0) { polygon.getExteriorRing } else { polygon.getInteriorRingN(j - 1) }
          val x = new OrcList[DoubleWritable](description.getChildren.get(xCol).getChildren.get(0), line.getNumPoints)
          val y = new OrcList[DoubleWritable](description.getChildren.get(yCol).getChildren.get(0), line.getNumPoints)
          var i = 0
          while (i < line.getNumPoints) {
            val pt = line.getCoordinateN(i)
            x.add(new DoubleWritable(pt.x))
            y.add(new DoubleWritable(pt.y))
            i += 1
          }
          xx.add(x)
          yy.add(y)
          j += 1
        }
      }
    }
  }

  /**
    * Reads a multi-linestring attribute from a simple feature and sets it in an output format.
    * A multi-linestring is modeled as a list of lines.
    *
    * @see LineStringOutputFormatWriter
    *
    * @param xCol index of x field, containing a list of lists of points
    * @param yCol index of y field, containing a list of lists of points
    * @param attribute simple feature attribute index
    */
  class MultiLineStringOutputFormatWriter(xCol: Int, yCol: Int, attribute: Int, description: TypeDescription)
      extends OrcOutputFormatWriter with SetOutputFormatDouble {
    override def apply(sf: SimpleFeature, output: OrcStruct): Unit = {
      val multiLineString = sf.getAttribute(attribute).asInstanceOf[MultiLineString]
      if (multiLineString == null) {
        output.setFieldValue(xCol, null)
        output.setFieldValue(yCol, null)
      } else {
        var xx = output.getFieldValue(xCol).asInstanceOf[OrcList[OrcList[DoubleWritable]]]
        var yy = output.getFieldValue(yCol).asInstanceOf[OrcList[OrcList[DoubleWritable]]]
        if (xx == null) {
          xx = new OrcList[OrcList[DoubleWritable]](description.getChildren.get(xCol), multiLineString.getNumGeometries)
          output.setFieldValue(xCol, xx)
        } else {
          xx.clear()
        }
        if (yy == null) {
          yy = new OrcList[OrcList[DoubleWritable]](description.getChildren.get(yCol), multiLineString.getNumGeometries)
          output.setFieldValue(yCol, yy)
        } else {
          yy.clear()
        }
        var j = 0
        while (j < multiLineString.getNumGeometries) {
          val line = multiLineString.getGeometryN(j).asInstanceOf[LineString]
          val x = new OrcList[DoubleWritable](description.getChildren.get(xCol).getChildren.get(0), line.getNumPoints)
          val y = new OrcList[DoubleWritable](description.getChildren.get(yCol).getChildren.get(0), line.getNumPoints)
          var i = 0
          while (i < line.getNumPoints) {
            val pt = line.getCoordinateN(i)
            x.add(new DoubleWritable(pt.x))
            y.add(new DoubleWritable(pt.y))
            i += 1
          }
          xx.add(x)
          yy.add(y)
          j += 1
        }
      }
    }
  }

  /**
    * Reads a multi-polygon attribute from a simple feature and sets it in an output format.
    * A multi-polygon is modeled as a list of polygons.
    *
    * @see PolygonOutputFormatWriter
    *
    * @param xCol index of x field, containing a list of lists of lists of points
    * @param yCol index of y field, containing a list of lists of lists of points
    * @param attribute simple feature attribute index
    */
  class MultiPolygonOutputFormatWriter(xCol: Int, yCol: Int, attribute: Int, description: TypeDescription)
      extends OrcOutputFormatWriter with SetOutputFormatDouble {
    override def apply(sf: SimpleFeature, output: OrcStruct): Unit = {
      val multiPolygon = sf.getAttribute(attribute).asInstanceOf[MultiPolygon]
      if (multiPolygon == null) {
        output.setFieldValue(xCol, null)
        output.setFieldValue(yCol, null)
      } else {
        var xxx = output.getFieldValue(xCol).asInstanceOf[OrcList[OrcList[OrcList[DoubleWritable]]]]
        var yyy = output.getFieldValue(yCol).asInstanceOf[OrcList[OrcList[OrcList[DoubleWritable]]]]
        if (xxx == null) {
          xxx = new OrcList[OrcList[OrcList[DoubleWritable]]](description.getChildren.get(xCol), multiPolygon.getNumGeometries)
          output.setFieldValue(xCol, xxx)
        } else {
          xxx.clear()
        }
        if (yyy == null) {
          yyy = new OrcList[OrcList[OrcList[DoubleWritable]]](description.getChildren.get(yCol), multiPolygon.getNumGeometries)
          output.setFieldValue(yCol, yyy)
        } else {
          yyy.clear()
        }
        var k = 0
        while (k < multiPolygon.getNumGeometries) {
          val polygon = multiPolygon.getGeometryN(k).asInstanceOf[Polygon]
          val xx = new OrcList[OrcList[DoubleWritable]](description.getChildren.get(xCol).getChildren.get(0), polygon.getNumGeometries)
          val yy = new OrcList[OrcList[DoubleWritable]](description.getChildren.get(yCol).getChildren.get(0), polygon.getNumGeometries)
          var j = 0

          while (j < polygon.getNumInteriorRing + 1) {
            val line = if (j == 0) { polygon.getExteriorRing } else { polygon.getInteriorRingN(j - 1) }
            val x = new OrcList[DoubleWritable](description.getChildren.get(xCol).getChildren.get(0), line.getNumPoints)
            val y = new OrcList[DoubleWritable](description.getChildren.get(yCol).getChildren.get(0), line.getNumPoints)
            var i = 0
            while (i < line.getNumPoints) {
              val pt = line.getCoordinateN(i)
              x.add(new DoubleWritable(pt.x))
              y.add(new DoubleWritable(pt.y))
              i += 1
            }
            xx.add(x)
            yy.add(y)
            j += 1
          }
          xxx.add(xx)
          yyy.add(yy)
          k += 1
        }
      }
    }
  }

  class ListOutputFormatWriter(col: Int, attribute: Int, binding: ObjectType, description: TypeDescription)
      extends OrcOutputFormatWriter {
    private val converter = getConverter(binding)

    override def apply(sf: SimpleFeature, output: OrcStruct): Unit = {
      import scala.collection.JavaConversions._
      val value = sf.getAttribute(attribute).asInstanceOf[java.util.List[AnyRef]]
      if (value == null) {
        output.setFieldValue(col, null)
      } else {
        var field = output.getFieldValue(col).asInstanceOf[OrcList[WritableComparable[_]]]
        if (field == null) {
          field = new OrcList(description.getChildren.get(col), value.size())
          output.setFieldValue(col, field)
        } else {
          field.clear()
        }
        value.foreach(v => field.add(converter.setValue(v, null)))
      }
    }
  }

  class MapOutputFormatWriter(col: Int,
                              attribute: Int,
                              keyBinding: ObjectType,
                              valueBinding: ObjectType,
                              description: TypeDescription) extends OrcOutputFormatWriter {
    private val keyConverter = getConverter(keyBinding)
    private val valueConverter = getConverter(valueBinding)

    override def apply(sf: SimpleFeature, output: OrcStruct): Unit = {
      import scala.collection.JavaConversions._
      val value = sf.getAttribute(attribute).asInstanceOf[java.util.Map[AnyRef, AnyRef]]
      if (value == null) {
        output.setFieldValue(col, null)
      } else {
        var field = output.getFieldValue(col).asInstanceOf[OrcMap[WritableComparable[_], WritableComparable[_]]]
        if (field == null) {
          field = new OrcMap(description.getChildren.get(col))
          output.setFieldValue(col, field)
        } else {
          field.clear()
        }
        value.foreach { case (k, v) => field.put(keyConverter.setValue(k, null), valueConverter.setValue(v, null)) }
      }
    }
  }

  trait SetOutputFormatValue {
    def setValue(value: AnyRef, existing: WritableComparable[_]): WritableComparable[_]
  }

  trait OutputFormatWriterAdapter extends OrcOutputFormatWriter with SetOutputFormatValue {
    def col: Int
    def attribute: Int
    override def apply(sf: SimpleFeature, output: OrcStruct): Unit =
      output.setFieldValue(col, setValue(sf.getAttribute(attribute), output.getFieldValue(col)))

  }

  trait SetOutputFormatDate extends SetOutputFormatValue {
    def setValue(value: AnyRef, existing: WritableComparable[_]): WritableComparable[_] = {
      if (value == null) { null } else {
        var field = existing.asInstanceOf[OrcTimestamp]
        if (field == null) {
          field = new OrcTimestamp
        }
        field.setTime(value.asInstanceOf[java.util.Date].getTime)
        field
      }
    }
  }

  trait SetOutputFormatString extends SetOutputFormatValue {
    def setValue(value: AnyRef, existing: WritableComparable[_]): WritableComparable[_] = {
      if (value == null) { null } else {
        var field = existing.asInstanceOf[Text]
        if (field == null) {
          field = new Text
        }
        field.set(value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
        field
      }
    }
  }

  trait SetOutputFormatInt extends SetOutputFormatValue {
    def setValue(value: AnyRef, existing: WritableComparable[_]): WritableComparable[_] = {
      if (value == null) { null } else {
        var field = existing.asInstanceOf[IntWritable]
        if (field == null) {
          field = new IntWritable
        }
        field.set(value.asInstanceOf[Integer].intValue)
        field
      }
    }
  }

  trait SetOutputFormatLong extends SetOutputFormatValue {
    def setValue(value: AnyRef, existing: WritableComparable[_]): WritableComparable[_] = {
      if (value == null) { null } else {
        var field = existing.asInstanceOf[LongWritable]
        if (field == null) {
          field = new LongWritable
        }
        field.set(value.asInstanceOf[java.lang.Long].longValue)
        field
      }
    }
  }

  trait SetOutputFormatFloat extends SetOutputFormatValue {
    def setValue(value: AnyRef, existing: WritableComparable[_]): WritableComparable[_] = {
      if (value == null) { null } else {
        var field = existing.asInstanceOf[FloatWritable]
        if (field == null) {
          field = new FloatWritable
        }
        field.set(value.asInstanceOf[java.lang.Float].floatValue)
        field
      }
    }
  }

  trait SetOutputFormatDouble extends SetOutputFormatValue {
    def setValue(value: AnyRef, existing: WritableComparable[_]): WritableComparable[_] = {
      if (value == null) { null } else {
        var field = existing.asInstanceOf[DoubleWritable]
        if (field == null) {
          field = new DoubleWritable
        }
        field.set(value.asInstanceOf[java.lang.Double].doubleValue)
        field
      }
    }
  }

  trait SetOutputFormatBoolean extends SetOutputFormatValue {
    def setValue(value: AnyRef, existing: WritableComparable[_]): WritableComparable[_] = {
      if (value == null) { null } else {
        var field = existing.asInstanceOf[BooleanWritable]
        if (field == null) {
          field = new BooleanWritable
        }
        field.set(value.asInstanceOf[java.lang.Boolean].booleanValue)
        field
      }
    }
  }

  trait SetOutputFormatBytes extends SetOutputFormatValue {
    def setValue(value: AnyRef, existing: WritableComparable[_]): WritableComparable[_] = {
      if (value == null) { null } else {
        var field = existing.asInstanceOf[BytesWritable]
        if (field == null) {
          field = new BytesWritable
        }
        val bytes = value.asInstanceOf[Array[Byte]]
        field.set(bytes, 0, bytes.length)
        field
      }
    }
  }

  trait SetOutputFormatUuid extends SetOutputFormatValue {
    def setValue(value: AnyRef, existing: WritableComparable[_]): WritableComparable[_] = {
      if (value == null) { null } else {
        var field = existing.asInstanceOf[Text]
        if (field == null) {
          field = new Text
        }
        field.set(value.asInstanceOf[UUID].toString.getBytes(StandardCharsets.UTF_8))
        field
      }
    }
  }

  /**
    * Gets a converter instance
    *
    * @param binding binding
    * @return
    */
  private def getConverter(binding: ObjectType): SetOutputFormatValue = {
    binding match {
      case ObjectType.DATE     => new SetOutputFormatDate {}
      case ObjectType.STRING   => new SetOutputFormatString {}
      case ObjectType.INT      => new SetOutputFormatInt {}
      case ObjectType.LONG     => new SetOutputFormatLong {}
      case ObjectType.FLOAT    => new SetOutputFormatFloat {}
      case ObjectType.DOUBLE   => new SetOutputFormatDouble {}
      case ObjectType.BOOLEAN  => new SetOutputFormatBoolean {}
      case ObjectType.BYTES    => new SetOutputFormatBytes {}
      case ObjectType.JSON     => new SetOutputFormatString {}
      case ObjectType.UUID     => new SetOutputFormatUuid {}
      case _ => throw new IllegalArgumentException(s"Unexpected object type $binding")
    }
  }
}
