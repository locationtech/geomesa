/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import java.nio.ByteBuffer
import java.util
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Coordinate
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.serialization.ObjectType
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Group converter that can create simple features. Note that we should refactor
  * this a little more and perhaps have this store raw values and then push the
  * conversions of SimpleFeature "types" and objects into the SimpleFeatureRecordMaterializer
  * which will mean they are only converted and then added to simple features if a
  * record passes the parquet filters and needs to be materialized.
  *
  * @param sft
  */
class SimpleFeatureGroupConverter(sft: SimpleFeatureType) extends GroupConverter {

  private val idConverter = new PrimitiveConverter {
    override def addBinary(value: Binary): Unit = {
      curId = value
    }
  }
  private val converters = SimpleFeatureParquetConverters.converters(sft, this) :+ idConverter

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
  private val geomIdx = sft.getGeomIndex
  private val numVals = sft.getAttributeCount
  private val gf = JTSFactoryFinder.getGeometryFactory

  // Temp placeholders
  private var curId: Binary = _
  private var currentArr: Array[AnyRef] = new Array[AnyRef](numVals)
  var x: Double = _
  var y: Double = _

  override def start(): Unit = {
    curId = null
    var i = 0
    while (i < numVals) {
      currentArr(i) = null
      i += 1
    }
    x = 0.0
    y = 0.0
  }

  // Don't materialize unless we have to
  def getCurrent = {
    set(geomIdx, gf.createPoint(new Coordinate(x, y)))
    new ScalaSimpleFeature(curId.toStringUsingUTF8, sft, util.Arrays.copyOf(currentArr, currentArr.length))
  }

  override def end(): Unit = { }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  def set(idx: Int, value: AnyRef): Unit = currentArr(idx) = value

}

abstract class SimpleFeatureFieldConverter(parent: SimpleFeatureGroupConverter) extends PrimitiveConverter

class PointConverter(parent: SimpleFeatureGroupConverter) extends GroupConverter {

  private val converters = Array[PrimitiveConverter](
    // Specific to this PointConverter instance
    new PrimitiveConverter {
      override def addDouble(value: Double): Unit = {
        parent.x = value
      }
    },
    new PrimitiveConverter {
      override def addDouble(value: Double): Unit = {
        parent.y = value
      }
    }
  )

  override def getConverter(fieldIndex: Int): Converter = {
    converters(fieldIndex)
  }

  override def start(): Unit = { }

  override def end(): Unit = { }
}

object SimpleFeatureParquetConverters {

  def converters(sft: SimpleFeatureType, sfGC: SimpleFeatureGroupConverter): Array[Converter] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.zipWithIndex.map { case (ad, idx) => converterFor(ad, idx, sfGC) }.toArray
  }

  // TODO we are creating lots of objects and boxing primitives here when we may not need to
  // unless a record is materialized so we can likely speed this up by not creating any of
  // the true SFT types util a record passes a filter in the SimpleFeatureRecordMaterializer
  def converterFor(ad: AttributeDescriptor, index: Int, parent: SimpleFeatureGroupConverter): Converter = {
    val binding = ad.getType.getBinding
    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)

    objectType match {

      case ObjectType.GEOMETRY =>
        // TODO support union type of other geometries based on the SFT
        new PointConverter(parent)

      case ObjectType.DATE =>
        new SimpleFeatureFieldConverter(parent) {
          override def addLong(value: Long): Unit = {
            // TODO this can be optimized to set a long and not materialize date objects
            parent.set(index, new Date(value) )
          }
        }

      case ObjectType.STRING =>
        new SimpleFeatureFieldConverter(parent) {
          override def addBinary(value: Binary): Unit = {
            parent.set(index, value.toStringUsingUTF8)
          }
        }

      case ObjectType.INT =>
        new SimpleFeatureFieldConverter(parent) {
          override def addInt(value: Int): Unit = {
            parent.set(index, Int.box(value))
          }
        }

      case ObjectType.DOUBLE =>
        new SimpleFeatureFieldConverter(parent) {
          override def addInt(value: Int): Unit = {
            parent.set(index, Double.box(value.toDouble))

          }

          override def addDouble(value: Double): Unit = {
            parent.set(index, Double.box(value))
          }

          override def addFloat(value: Float): Unit = {
            parent.set(index, Double.box(value.toDouble))
          }

          override def addLong(value: Long): Unit = {
            parent.set(index, Double.box(value.toDouble))
          }
        }

      case ObjectType.LONG =>
        new SimpleFeatureFieldConverter(parent) {
          override def addLong(value: Long): Unit = {
            parent.set(index, Long.box(value))
          }
        }


      case ObjectType.FLOAT =>
        new SimpleFeatureFieldConverter(parent) {
          override def addFloat(value: Float): Unit = {
            parent.set(index, Float.box(value))
          }
        }

      case ObjectType.BOOLEAN =>
        new SimpleFeatureFieldConverter(parent) {
          override def addBoolean(value: Boolean): Unit = {
            parent.set(index, Boolean.box(value))
          }
        }


      case ObjectType.BYTES =>
        new SimpleFeatureFieldConverter(parent) {
          override def addBinary(value: Binary): Unit = {
            parent.set(index, value.getBytes)
          }
        }

      case ObjectType.LIST =>
        // TODO:
        null

      case ObjectType.MAP =>
        // TODO:
        null

      case ObjectType.UUID =>
        new SimpleFeatureFieldConverter(parent) {
          override def addBinary(value: Binary): Unit = {
            val bb = ByteBuffer.wrap(value.getBytes)
            val uuid = new UUID(bb.getLong, bb.getLong)
            parent.set(index, uuid)
          }
        }
    }

  }

}
