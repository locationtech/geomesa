/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Coordinate
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.serialization.ObjectType
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType


class SimpleFeatureGroupConverter(sft: SimpleFeatureType) extends GroupConverter {

  private val idConverter = new PrimitiveConverter {
    override def addBinary(value: Binary): Unit = {
      current.getIdentifier.asInstanceOf[FeatureIdImpl].setID(value.toStringUsingUTF8)
    }
  }
  private val converters = SimpleFeatureParquetConverters.converters(sft, this) :+ idConverter

  var current: ScalaSimpleFeature = _

  override def start(): Unit = {
    current = new ScalaSimpleFeature("", sft)
  }

  override def end(): Unit = { }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

}

abstract class SimpleFeatureFieldConverter(parent: SimpleFeatureGroupConverter) extends PrimitiveConverter

class PointConverter(parent: SimpleFeatureGroupConverter) extends GroupConverter {

  private val gf = JTSFactoryFinder.getGeometryFactory
  private var x: Double = _
  private var y: Double = _

  private val converters = Array[PrimitiveConverter](
    // Specific to this PointConverter instance
    new PrimitiveConverter {
      override def addDouble(value: Double): Unit = {
        x = value
      }
    },
    new PrimitiveConverter {
      override def addDouble(value: Double): Unit = {
        y = value
      }
    }
  )

  override def getConverter(fieldIndex: Int): Converter = {
    converters(fieldIndex)
  }

  override def start(): Unit = {
    x = 0.0
    y = 0.0
  }

  override def end(): Unit = {
    parent.current.setDefaultGeometry(gf.createPoint(new Coordinate(x, y)))
  }
}

object SimpleFeatureParquetConverters {

  def converters(sft: SimpleFeatureType, sfGC: SimpleFeatureGroupConverter): Array[Converter] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.zipWithIndex.map { case (ad, idx) => converterFor(ad, idx, sfGC) }.toArray
  }

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
            parent.current.setAttributeNoConvert(index, new Date(value))
          }
        }

      case ObjectType.STRING =>
        new SimpleFeatureFieldConverter(parent) {
          override def addBinary(value: Binary): Unit = {
            parent.current.setAttributeNoConvert(index, value.toStringUsingUTF8)
          }
        }

      case ObjectType.INT =>
        new SimpleFeatureFieldConverter(parent) {
          override def addInt(value: Int): Unit = {
            parent.current.setAttributeNoConvert(index, Int.box(value))
          }
        }

      case ObjectType.DOUBLE =>
        new SimpleFeatureFieldConverter(parent) {
          override def addInt(value: Int): Unit = {
            parent.current.setAttributeNoConvert(index, Double.box(value.toDouble))
          }

          override def addDouble(value: Double): Unit = {
            parent.current.setAttributeNoConvert(index, Double.box(value))
          }

          override def addFloat(value: Float): Unit = {
            parent.current.setAttributeNoConvert(index, Double.box(value.toDouble))
          }

          override def addLong(value: Long): Unit = {
            parent.current.setAttributeNoConvert(index, Double.box(value.toDouble))
          }
        }

      case ObjectType.LONG =>
        new SimpleFeatureFieldConverter(parent) {
          override def addLong(value: Long): Unit = {
            parent.current.setAttributeNoConvert(index, Long.box(value))
          }
        }


      case ObjectType.FLOAT =>
        new SimpleFeatureFieldConverter(parent) {
          override def addFloat(value: Float): Unit = {
            parent.current.setAttributeNoConvert(index, Float.box(value))
          }
        }

      case ObjectType.BOOLEAN =>
        new SimpleFeatureFieldConverter(parent) {
          override def addBoolean(value: Boolean): Unit = {
            parent.current.setAttributeNoConvert(index, Boolean.box(value))
          }
        }


      case ObjectType.BYTES =>
        new SimpleFeatureFieldConverter(parent) {
          override def addBinary(value: Binary): Unit = {
            parent.current.setAttributeNoConvert(index, value.getBytes)
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
            parent.current.setAttributeNoConvert(index, uuid)
          }
        }
    }

  }

}
