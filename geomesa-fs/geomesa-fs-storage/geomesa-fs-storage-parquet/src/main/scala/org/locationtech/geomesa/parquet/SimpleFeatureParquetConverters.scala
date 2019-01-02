/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import java.nio.ByteBuffer
import java.util
import java.util.{Date, UUID}

import org.locationtech.jts.geom.Coordinate
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.parquet.SimpleFeatureParquetConverters.Setter
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable

/**
  * Group converter that can create simple features. Note that we should refactor
  * this a little more and perhaps have this store raw values and then push the
  * conversions of SimpleFeature "types" and objects into the SimpleFeatureRecordMaterializer
  * which will mean they are only converted and then added to simple features if a
  * record passes the parquet filters and needs to be materialized.
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

  // Temp placeholders
  private var curId: Binary = _
  private val currentArr: Array[AnyRef] = new Array[AnyRef](numVals)


  override def start(): Unit = {
    curId = null
    var i = 0
    while (i < numVals) {
      currentArr(i) = null
      i += 1
    }
  }

  // Don't materialize unless we have to
  def getCurrent: SimpleFeature = {
    // Deep copy array since the next record may change references in the array
    new ScalaSimpleFeature(sft, curId.toStringUsingUTF8, util.Arrays.copyOf(currentArr, currentArr.length))
  }

  override def end(): Unit = { }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  def set(idx: Int, value: AnyRef): Unit = currentArr(idx) = value
}

class PointConverter(index: Int, set: Setter) extends GroupConverter {

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
    set(index, gf.createPoint(new Coordinate(x, y)))
  }
}

object SimpleFeatureParquetConverters {

  type Setter = (Int, AnyRef) => Unit

  def converters(sft: SimpleFeatureType, sfGC: SimpleFeatureGroupConverter): Array[Converter] = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.zipWithIndex.map { case (ad, idx) => converterFor(ad, idx, sfGC) }.toArray
  }

  // TODO we are creating lots of objects and boxing primitives here when we may not need to
  // unless a record is materialized so we can likely speed this up by not creating any of
  // the true SFT types util a record passes a filter in the SimpleFeatureRecordMaterializer
  def converterFor(ad: AttributeDescriptor, index: Int, parent: SimpleFeatureGroupConverter): Converter = {
    val sfBinding = ad.getType.getBinding
    val bindings = ObjectType.selectType(sfBinding, ad.getUserData)
    converterFor(bindings, index, (index: Int, value: AnyRef) => parent.set(index, value))
  }

  def converterFor(bindings: Seq[ObjectType], index: Int, set: Setter): Converter = {
    bindings.head match {

      case ObjectType.GEOMETRY =>
        // TODO support union type of other geometries based on the SFT
        new PointConverter(index, set)

      case ObjectType.DATE =>
        new PrimitiveConverter {
          override def addLong(value: Long): Unit = {
            // TODO this can be optimized to set a long and not materialize date objects
            set(index, new Date(value) )
          }
        }

      case ObjectType.STRING =>
        new PrimitiveConverter {
          override def addBinary(value: Binary): Unit = {
            set(index, value.toStringUsingUTF8)
          }
        }

      case ObjectType.INT =>
        new PrimitiveConverter {
          override def addInt(value: Int): Unit = {
            set(index, Int.box(value))
          }
        }

      case ObjectType.DOUBLE =>
        new PrimitiveConverter {
          override def addInt(value: Int): Unit = {
            set(index, Double.box(value.toDouble))
          }

          override def addDouble(value: Double): Unit = {
            set(index, Double.box(value))
          }

          override def addFloat(value: Float): Unit = {
            set(index, Double.box(value.toDouble))
          }

          override def addLong(value: Long): Unit = {
            set(index, Double.box(value.toDouble))
          }
        }

      case ObjectType.LONG =>
        new PrimitiveConverter {
          override def addLong(value: Long): Unit = {
            set(index, Long.box(value))
          }
        }

      case ObjectType.FLOAT =>
        new PrimitiveConverter {
          override def addFloat(value: Float): Unit = {
            set(index, Float.box(value))
          }
        }

      case ObjectType.BOOLEAN =>
        new PrimitiveConverter {
          override def addBoolean(value: Boolean): Unit = {
            set(index, Boolean.box(value))
          }
        }


      case ObjectType.BYTES =>
        new PrimitiveConverter {
          override def addBinary(value: Binary): Unit = {
            set(index, value.getBytes)
          }
        }

      // TODO support things other than strings
      case ObjectType.LIST =>
        new GroupConverter() {
          private val values = mutable.ListBuffer.empty[AnyRef]
          private val conv =
            new GroupConverter {
              private val converter =
                converterFor(bindings.drop(1), 0, (index: Int, value: AnyRef) => values += value)

              // better only be one field (0)
              override def getConverter(fieldIndex: Int): Converter = converter

              override def end(): Unit = {}
              override def start(): Unit = {}
            }
          override def getConverter(fieldIndex: Int): GroupConverter = conv

          override def start(): Unit = values.clear()

          override def end(): Unit = {
            import scala.collection.JavaConverters._
            set(index, values.asJava)
          }
        }

      case ObjectType.MAP =>
        new GroupConverter {
          val m = mutable.HashMap.empty[AnyRef, AnyRef]
          private var k: AnyRef = _
          private var v: AnyRef = _

          val conv =
            new GroupConverter {
              private val keyConverter =
                converterFor(bindings.slice(1, 2), 0, (index: Int, value: AnyRef) => k = value)
              private val valueConverter =
                converterFor(bindings.drop(2), 0, (index: Int, value: AnyRef) => v = value)

              override def getConverter(fieldIndex: Int): Converter =
                if (fieldIndex == 0) {
                  keyConverter
                } else {
                  valueConverter
                }

              override def end(): Unit = m += k -> v
              override def start(): Unit = {}
            }

          override def getConverter(fieldIndex: Int): GroupConverter = conv

          override def start(): Unit = m.clear()
          override def end(): Unit = {
            import scala.collection.JavaConverters._
            set(index, m.asJava)
          }

        }

      case ObjectType.UUID =>
        new PrimitiveConverter {
          override def addBinary(value: Binary): Unit = {
            val bb = ByteBuffer.wrap(value.getBytes)
            val uuid = new UUID(bb.getLong, bb.getLong)
            set(index, uuid)
          }
        }
    }

  }

}
