/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.io.Closeable

import com.vividsolutions.jts.geom.Point
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.complex.impl.NullableMapWriter
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class SimpleFeatureVector private (val sft: SimpleFeatureType,
                                   val underlying: NullableMapVector,
                                   val dictionaries: Map[String, ArrowDictionary])
                                  (implicit allocator: BufferAllocator) extends Closeable {

  import scala.collection.JavaConversions._

  // TODO user data

  // note: writer creates the map child vectors based on the sft, and should be instantiated before the reader
  val writer = new Writer(this)
  val reader = new Reader(this)

  def reset(): Unit = {
    // TODO is there a better way to reset the buffer?
    underlying.clear()
    underlying.allocateNewSafe()
  }

  override def close(): Unit = {
    underlying.close()
    writer.arrowWriter.close()
  }

  class Writer(vector: SimpleFeatureVector) {
    private [SimpleFeatureVector] val arrowWriter = new NullableMapWriter(vector.underlying)
    private val idWriter = ArrowAttributeWriter("id", Seq(ObjectType.STRING), classOf[String], vector.underlying, arrowWriter, None, allocator)
    private [SimpleFeatureVector] val attributeWriters = sft.getAttributeDescriptors.map { ad =>
      ArrowAttributeWriter(ad, vector.underlying, arrowWriter, dictionaries.get(ad.getLocalName), allocator)
    }.toArray

    def set(i: Int, feature: SimpleFeature): Unit = {
      arrowWriter.setPosition(i)
      arrowWriter.start()
      idWriter.apply(feature.getID)
      var j = 0
      while (j < attributeWriters.length) {
        attributeWriters(j).apply(feature.getAttribute(j))
        j += 1
      }
      arrowWriter.end()
    }

    def setValueCount(count: Int): Unit = arrowWriter.setValueCount(count)
  }

  class Reader(vector: SimpleFeatureVector) {
    private val idReader = ArrowAttributeReader("id", Seq(ObjectType.STRING), classOf[String], vector.underlying, None)
    private val attributeReaders = sft.getAttributeDescriptors.map { ad =>
      ArrowAttributeReader(ad, vector.underlying, dictionaries.get(ad.getLocalName))
    }.toArray

    def get(i: Int): SimpleFeature = {
      val id = idReader(i).asInstanceOf[String]
      val attributes = attributeReaders.map(_.apply(i))
      new ScalaSimpleFeature(id, vector.sft, attributes)
    }

    def getValueCount: Int = vector.underlying.getAccessor.getValueCount
  }
}

object SimpleFeatureVector {

  def create(sft: SimpleFeatureType,
             dictionaries: Map[String, ArrowDictionary])
            (implicit allocator: BufferAllocator): SimpleFeatureVector = {
    val underlying = new NullableMapVector(sft.getTypeName, allocator, null, null)
    underlying.allocateNew()
    new SimpleFeatureVector(sft, underlying, dictionaries)
  }

  def wrap(vector: NullableMapVector,
           dictionaries: Map[String, ArrowDictionary])
          (implicit allocator: BufferAllocator): SimpleFeatureVector = {
    import scala.collection.JavaConversions._
    val attributes = vector.getField.getChildren.flatMap { field =>
      val name = field.getName
      // filter out feature id from attributes
      if (name == "id") {
        None
      } else if (field.getDictionary != null) {
        // currently dictionary encoding is only for string types
        Some(s"$name:String")
      } else {
        lazy val geometry = GeometryVector.typeOf(field)
        val binding = field.getType match {
          case t: ArrowType.Utf8   => "String" // TODO json, uuid
          case t: ArrowType.Binary => "Bytes"
          case t: ArrowType.Bool   => "Boolean"
          case t: ArrowType.Date   => "Date"
          case t: ArrowType.Int if t.getBitWidth == 32 => "Int"
          case t: ArrowType.Int if t.getBitWidth == 64 => "Long"
          case t: ArrowType.FloatingPoint if t.getPrecision == FloatingPointPrecision.SINGLE => "Float"
          case t: ArrowType.FloatingPoint if t.getPrecision == FloatingPointPrecision.DOUBLE => "Double"
          case t: ArrowType.Struct if geometry == classOf[Point] => "Point:srid=4326"
          case _ => throw new IllegalArgumentException(s"Unexpected field type for field $field")
        }
        Some(s"$name:$binding")
      }
    }
    // TODO user data
    val sft = SimpleFeatureTypes.createType(vector.getField.getName, attributes.mkString(","))
    new SimpleFeatureVector(sft, vector, dictionaries)
  }
}
