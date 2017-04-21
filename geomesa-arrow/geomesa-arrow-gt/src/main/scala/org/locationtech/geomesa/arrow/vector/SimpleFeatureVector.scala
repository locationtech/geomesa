/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.io.Closeable

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.GeometryPrecision.GeometryPrecision
import org.locationtech.geomesa.features.arrow.ArrowSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Abstraction for using simple features in Arrow vectors
  *
  * @param sft simple feature type
  * @param underlying underlying arrow vector
  * @param dictionaries map of field names to dictionary values, used for dictionary encoding fields.
  *                     All values must be provided up front.
  * @param includeFids encode feature ids in vectors or not
  * @param precision precision of coordinates - double or float
  * @param allocator buffer allocator
  */
class SimpleFeatureVector private (val sft: SimpleFeatureType,
                                   val underlying: NullableMapVector,
                                   val dictionaries: Map[String, ArrowDictionary],
                                   val includeFids: Boolean,
                                   val precision: GeometryPrecision)
                                  (implicit allocator: BufferAllocator) extends Closeable {

  // TODO user data at feature and schema level

  private var maxIndex = underlying.getValueCapacity - 1

  // note: writer creates the map child vectors based on the sft, and should be instantiated before the reader
  val writer = new Writer(this)
  val reader = new Reader(this)

  /**
    * double underlying vector capacity
    */
  def expand(): Unit = {
    underlying.reAlloc()
    maxIndex = underlying.getValueCapacity - 1
  }

  /**
    * Clear any simple features currently stored in the vector
    */
  def reset(): Unit = underlying.getMutator.setValueCount(0)

  override def close(): Unit = {
    underlying.close()
    writer.close()
  }

  class Writer(vector: SimpleFeatureVector) {
    private [SimpleFeatureVector] val arrowWriter = vector.underlying.getWriter
    private val idWriter = ArrowAttributeWriter.id(vector.underlying, vector.includeFids)
    private [arrow] val attributeWriters = ArrowAttributeWriter(sft, vector.underlying, dictionaries, precision).toArray

    def set(index: Int, feature: SimpleFeature): Unit = {
      while (index > vector.maxIndex ) {
        vector.expand()
      }
      arrowWriter.setPosition(index)
      arrowWriter.start()
      idWriter.apply(index, feature.getID)
      var i = 0
      while (i < attributeWriters.length) {
        attributeWriters(i).apply(index, feature.getAttribute(i))
        i += 1
      }
      arrowWriter.end()
    }

    def setValueCount(count: Int): Unit = {
      arrowWriter.setValueCount(count)
      attributeWriters.foreach(_.setValueCount(count))
    }

    private [vector] def close(): Unit = arrowWriter.close()
  }

  class Reader(vector: SimpleFeatureVector) {
    private val idReader = ArrowAttributeReader.id(vector.underlying, vector.includeFids)
    private [arrow] val attributeReaders = ArrowAttributeReader(sft, vector.underlying, dictionaries, precision).toArray

    def get(index: Int): ArrowSimpleFeature = new ArrowSimpleFeature(sft, idReader, attributeReaders, index)

    def getValueCount: Int = vector.underlying.getAccessor.getValueCount
  }
}

object SimpleFeatureVector {

  val DefaultCapacity = 8096

  object GeometryPrecision extends Enumeration {
    type GeometryPrecision = Value
    val Float, Double = Value
  }

  /**
    * * Create a new simple feature vector
    *
    * @param sft simple feature type
    * @param dictionaries map of field names to dictionary values, used for dictionary encoding fields.
    *                     All values must be provided up front.
    * @param includeFids include feature ids in arrow file or not
    * @param precision precision of coordinates - double or float
    * @param capacity initial capacity for number of features able to be stored in vectors
    * @param allocator buffer allocator
    * @return
    */
  def create(sft: SimpleFeatureType,
             dictionaries: Map[String, ArrowDictionary],
             includeFids: Boolean = true,
             precision: GeometryPrecision = GeometryPrecision.Double,
             capacity: Int = DefaultCapacity)
            (implicit allocator: BufferAllocator): SimpleFeatureVector = {
    val underlying = new NullableMapVector(sft.getTypeName, allocator, null, null)
    val vector = new SimpleFeatureVector(sft, underlying, dictionaries, includeFids, precision)
    // set capacity after all child vectors have been created by the writers, then allocate
    underlying.setInitialCapacity(capacity)
    underlying.allocateNew()
    vector
  }

  /**
    * Creates a simple feature vector based on an existing arrow vector
    *
    * @param vector arrow vector
    * @param dictionaries map of field names to dictionary values, used for dictionary encoding fields.
    *                     All values must be provided up front.
    * @param allocator buffer allocator
    * @return
    */
  def wrap(vector: NullableMapVector,
           dictionaries: Map[String, ArrowDictionary])
          (implicit allocator: BufferAllocator): SimpleFeatureVector = {
    import scala.collection.JavaConversions._
    val attributes = vector.getField.getChildren.collect {
      // filter out feature id from attributes
      case field if field.getName != "id" => field.getName
    }
    val includeFids = vector.getField.getChildren.exists(_.getName == "id")
    val sft = SimpleFeatureTypes.createType(vector.getField.getName, attributes.mkString(","))
    val geomVector = Option(vector.getChild(SimpleFeatureTypes.encodeDescriptor(sft, sft.getGeometryDescriptor)))
    val isFloat = geomVector.exists(v => GeometryFields.precisionFromField(v.getField) == FloatingPointPrecision.SINGLE)
    val precision = if (isFloat) { GeometryPrecision.Float } else { GeometryPrecision.Double }
    new SimpleFeatureVector(sft, vector, dictionaries, includeFids, precision)
  }
}
