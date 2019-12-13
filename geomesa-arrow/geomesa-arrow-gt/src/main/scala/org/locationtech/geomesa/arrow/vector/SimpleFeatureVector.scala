/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.io.Closeable
import java.util.{Collections, Date}

import org.locationtech.jts.geom.Geometry
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.{ListVector, StructVector}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}
import org.apache.arrow.vector.{BigIntVector, FieldVector}
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding.Encoding
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Abstraction for using simple features in Arrow vectors
  *
  * @param sft simple feature type
  * @param underlying underlying arrow vector
  * @param dictionaries map of field names to dictionary values, used for dictionary encoding fields.
  *                     All values must be provided up front.
  * @param encoding options for encoding
  * @param allocator buffer allocator
  */
class SimpleFeatureVector private [arrow] (val sft: SimpleFeatureType,
                                           val underlying: StructVector,
                                           val dictionaries: Map[String, ArrowDictionary],
                                           val encoding: SimpleFeatureEncoding)
                                          (implicit allocator: BufferAllocator) extends Closeable {

  // note: writer creates the map child vectors based on the sft, and should be instantiated before the reader
  val writer = new Writer(this)
  val reader = new Reader(this)

  /**
    * Clear any simple features currently stored in the vector
    */
  def clear(): Unit = underlying.setValueCount(0)

  override def close(): Unit = {
    underlying.close()
    writer.close()
  }

  class Writer(vector: SimpleFeatureVector) {
    private [SimpleFeatureVector] val arrowWriter = vector.underlying.getWriter
    private val idWriter = ArrowAttributeWriter.id(sft, Some(vector.underlying), vector.encoding)
    private [arrow] val attributeWriters = ArrowAttributeWriter(sft, Some(vector.underlying), dictionaries, encoding).toArray

    def set(index: Int, feature: SimpleFeature): Unit = {
      arrowWriter.setPosition(index)
      arrowWriter.start()
      idWriter.apply(index, feature)
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
    val idReader: ArrowAttributeReader = ArrowAttributeReader.id(sft, vector.underlying, vector.encoding)
    val readers: Array[ArrowAttributeReader] =
      ArrowAttributeReader(sft, vector.underlying, dictionaries, encoding).toArray

    // feature that can be re-populated with calls to 'load'
    val feature: ArrowSimpleFeature = new ArrowSimpleFeature(sft, idReader, readers, -1)

    def get(index: Int): ArrowSimpleFeature = new ArrowSimpleFeature(sft, idReader, readers, index)

    def load(index: Int): Unit = feature.index = index

    def getValueCount: Int = vector.underlying.getValueCount
  }
}

object SimpleFeatureVector {

  val DefaultCapacity = 8096
  val FeatureIdField  = "id"
  val DescriptorKey   = "descriptor"
  val OptionsKey      = "options"

  case class SimpleFeatureEncoding(fids: Option[Encoding], geometry: Encoding, date: Encoding)

  object SimpleFeatureEncoding {

    val Min = SimpleFeatureEncoding(Some(Encoding.Min), Encoding.Min, Encoding.Min)
    val Max = SimpleFeatureEncoding(Some(Encoding.Max), Encoding.Max, Encoding.Max)

    def min(includeFids: Boolean, proxyFids: Boolean = false): SimpleFeatureEncoding = {
      val fids = if (includeFids) { Some(if (proxyFids) { Encoding.Min } else { Encoding.Max }) } else { None }
      SimpleFeatureEncoding(fids, Encoding.Min, Encoding.Min)
    }

    object Encoding extends Enumeration {
      type Encoding = Value
      val Min, Max = Value
    }
  }

  /**
    * Create a new simple feature vector
    *
    * @param sft simple feature type
    * @param dictionaries map of field names to dictionary values, used for dictionary encoding fields.
    *                     All values must be provided up front.
    * @param encoding options for encoding
    * @param capacity initial capacity for number of features able to be stored in vectors
    * @param allocator buffer allocator
    * @return
    */
  def create(sft: SimpleFeatureType,
             dictionaries: Map[String, ArrowDictionary],
             encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.Min,
             capacity: Int = DefaultCapacity)
            (implicit allocator: BufferAllocator): SimpleFeatureVector = {
    val metadata = Collections.singletonMap(OptionsKey, SimpleFeatureTypes.encodeUserData(sft))
    val fieldType = new FieldType(true, ArrowType.Struct.INSTANCE, null, metadata)
    val underlying = new StructVector(sft.getTypeName, allocator, fieldType, null)
    val vector = new SimpleFeatureVector(sft, underlying, dictionaries, encoding)
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
  def wrap(vector: StructVector, dictionaries: Map[String, ArrowDictionary])
          (implicit allocator: BufferAllocator): SimpleFeatureVector = {
    val (sft, encoding) = getFeatureType(vector)
    new SimpleFeatureVector(sft, vector, dictionaries, encoding)
  }

  /**
    * Create a simple feature vector using a new arrow vector
    *
    * @param vector simple feature vector to copy
    * @param underlying arrow vector
    * @param allocator buffer allocator
    * @return
    */
  def clone(vector: SimpleFeatureVector, underlying: StructVector)
           (implicit allocator: BufferAllocator): SimpleFeatureVector = {
    new SimpleFeatureVector(vector.sft, underlying, vector.dictionaries, vector.encoding)
  }

  /**
    * Reads the feature type and feature encoding from an existing arrow vector
    *
    * @param vector vector
    * @return
    */
  def getFeatureType(vector: StructVector): (SimpleFeatureType, SimpleFeatureEncoding) = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConverters._

    val attributes = ArrayBuffer.empty[String]
    var fidEncoding: Option[Encoding] = None

    vector.getField.getChildren.asScala.foreach { field =>
      if (field.getName == FeatureIdField) {
        field.getType match {
          case _: ArrowType.Int           => fidEncoding = Some(Encoding.Min) // proxy id encoded fids
          case _: ArrowType.FixedSizeList => fidEncoding = Some(Encoding.Max) // uuid encoded fids
          case _: ArrowType.Utf8          => fidEncoding = Some(Encoding.Max) // normal string fids
          case _ => throw new IllegalArgumentException(s"Found feature ID vector field of unexpected type: $field")
        }
      } else {
        attributes.append(field.getMetadata.get(DescriptorKey))
      }
    }
    // add sft-level metadata
    val options = Option(vector.getField.getMetadata.get(OptionsKey)).getOrElse("")

    val sft = SimpleFeatureTypes.createImmutableType(vector.getField.getName, attributes.mkString(",") + options)
    val geomPrecision = {
      val geomVector: Option[FieldVector] =
        Option(sft.getGeomField).flatMap(d => Option(vector.getChild(d))).orElse(getNestedVector[Geometry](sft, vector))
      val isDouble = geomVector.exists(v => GeometryFields.precisionFromField(v.getField) == FloatingPointPrecision.DOUBLE)
      if (isDouble) { Encoding.Max } else { Encoding.Min }
    }
    val datePrecision = {
      val dateVector: Option[FieldVector] =
        sft.getDtgField.flatMap(d => Option(vector.getChild(d))).orElse(getNestedVector[Date](sft, vector))
      val isLong = dateVector.exists(_.isInstanceOf[BigIntVector])
      if (isLong) { Encoding.Max } else { Encoding.Min }
    }
    val encoding = SimpleFeatureEncoding(fidEncoding, geomPrecision, datePrecision)

    (sft, encoding)
  }

  def isGeometryVector(vector: FieldVector): Boolean = {
    Option(vector.getField.getMetadata.get(DescriptorKey))
        .map(SimpleFeatureTypes.createDescriptor)
        .exists(d => classOf[Geometry].isAssignableFrom(d.getType.getBinding))
  }

  /**
    * Checks nested vector types (lists and maps) for instances of the given type
    *
    * @param sft simple feature type
    * @param vector simple feature vector
    * @param ct class tag
    *
    * @return
    */
  private def getNestedVector[T](sft: SimpleFeatureType,
                                 vector: StructVector)
                                (implicit ct: ClassTag[T]): Option[FieldVector] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    import scala.collection.JavaConversions._

    sft.getAttributeDescriptors.flatMap {
      case d if d.isList && ct.runtimeClass.isAssignableFrom(d.getListType()) =>
        Option(vector.getChild(d.getLocalName).asInstanceOf[ListVector]).map(_.getDataVector)
      case d if d.isMap && ct.runtimeClass.isAssignableFrom(d.getMapTypes()._1) =>
        Option(vector.getChild(d.getLocalName).asInstanceOf[StructVector]).map(_.getChildrenFromFields.get(0))
      case d if d.isMap && ct.runtimeClass.isAssignableFrom(d.getMapTypes()._2) =>
        Option(vector.getChild(d.getLocalName).asInstanceOf[StructVector]).map(_.getChildrenFromFields.get(1))
      case _ => None
    }.headOption
  }
}
