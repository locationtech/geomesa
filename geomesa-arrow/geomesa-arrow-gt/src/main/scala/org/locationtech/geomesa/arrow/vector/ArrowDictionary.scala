/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding}
import org.locationtech.geomesa.arrow.ArrowAllocator
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.utils.geotools.ObjectType
import org.locationtech.geomesa.utils.geotools.ObjectType.ObjectType
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.`type`.AttributeDescriptor

import scala.reflect.ClassTag

/**
  * Holder for dictionary values
  */
sealed trait ArrowDictionary extends Closeable {

  lazy private val map = {
    val builder = scala.collection.mutable.Map.newBuilder[AnyRef, Int]
    builder.sizeHint(length)
    var i = 0
    foreach { value =>
      builder += ((value, i))
      i += 1
    }
    builder.result()
  }

  def encoding: DictionaryEncoding
  def id: Long = encoding.getId
  def length: Int

  /**
    * Decode a dictionary int to a value. Note: may not be thread safe
    *
    * @param i dictionary encoded int
    * @return value
    */
  def lookup(i: Int): AnyRef

  /**
    * Dictionary encode a value to an int
    *
    * @param value value to encode
    * @return dictionary encoded int
    */
  def index(value: AnyRef): Int = map.getOrElse(value, length)

  /**
    * Apply a function to each value in the dictionary
    *
    * @param f function
    * @tparam U function return type
    */
  def foreach[U](f: AnyRef => U): Unit = iterator.foreach(f)

  /**
    * Create an iterator over the values in this dictionary
    *
    * @return
    */
  def iterator: Iterator[AnyRef] = new Iterator[AnyRef] {
    private var i = 0
    override def hasNext: Boolean = i < ArrowDictionary.this.length
    override def next(): AnyRef = try { lookup(i) } finally { i += 1 }
  }

  /**
   * Create an arrow dictionary vector
   *
   * @param precision precision
   * @return
   */
  def toDictionary(precision: SimpleFeatureEncoding): Dictionary with Closeable
}

object ArrowDictionary {

  /**
    * Create a dictionary based off a sequence of values. Encoding will be smallest that will fit all values.
    *
    * @param id dictionary id
    * @param values dictionary values
    * @return dictionary
    */
  def create[T <: AnyRef](typename: String, id: Long, values: Array[T])(implicit ct: ClassTag[T]): ArrowDictionary =
    create(typename, id, values, values.length)

  /**
    * Create a dictionary based on a subset of a value array
    *
    * @param id dictionary id
    * @param values array of dictionary values
    * @param length number of valid entries in the values array, starting at position 0
    * @return
    */
  def create[T <: AnyRef](typename: String, id: Long, values: Array[T], length: Int)(implicit ct: ClassTag[T]): ArrowDictionary =
    new ArrowDictionaryArray[T](typename, createEncoding(id, length), values, length, ct.runtimeClass.asInstanceOf[Class[T]])

  /**
    * Create a dictionary based on wrapping an arrow vector
    *
    * @param encoding dictionary id and metadata
    * @param values dictionary vector
    * @param descriptor attribute descriptor for the dictionary, used to read values from the underlying vector
    * @param precision simple feature encoding used on the dictionary values
    * @return
    */
  def create(
      encoding: DictionaryEncoding,
      values: FieldVector,
      descriptor: AttributeDescriptor,
      precision: SimpleFeatureEncoding): ArrowDictionary = {
    new ArrowDictionaryVector(encoding, values, ObjectType.selectType(descriptor), precision)
  }

  /**
   * Create a dictionary based on wrapping an arrow vector
   *
   * @param encoding dictionary id and metadata
   * @param values dictionary vector
   * @param bindings attribute descriptor bindings, used for reading values from the arrow vector
   * @param precision simple feature encoding used on the dictionary values
   * @return
   */
  def create(
      encoding: DictionaryEncoding,
      values: FieldVector,
      bindings: Seq[ObjectType],
      precision: SimpleFeatureEncoding): ArrowDictionary = {
    new ArrowDictionaryVector(encoding, values, bindings, precision)
  }

  /**
    * Holder for dictionary values
    *
    * @param values dictionary values. When encoded, values are replaced with their index in the seq
    * @param encoding dictionary id and int width, id must be unique per arrow file
    */
  class ArrowDictionaryArray[T <: AnyRef](
      typename: String,
      val encoding: DictionaryEncoding,
      values: Array[T],
      val length: Int,
      binding: Class[T]
    ) extends ArrowDictionary {

    override def lookup(i: Int): AnyRef = if (i < length) { values(i) } else { "[other]" }

    override def toDictionary(precision: SimpleFeatureEncoding): Dictionary with Closeable = {
      val allocator = ArrowAllocator(s"dictionary-array:$typename")
      val name = s"dictionary-$id"
      val bindings = ObjectType.selectType(binding)
      val writer = ArrowAttributeWriter(name, bindings, None, Map.empty, precision, VectorFactory(allocator))
      var i = 0
      while (i < length) {
        writer.apply(i, values(i))
        i += 1
      }
      writer.setValueCount(length)

      new Dictionary(writer.vector, encoding) with Closeable {
        override def close(): Unit = CloseWithLogging.raise(writer.vector, allocator)
      }
    }

    override def close(): Unit = {}
  }

  /**
    * Dictionary that wraps an arrow vector
    *
    * @param encoding dictionary id and metadata
    * @param vector arrow vector
    * @param bindings attribute descriptor bindings, used for reading values from the arrow vector
    * @param precision simple feature encoding used for the arrow vector
    */
  class ArrowDictionaryVector(
      val encoding: DictionaryEncoding,
      vector: FieldVector,
      bindings: Seq[ObjectType],
      precision: SimpleFeatureEncoding
    ) extends ArrowDictionary {

    // we use an attribute reader to get the right type conversion
    private val reader = ArrowAttributeReader(bindings, vector, None, precision)
    private var references = 1

    override val length: Int = vector.getValueCount

    override def lookup(i: Int): AnyRef = if (i < length) { reader.apply(i) } else { "[other]" }

    override def toDictionary(precision: SimpleFeatureEncoding): Dictionary with Closeable = synchronized {
      if (references < 1) {
        throw new IllegalStateException("Trying to create a dictionary from a closed vector")
      } else if (precision != this.precision) {
        throw new IllegalArgumentException("Wrapped dictionary vectors can't be re-encoded with a different precision")
      }
      references += 1
      new Dictionary(vector, encoding) with Closeable {
        override def close(): Unit = ArrowDictionaryVector.this.close()
      }
    }

    override def close(): Unit = synchronized {
      references -= 1
      if (references < 1) {
        vector.close()
      }
    }
  }

  /**
   * Tracks values seen and writes dictionary encoded ints instead
   */
  class ArrowDictionaryBuilder(val encoding: DictionaryEncoding) extends ArrowDictionary {

    // next dictionary index to use
    private val counter = new AtomicInteger(0)

    // values that we have seen, and their dictionary index
    private val values = scala.collection.mutable.LinkedHashMap.empty[AnyRef, Int]

    override def lookup(i: Int): AnyRef = values.find(_._2 == i).map(_._1).getOrElse("[other]")

    override def index(value: AnyRef): Int = values.getOrElseUpdate(value, counter.getAndIncrement())

    override def length: Int = values.size

    // note: iterator will return in insert order
    // we need to keep it ordered so that dictionary values match up with their index
    override def iterator: Iterator[AnyRef] = values.keys.iterator

    override def toDictionary(precision: SimpleFeatureEncoding): Dictionary with Closeable =
      throw new NotImplementedError()

    override def close(): Unit = {}

    def clear(): Unit = {
      counter.set(0)
      values.clear()
    }
  }

  // use the smallest int type possible to minimize bytes used
  private def createEncoding(id: Long, count: Int): DictionaryEncoding = {
    // we check `MaxValue - 1` to allow for the fallback 'other'
    if (count < Byte.MaxValue - 1) {
      new DictionaryEncoding(id, false, new ArrowType.Int(8, true))
    } else if (count < Short.MaxValue - 1) {
      new DictionaryEncoding(id, false, new ArrowType.Int(16, true))
    } else {
      new DictionaryEncoding(id, false, new ArrowType.Int(32, true))
    }
  }
}
