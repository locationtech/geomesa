/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

import org.locationtech.geomesa.features.AbstractSimpleFeature.{AbstractImmutableSimpleFeature, AbstractMutableSimpleFeature}
import org.locationtech.geomesa.utils.collection.WordBitSet
import org.locationtech.geomesa.utils.io.Sizable
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Simple feature implementation optimized to instantiate from serialization
 *
 * @param sft simple feature type
 * @param values array of attribute values
 */
class ScalaSimpleFeature private (sft: SimpleFeatureType, values: Array[AnyRef])
    extends AbstractMutableSimpleFeature(sft) with Sizable {

  private var userData: java.util.Map[AnyRef, AnyRef] = _

  /**
   * Public constructor
   *
   * @param sft simple feature type
   * @param id simple feature id
   * @param values if provided, must already be converted into the appropriate types
   * @param userData user data
   */
  def this(
      sft: SimpleFeatureType,
      id: String,
      values: Array[AnyRef] = null,
      userData: java.util.Map[AnyRef, AnyRef] = null) = {
    this(sft, if (values == null) { Array.ofDim[AnyRef](sft.getAttributeCount) } else { values })
    this.id = id
    this.userData = userData
  }

  override def setAttributeNoConvert(index: Int, value: AnyRef): Unit = values(index) = value
  override def getAttribute(index: Int): AnyRef = values(index)

  override def getUserData: java.util.Map[AnyRef, AnyRef] = synchronized {
    if (userData == null) {
      userData = new java.util.HashMap[AnyRef, AnyRef]()
    }
    userData
  }

  override def calculateSizeOf(): Long = Sizable.sizeOf(this) + Sizable.deepSizeOf(id, values, userData)
}

object ScalaSimpleFeature {

  import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

  import scala.collection.JavaConverters._

  /**
    * Copy the feature. This is a shallow copy, in that the attributes and user data values will be shared
    * between the two features
    *
    * @param in feature to copy
    * @return
    */
  def copy(in: SimpleFeature): ScalaSimpleFeature = copy(in.getFeatureType, in)

  /**
    * Copy the feature, with a new feature type. Attributes are copied by index and not converted, so the new
    * feature type must have a compatible schema.
    *
    * This is a shallow copy, in that the attributes and user data values will be shared between the two features
    *
    * @param sft new simple feature type
    * @param in feature to copy
    * @return
    */
  def copy(sft: SimpleFeatureType, in: SimpleFeature): ScalaSimpleFeature =
    new ScalaSimpleFeature(sft, in.getID, in.getAttributes.toArray, new java.util.HashMap[AnyRef, AnyRef](in.getUserData))

  /**
    * Copy the feature with a new feature type. Attributes will be copied by name, and converted
    * as necessary. As compared to `copy`, the new feature type does not have to have a compatible schema.
    *
    * If the feature already has the desired feature type, it will be returned as-is and not copied.
    *
    * This is a shallow copy, in that the attributes and user data values will be shared between the two features
    *
    * @param sft new feature type
    * @param in feature to copy
    * @return
    */
  def retype(sft: SimpleFeatureType, in: SimpleFeature): SimpleFeature = {
    if (sft == in.getFeatureType) { in } else {
      val out = new ScalaSimpleFeature(sft, in.getID)
      sft.getAttributeDescriptors.asScala.foreachIndex { case (d, i) =>
        out.setAttribute(i, in.getAttribute(d.getLocalName))
      }
      out.getUserData.putAll(in.getUserData)
      out
    }
  }

  /**
    * Creates a simple feature, converting the values to the appropriate type
    *
    * @param sft simple feature type
    * @param id feature id
    * @param values attributes values, corresponding to the feature type. types will be converted as necessary
    * @return
    */
  def create(sft: SimpleFeatureType, id: String, values: Any*): ScalaSimpleFeature = {
    val sf = new ScalaSimpleFeature(sft, id)
    var i = 0
    while (i < values.length) {
      sf.setAttribute(i, values(i).asInstanceOf[AnyRef])
      i += 1
    }
    sf
  }

  /**
   * Compares the id and attributes for the simple features - concrete class is not checked
   */
  def equalIdAndAttributes(sf1: SimpleFeature, sf2: SimpleFeature): Boolean =
    sf1 != null && sf2 != null && sf1.getIdentifier.equalsExact(sf2.getIdentifier) &&
        java.util.Arrays.equals(sf1.getAttributes.toArray, sf2.getAttributes.toArray)

  /**
    * Immutable simple feature implementation
    *
    * @param sft simple feature type
    * @param values attribute values, must already be converted into the appropriate types
    */
  class ImmutableSimpleFeature private (sft: SimpleFeatureType, values: Array[AnyRef])
      extends AbstractImmutableSimpleFeature(sft) with Sizable {

    private var userData: java.util.Map[AnyRef, AnyRef] = _

    /**
     *
     * Alternate constructor
     *
     * @param sft simple feature type
     * @param id simple feature id
     * @param values attribute values, must already be converted into the appropriate types
     * @param userData user data (not null)
     */
    def this(
        sft: SimpleFeatureType,
        id: String,
        values: Array[AnyRef],
        userData: java.util.Map[AnyRef, AnyRef] = Collections.emptyMap()) = {
      this(sft, values)
      this.id = id
      this.userData = Collections.unmodifiableMap(userData)
    }

    override def getAttribute(index: Int): AnyRef = values(index)
    override def getUserData: util.Map[AnyRef, AnyRef] = userData

    override def calculateSizeOf(): Long = Sizable.sizeOf(this) + Sizable.deepSizeOf(id, values, userData)
  }

  /**
    * Lazily evaluated, immutable simple feature implementation
    *
    * @param sft simple feature type
    */
  class LazyImmutableSimpleFeature private (sft: SimpleFeatureType)
      extends AbstractImmutableSimpleFeature(sft) with LazySimpleFeature with Sizable {

    /**
     * Constructor
     *
     * @param sft simple feature type
     * @param id simple feature id
     * @param reader lazily read attributes, must be thread-safe and already of the appropriate types
     * @param userDataReader lazily read user data, must be thread-safe
     *
     */
    def this(sft: SimpleFeatureType, id: String, reader: LazyAttributeReader, userDataReader: LazyUserDataReader) = {
      this(sft)
      this.id = id
      this.reader = reader
      this.userDataReader = new ImmutableLazyUserDataReader(userDataReader)
    }

    override def calculateSizeOf(): Long =
      Sizable.sizeOf(this) + Sizable.deepSizeOf(id, bits, attributes, userData, count, reader, userDataReader)
  }

  /**
    * Lazily evaluated, mutable simple feature implementation
    *
    * @param sft simple feature type
    */
  class LazyMutableSimpleFeature private (sft: SimpleFeatureType)
      extends AbstractMutableSimpleFeature(sft) with LazySimpleFeature with Sizable {

    /**
     * Constructor
     *
     * @param sft simple feature type
     * @param id simple feature id
     * @param reader lazily read attributes, must be thread-safe and already of the appropriate types
     * @param userDataReader lazily read user data, must be thread-safe
     *
     */
    def this(sft: SimpleFeatureType, id: String, reader: LazyAttributeReader, userDataReader: LazyUserDataReader) = {
      this(sft)
      this.id = id
      this.reader = reader
      this.userDataReader = userDataReader
    }

    override def setAttributeNoConvert(index: Int, value: AnyRef): Unit = {
      if (reader != null) {
        val word = bits.word(index)
        word.synchronized {
          if (word.add(index)) {
            bits.synchronized {
              if (attributes == null) {
                attributes = Array.ofDim[AnyRef](sft.getAttributeCount)
              }
            }
            if (count.decrementAndGet() == 0) {
              // once all attributes have been read, dereference any backing resources so that they can be gc'd
              reader = null
              count = null
            }
          }
        }
      }
      attributes(index) = value
    }

    override def calculateSizeOf(): Long =
      Sizable.sizeOf(this) + Sizable.deepSizeOf(id, bits, attributes, userData, count, reader, userDataReader)
  }

  /**
   * Thread-safe lazy evaluation of attributes
   */
  trait LazySimpleFeature extends AbstractSimpleFeature {

    // we synchronize on the low-level words, in order to minimize contention
    protected val bits: WordBitSet = WordBitSet(getFeatureType.getAttributeCount + 1)
    protected var attributes: Array[AnyRef] = _
    protected var userData: java.util.Map[AnyRef, AnyRef] = _

    protected var count = new AtomicInteger(getFeatureType.getAttributeCount)
    protected var reader: LazyAttributeReader = _
    protected var userDataReader: LazyUserDataReader = _

    override def getAttribute(index: Int): AnyRef = {
      if (reader != null) {
        val word = bits.word(index)
        word.synchronized {
          if (word.add(index)) {
            bits.synchronized {
              if (attributes == null) {
                attributes = Array.ofDim[AnyRef](getFeatureType.getAttributeCount)
              }
            }
            attributes(index) = reader.read(index)
            if (count.decrementAndGet() == 0) {
              // once all attributes have been read, dereference any backing resources so that they can be gc'd
              reader = null
              count = null
            }
          }
        }
      }
      attributes(index)
    }

    override def getUserData: java.util.Map[AnyRef, AnyRef] = {
      if (userDataReader != null) {
        synchronized {
          if (userData == null) {
            userData = userDataReader.read()
            userDataReader == null // dereference any backing resources
          }
        }
      }
      userData
    }
  }

  /**
    * Lazy attribute reader
    */
  trait LazyAttributeReader extends Sizable {
    def read(i: Int): AnyRef
  }

  /**
    * Lazy user data reader
    */
  trait LazyUserDataReader extends Sizable {
    def read(): java.util.Map[AnyRef, AnyRef]
  }

  class ImmutableLazyUserDataReader(delegate: LazyUserDataReader) extends LazyUserDataReader {
    override def read(): java.util.Map[AnyRef, AnyRef] = Collections.unmodifiableMap(delegate.read())
    override def calculateSizeOf(): Long = delegate.calculateSizeOf()
  }
}
