/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import java.util.Collections
import java.util.concurrent.Phaser

import org.locationtech.geomesa.features.AbstractSimpleFeature.{AbstractImmutableSimpleFeature, AbstractMutableSimpleFeature}
import org.locationtech.geomesa.utils.collection.WordBitSet
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Simple feature implementation optimized to instantiate from serialization
  *
  * @param sft simple feature type
  * @param initialId simple feature id
  * @param initialValues if provided, must already be converted into the appropriate types
  * @param initialUserData user data
  */
class ScalaSimpleFeature(
    sft: SimpleFeatureType,
    initialId: String,
    initialValues: Array[AnyRef] = null,
    initialUserData: java.util.Map[AnyRef, AnyRef] = null
  ) extends AbstractMutableSimpleFeature(sft, initialId, initialUserData) {

  private val values = if (initialValues == null) { Array.ofDim[AnyRef](sft.getAttributeCount) } else { initialValues }

  override def setAttributeNoConvert(index: Int, value: AnyRef): Unit = values(index) = value
  override def getAttribute(index: Int): AnyRef = values(index)
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
    * @param id simple feature id
    * @param values attribute values, must already be converted into the appropriate types
    * @param userData user data (not null)
    */
  class ImmutableSimpleFeature(
      sft: SimpleFeatureType,
      id: String,
      values: Array[AnyRef],
      userData: java.util.Map[AnyRef, AnyRef] = Collections.emptyMap()
    ) extends AbstractImmutableSimpleFeature(sft, id) {

    override lazy val getUserData: java.util.Map[AnyRef, AnyRef] = Collections.unmodifiableMap(userData)

    override def getAttribute(index: Int): AnyRef = values(index)
  }

  /**
    * Lazily evaluated, immutable simple feature implementation
    *
    * @param sft simple feature type
    * @param id simple feature id
    * @param reader lazily read attributes, must be thread-safe and already of the appropriate types
    * @param userDataReader lazily read user data, must be thread-safe
    */
  class LazyImmutableSimpleFeature(
      sft: SimpleFeatureType,
      id: String,
      private var reader: LazyAttributeReader,
      private var userDataReader: LazyUserDataReader
    ) extends AbstractImmutableSimpleFeature(sft, id) {

    // we synchronize on the low-level words, in order to minimize contention
    private val bits = WordBitSet(sft.getAttributeCount)
    private lazy val attributes = Array.ofDim[AnyRef](sft.getAttributeCount)

    private var phaser: Phaser = new Phaser(sft.getAttributeCount) {
      override def onAdvance(phase: Int, registeredParties: Int): Boolean = {
        // once all attributes have been read, dereference any backing resources so that they can be gc'd
        reader = null
        phaser = null
        true
      }
    }

    override lazy val getUserData: java.util.Map[AnyRef, AnyRef] = {
      val read = userDataReader.read()
      userDataReader = null // dereference any backing resources
      Collections.unmodifiableMap(read)
    }

    override def getAttribute(index: Int): AnyRef = {
      if (reader != null) {
        val word = bits.word(index)
        word.synchronized {
          if (word.add(index)) {
            attributes(index) = reader.read(index)
            phaser.arriveAndDeregister()
          }
        }
      }
      attributes(index)
    }
  }

  /**
    * Lazily evaluated, mutable simple feature implementation
    *
    * @param sft simple feature type
    * @param initialId simple feature id
    * @param reader lazily read attributes, must be thread-safe and already of the appropriate types
    * @param userDataReader lazily read user data, must be thread-safe
    */
  class LazyMutableSimpleFeature(
      sft: SimpleFeatureType,
      initialId: String,
      private var reader: LazyAttributeReader,
      private var userDataReader: LazyUserDataReader
    ) extends AbstractMutableSimpleFeature(sft, initialId, null) {

    // we synchronize on the low-level words, in order to minimize contention
    private val bits = WordBitSet(sft.getAttributeCount)
    private lazy val attributes = Array.ofDim[AnyRef](sft.getAttributeCount)

    private var phaser: Phaser = new Phaser(sft.getAttributeCount) {
      override def onAdvance(phase: Int, registeredParties: Int): Boolean = {
        // once all attributes have been read, dereference any backing resources so that they can be gc'd
        reader = null
        phaser = null
        true
      }
    }

    override lazy val getUserData: java.util.Map[AnyRef, AnyRef] = {
      val ud = userDataReader.read()
      userDataReader = null // dereference any backing resources
      ud
    }

    override def setAttributeNoConvert(index: Int, value: AnyRef): Unit = {
      if (reader != null) {
        val word = bits.word(index)
        word.synchronized {
          if (word.add(index)) {
            phaser.arriveAndDeregister()
          }
        }
      }
      attributes(index) = value
    }

    override def getAttribute(index: Int): AnyRef = {
      if (reader != null) {
        val word = bits.word(index)
        word.synchronized {
          if (word.add(index)) {
            attributes(index) = reader.read(index)
            phaser.arriveAndDeregister()
          }
        }
      }
      attributes(index)
    }
  }


  /**
    * Lazy attribute reader
    */
  trait LazyAttributeReader {
    def read(i: Int): AnyRef
  }

  /**
    * Lazy user data reader
    */
  trait LazyUserDataReader {
    def read(): java.util.Map[AnyRef, AnyRef]
  }
}
