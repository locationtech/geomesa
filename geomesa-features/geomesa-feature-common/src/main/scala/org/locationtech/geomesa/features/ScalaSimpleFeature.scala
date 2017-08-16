/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import java.util.Collections

import org.locationtech.geomesa.features.AbstractSimpleFeature.{AbstractImmutableSimpleFeature, AbstractMutableSimpleFeature}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Simple feature implementation optimized to instantiate from serialization
  *
  * @param sft simple feature type
  * @param initialId simple feature id
  * @param initialValues if provided, must already be converted into the appropriate types
  * @param initialUserData user data
  */
class ScalaSimpleFeature(sft: SimpleFeatureType,
                         initialId: String,
                         initialValues: Array[AnyRef] = null,
                         initialUserData: java.util.Map[AnyRef, AnyRef] = null)
    extends AbstractMutableSimpleFeature(sft, initialId, initialUserData) {

  @deprecated("use primary constructor")
  def this(initialId: String, sft: SimpleFeatureType) = {
    this(sft, initialId, null, null)
  }

  private val values = if (initialValues == null) { Array.ofDim[AnyRef](sft.getAttributeCount) } else { initialValues }

  override def setAttributeNoConvert(index: Int, value: AnyRef): Unit = values(index) = value
  override def getAttribute(index: Int): AnyRef = values(index)
}

object ScalaSimpleFeature {

  def copy(in: SimpleFeature): ScalaSimpleFeature = copy(in.getFeatureType, in)

  def copy(sft: SimpleFeatureType, in: SimpleFeature): ScalaSimpleFeature =
    new ScalaSimpleFeature(sft, in.getID, in.getAttributes.toArray, new java.util.HashMap[AnyRef, AnyRef](in.getUserData))

  @deprecated("use copy")
  def create(sft: SimpleFeatureType, in: SimpleFeature): ScalaSimpleFeature = copy(sft, in)

  /**
   * Creates a simple feature, converting the values to the appropriate type
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
  class ImmutableSimpleFeature(sft: SimpleFeatureType,
                               id: String,
                               values: Array[AnyRef],
                               userData: java.util.Map[AnyRef, AnyRef] = Collections.emptyMap())
      extends AbstractImmutableSimpleFeature(sft, id, userData) {
    override def getAttribute(index: Int): AnyRef = values(index)
  }

  /**
    * Lazily evaluated, immutable simple feature implementation
    *
    * @param sft simple feature type
    * @param id simple feature id
    * @param readAttribute lazily read an attribute value, must already be converted into the appropriate types
    * @param readUserData lazily read the user data
    */
  class LazyImmutableSimpleFeature(sft: SimpleFeatureType,
                                   id: String,
                                   readAttribute: (Int) => AnyRef,
                                   readUserData: => java.util.Map[AnyRef, AnyRef])
      extends AbstractImmutableSimpleFeature(sft, id, readUserData) {

    private val bits = scala.collection.mutable.BitSet.fromBitMask(Array.fill(sft.getAttributeCount / 8 + 1)(0L))
    private val attributes = Array.ofDim[AnyRef](sft.getAttributeCount)

    override def getAttribute(index: Int): AnyRef = {
      if (bits.add(index)) {
        attributes(index) = readAttribute(index)
      }
      attributes(index)
    }
  }


  /**
    * Lazily evaluated, mutable simple feature implementation
    *
    * @param sft simple feature type
    * @param initialId simple feature id
    * @param readAttribute lazily read an attribute value, must already be converted into the appropriate types
    * @param readUserData lazily read the user data
    */
  class LazyMutableSimpleFeature(sft: SimpleFeatureType,
                                 initialId: String,
                                 readAttribute: (Int) => AnyRef,
                                 readUserData: => java.util.Map[AnyRef, AnyRef])
      extends AbstractMutableSimpleFeature(sft, initialId, null) {

    private val bits = scala.collection.mutable.BitSet.fromBitMask(Array.fill(sft.getAttributeCount / 8 + 1)(0L))
    private val attributes = Array.ofDim[AnyRef](sft.getAttributeCount)
    private var userDataRead = false

    override def setAttributeNoConvert(index: Int, value: AnyRef): Unit = {
      bits.add(index)
      attributes(index) = value
    }

    override def getAttribute(index: Int): AnyRef = {
      if (bits.add(index)) {
        attributes(index) = readAttribute(index)
      }
      attributes(index)
    }

    override def getUserData: java.util.Map[AnyRef, AnyRef] = {
      if (userDataRead) { super.getUserData } else {
        val res = super.getUserData
        res.putAll(readUserData)
        userDataRead = true
        res
      }
    }
  }
}