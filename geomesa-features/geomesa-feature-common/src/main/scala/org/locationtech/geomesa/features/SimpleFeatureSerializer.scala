/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import java.io.{InputStream, OutputStream}

import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.opengis.feature.simple.SimpleFeature

trait HasEncodingOptions {
  def options: Set[SerializationOption]
}

/**
 * Interface to encode SimpleFeatures with a configurable serialization format.
 *
 * A SimpleFeatureEncoder is bound to a given SimpleFeatureType since serialization
 * may depend upon the schema of the feature type.
 *
 * SimpleFeatureEncoder classes may not be thread safe and should generally be used
 * as instance variables for performance reasons.
 */
trait SimpleFeatureSerializer extends HasEncodingOptions {

  /**
    * Serialize a simple feature to a byte array
    *
    * @param feature feature
    * @return
    */
  def serialize(feature: SimpleFeature): Array[Byte]

  /**
    * Serialize a simple feature to an output stream
    *
    * @param feature feature
    * @param out output stream
    */
  def serialize(feature: SimpleFeature, out: OutputStream): Unit

  /**
    * Deserialize a simple feature from a byte array
    *
    * @param bytes bytes
    * @return
    */
  def deserialize(bytes: Array[Byte]): SimpleFeature = deserialize(bytes, 0, bytes.length)

  /**
    * Deserialize a simple feature from an input stream
    *
    * @param in input
    * @return
    */
  def deserialize(in: InputStream): SimpleFeature

  /**
    * Deserialize a simple feature from a subset of a byte array
    *
    * @param bytes bytes
    * @param offset offset of first byte to read
    * @param length total bytes to read
    * @return
    */
  def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature

  /**
    * Deserialize a simple feature from a byte array, with a feature id provided separately.
    *
    * Note that this only makes sense when used in conjunction with
    * `org.locationtech.geomesa.features.SerializationOption.WithoutId()`
    *
    * @param id feature id
    * @param bytes bytes
    * @return
    */
  def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = deserialize(id, bytes, 0, bytes.length)

  /**
    * Deserialize a simple feature from an input stream, with a feature id provided separately
    *
    * Note that this only makes sense when used in conjunction with
    * `org.locationtech.geomesa.features.SerializationOption.WithoutId()`
    *
    * @param id feature id
    * @param in input
    * @return
    */
  def deserialize(id: String, in: InputStream): SimpleFeature

  /**
    * Deserialize a simple feature from a subset of a byte array, with a feature id provided separately
    *
    * Note that this only makes sense when used in conjunction with
    * `org.locationtech.geomesa.features.SerializationOption.WithoutId()`
    *
    * @param id feature id
    * @param bytes bytes
    * @param offset offset of first byte to read
    * @param length total bytes to read
    * @return
    */
  def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature
}

object SimpleFeatureSerializer {

  trait Builder[T <: Builder[T]] {

    this: T =>

    protected val options = scala.collection.mutable.Set.empty[SerializationOption]

    protected def add(key: SerializationOption): T = { options.add(key); this }
    protected def remove(key: SerializationOption): T = { options.remove(key); this }

    def immutable: T = add(SerializationOption.Immutable)
    def mutable: T = remove(SerializationOption.Immutable)
    def withUserData: T = add(SerializationOption.WithUserData)
    def withoutUserData: T = remove(SerializationOption.WithUserData)
    def withId: T = remove(SerializationOption.WithoutId)
    def withoutId: T = add(SerializationOption.WithoutId)
    def `lazy`: T = add(SerializationOption.Lazy)
    def active: T = remove(SerializationOption.Lazy)

    def build(): SimpleFeatureSerializer
  }

  /**
    * Serializer that doesn't implement all the semi-optional methods
    */
  trait LimitedSerialization extends SimpleFeatureSerializer {
    override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
      throw new NotImplementedError
    override def deserialize(in: InputStream): SimpleFeature =
      throw new NotImplementedError
    override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
      throw new NotImplementedError
    override def deserialize(id: String, in: InputStream): SimpleFeature =
      throw new NotImplementedError
    override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
      throw new NotImplementedError
  }
}