/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
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
  def deserialize(bytes: Array[Byte]): SimpleFeature

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
  def deserialize(id: String, bytes: Array[Byte]): SimpleFeature

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
