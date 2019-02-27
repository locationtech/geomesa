/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data.index

import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, KeyValue, WritableFeature}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Writable feature values cache for Redis
  *
  * @param feature feature being written
  * @param serializer serializer
  * @param idSerializer id serializer
  */
class RedisWritableFeature(
    val feature: SimpleFeature,
    serializer: SimpleFeatureSerializer,
    idSerializer: String => Array[Byte]
  ) extends WritableFeature {

  import RedisWritableFeature.EmptyBytes

  // we don't use column families, column qualifiers or visibilities in the the rows
  override lazy val values: Seq[KeyValue] =
    Seq(KeyValue(EmptyBytes, EmptyBytes, EmptyBytes, serializer.serialize(feature)))

  override lazy val id: Array[Byte] = idSerializer(feature.getID)
}

object RedisWritableFeature {

  private val EmptyBytes = Array.empty[Byte]

  /**
    * Create a writable feature factory
    *
    * @param sft simple feature type
    * @return
    */
  def wrapper(sft: SimpleFeatureType): FeatureWrapper = {
    val id: String => Array[Byte] = GeoMesaFeatureIndex.idToBytes(sft)
    // add the length of the feature id into the byte array so that we can decode it after the value is concatenated
    val idSerializer: String => Array[Byte] = fid => {
      val bytes = id(fid)
      val result = Array.ofDim[Byte](bytes.length + 2)
      System.arraycopy(bytes, 0, result, 2, bytes.length)
      // note: if the feature id is longer than 32k characters this will cause problems...
      ByteArrays.writeShort(bytes.length.toShort, result)
      result
    }
    // we serialize with user data to store visibilities
    val serializer = KryoFeatureSerializer.builder(sft).withUserData.withoutId.build()
    new RedisFeatureWrapper(serializer, idSerializer)
  }

  class RedisFeatureWrapper(serializer: SimpleFeatureSerializer, idSerializer: String => Array[Byte])
      extends FeatureWrapper {
    override def wrap(feature: SimpleFeature): WritableFeature = {
      // remove all user data except visibilities
      // we need to keep visibilities for filtering, but don't want to store anything else,
      // as generally we don't store user data
      // note: at this point provided_fid has already been handled
      val visibility = feature.getUserData.get(SecurityUtils.FEATURE_VISIBILITY)
      feature.getUserData.clear()
      if (visibility != null) {
        feature.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, visibility)
      }
      new RedisWritableFeature(feature, serializer, idSerializer)
    }
  }
}
