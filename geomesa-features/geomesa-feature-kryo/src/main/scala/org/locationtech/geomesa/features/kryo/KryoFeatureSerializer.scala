/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo

import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.impl.ActiveDeserialization.{ImmutableActiveDeserialization, MutableActiveDeserialization}
import org.locationtech.geomesa.features.kryo.impl.LazyDeserialization.{ImmutableLazyDeserialization, MutableLazyDeserialization}
import org.locationtech.geomesa.features.kryo.impl.{KryoFeatureDeserialization, KryoFeatureSerialization}
import org.opengis.feature.simple.SimpleFeatureType

trait KryoFeatureSerializer extends KryoFeatureSerialization with KryoFeatureDeserialization

object KryoFeatureSerializer {

  val VERSION = 2
  assert(VERSION < Byte.MaxValue, "Serialization expects version to be in one byte")

  val NULL_BYTE: Byte     = 0.asInstanceOf[Byte]
  val NON_NULL_BYTE: Byte = 1.asInstanceOf[Byte]

  def apply(sft: SimpleFeatureType, options: Set[SerializationOption] = Set.empty): KryoFeatureSerializer = {
    (options.immutable, options.isLazy) match {
      case (true,  true)  => new ImmutableLazySerializer(sft, options)
      case (true,  false) => new ImmutableActiveSerializer(sft, options)
      case (false, true)  => new MutableLazySerializer(sft, options)
      case (false, false) => new MutableActiveSerializer(sft, options)
    }
  }

  def builder(sft: SimpleFeatureType): Builder = new Builder(sft)

  class ImmutableActiveSerializer(sft: SimpleFeatureType, val options: Set[SerializationOption])
      extends KryoFeatureSerializer with ImmutableActiveDeserialization {
    override private [kryo] def serializeSft = sft
    override private [kryo] def deserializeSft = sft
  }

  class ImmutableLazySerializer(sft: SimpleFeatureType, val options: Set[SerializationOption])
      extends KryoFeatureSerializer with ImmutableLazyDeserialization {
    override private [kryo] def serializeSft = sft
    override private [kryo] def deserializeSft = sft
  }

  class MutableActiveSerializer(sft: SimpleFeatureType, val options: Set[SerializationOption])
      extends KryoFeatureSerializer with MutableActiveDeserialization {
    override private [kryo] def serializeSft = sft
    override private [kryo] def deserializeSft = sft
  }

  class MutableLazySerializer(sft: SimpleFeatureType, val options: Set[SerializationOption])
      extends KryoFeatureSerializer with MutableLazyDeserialization {
    override private [kryo] def serializeSft = sft
    override private [kryo] def deserializeSft = sft
  }

  class Builder private [KryoFeatureSerializer] (sft: SimpleFeatureType)
      extends SimpleFeatureSerializer.Builder[Builder] {
    override def build(): KryoFeatureSerializer = apply(sft, options.toSet)
  }
}

