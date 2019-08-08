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

  val Version : Byte = 3
  val Version2: Byte = 2

  @deprecated("Version")
  lazy val VERSION: Int = Version

  val NullByte: Byte    = 0.asInstanceOf[Byte]
  val NonNullByte: Byte = 1.asInstanceOf[Byte]

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
    override protected [kryo] def in: SimpleFeatureType = sft
    override protected [kryo] def out: SimpleFeatureType = sft
  }

  class ImmutableLazySerializer(sft: SimpleFeatureType, val options: Set[SerializationOption])
      extends KryoFeatureSerializer with ImmutableLazyDeserialization {
    override protected [kryo] def in: SimpleFeatureType = sft
    override protected [kryo] def out: SimpleFeatureType = sft
  }

  class MutableActiveSerializer(sft: SimpleFeatureType, val options: Set[SerializationOption])
      extends KryoFeatureSerializer with MutableActiveDeserialization {
    override protected [kryo] def in: SimpleFeatureType = sft
    override protected [kryo] def out: SimpleFeatureType = sft
  }

  class MutableLazySerializer(sft: SimpleFeatureType, val options: Set[SerializationOption])
      extends KryoFeatureSerializer with MutableLazyDeserialization {
    override protected [kryo] def in: SimpleFeatureType = sft
    override protected [kryo] def out: SimpleFeatureType = sft
  }

  class Builder private [KryoFeatureSerializer] (sft: SimpleFeatureType)
      extends SimpleFeatureSerializer.Builder[Builder] {
    override def build(): KryoFeatureSerializer = apply(sft, options.toSet)
  }
}

