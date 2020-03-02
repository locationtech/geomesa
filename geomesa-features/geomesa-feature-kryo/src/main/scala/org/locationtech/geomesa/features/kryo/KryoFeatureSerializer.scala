/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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

/**
  * Kryo feature serialization and deserialization.
  *
  * The current serialization scheme (version 3):
  *
  * <ol>
  *   <li>one byte containing the serialization version (3)</li>
  *   <li>two bytes for a short containing the number of serialized attributes. this supports
  *       appending attributes to the schema, as we know how many attributes were written when deserializing</li>
  *   <li>one byte for the size of the stored offsets, which will currently be either 2 (for shorts) or 4 (for ints)</li>
  *   <li>a known number of bytes for metadata, which consists of:
  *     <ol>
  *       <li>2 byte short or 4 byte int (determined by the size as stored above)
  *           per attribute for the offset to the start of the attribute in the serialized bytes</li>
  *       <li>2 byte short or 4 byte int (determined by the size as stored above)
  *           for the offset to the start of the user data (or end of the feature if no user data)</li>
  *       <li>4 bytes per 32 attributes (or part thereof) to store a bitset tracking null attributes</li>
  *     </ol>
  *   </li>
  *   <li>the feature id as a string (if `withId`)</li>
  *   <li>the serialized attributes, in order</li>
  *   <li>the serialized user data (if `withUserData`)</li>
  * </ol>
  */
trait KryoFeatureSerializer extends KryoFeatureSerialization with KryoFeatureDeserialization

object KryoFeatureSerializer {

  val Version3: Byte = 3
  val Version2: Byte = 2

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

