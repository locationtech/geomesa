/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.serialization

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Kryo serialization implementation for simple features. This class shouldn't be used directly -
 * see @KryoFeatureSerializer
 *
 * @param sft the type of simple feature
 * @param opts the encoding options (optional)
 */
class SimpleFeatureSerializer(sft: SimpleFeatureType, opts: Set[SerializationOption] = Set.empty)
    extends Serializer[SimpleFeature] {

  private val serializer = KryoFeatureSerializer(sft, opts)

  override def write(kryo: Kryo, output: Output, sf: SimpleFeature): Unit = {
    val bytes = serializer.serialize(sf)
    output.writeInt(bytes.length, true)
    output.write(bytes)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[SimpleFeature]): SimpleFeature = {
    val bytes = Array.ofDim[Byte](input.readInt(true))
    input.read(bytes)
    serializer.deserialize(bytes)
  }
}

