/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.serialization

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer

/**
 * Kryo serializer for geometries.  Not thread safe.
 */
class GeometrySerializer extends Serializer[Geometry] {

  override def write(kryo: Kryo, output: Output, geom: Geometry): Unit = {
    output.writeByte(KryoFeatureSerializer.Version3)
    KryoGeometrySerialization.serialize(output, geom)
  }

  override def read(kryo: Kryo, input: Input, typ: Class[Geometry]): Geometry = {
    input.readByte() // version - not used
    KryoGeometrySerialization.deserialize(input)
  }
}
