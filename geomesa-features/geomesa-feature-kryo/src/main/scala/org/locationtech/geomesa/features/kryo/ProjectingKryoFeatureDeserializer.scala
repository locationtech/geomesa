/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo

import java.io.{InputStream, OutputStream}

import org.locationtech.geomesa.features.SerializationOption.{SerializationOption, SerializationOptions}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Deserialize and project to a new feature type
  *
  * @param original the simple feature type that was encoded
  * @param projected the simple feature type to project to when decoding
  * @param options serialization options
  */
class ProjectingKryoFeatureDeserializer(
    original: SimpleFeatureType,
    projected: SimpleFeatureType,
    val options: Set[SerializationOption] = Set.empty
  ) extends SimpleFeatureSerializer {

  private val delegate = KryoFeatureSerializer(original, SerializationOptions.builder.`lazy`.build ++ options)

  private val mappings = Array.tabulate(projected.getAttributeCount) { i =>
    original.indexOf(projected.getDescriptor(i).getLocalName)
  }

  override def serialize(feature: SimpleFeature): Array[Byte] = delegate.serialize(feature)

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit = delegate.serialize(feature, out)

  override def deserialize(in: InputStream): SimpleFeature = project(delegate.deserialize(in))

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    project(delegate.deserialize(bytes, offset, length))

  override def deserialize(id: String, in: InputStream): SimpleFeature =
    project(delegate.deserialize(id, in))

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    project(delegate.deserialize(id, bytes, offset, length))

  private def project(feature: SimpleFeature): SimpleFeature = {
    val values = mappings.map(i => if (i == -1) { null } else { feature.getAttribute(i) })
    new ScalaSimpleFeature(projected, feature.getID, values, feature.getUserData)
  }
}
