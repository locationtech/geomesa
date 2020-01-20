/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo

import java.io.{InputStream, OutputStream}

import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Project to a subtype when serializing
  *
  * @param original the simple feature type that will be serialized
  * @param projected the simple feature type to project to when serializing
  * @param options serialization options
  */
class ProjectingKryoFeatureSerializer(
    original: SimpleFeatureType,
    projected: SimpleFeatureType,
    val options: Set[SerializationOption] = Set.empty
  ) extends SimpleFeatureSerializer {

  private val delegate = KryoFeatureSerializer(projected, options)

  private val mappings = Array.tabulate(projected.getAttributeCount) { i =>
    original.indexOf(projected.getDescriptor(i).getLocalName)
  }

  private val features = new ThreadLocal[ScalaSimpleFeature]() {
    override def initialValue(): ScalaSimpleFeature = new ScalaSimpleFeature(projected, "")
  }

  override def serialize(feature: SimpleFeature): Array[Byte] = delegate.serialize(project(feature))

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
    delegate.serialize(project(feature), out)

  override def deserialize(in: InputStream): SimpleFeature = delegate.deserialize(in)

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    delegate.deserialize(bytes, offset, length)

  override def deserialize(id: String, in: InputStream): SimpleFeature = delegate.deserialize(id, in)

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    delegate.deserialize(id, bytes, offset, length)

  private def project(feature: SimpleFeature): SimpleFeature = {
    val projected = features.get
    projected.setId(feature.getID)
    var i = 0
    while (i < mappings.length) {
      projected.setAttributeNoConvert(i, feature.getAttribute(mappings(i)))
      i += 1
    }
    projected.getUserData.clear()
    projected.getUserData.putAll(feature.getUserData)
    projected
  }
}
