/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import com.googlecode.cqengine.attribute.Attribute
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.memory.cqengine.attribute.{SimpleFeatureAttribute, SimpleFeatureFidAttribute}

case class SFTAttributes(sft: SimpleFeatureType) {

  import scala.collection.JavaConverters._

  private val lookupMap: Map[String, Attribute[SimpleFeature, _]] =
    sft.getAttributeDescriptors.asScala.map(buildSimpleFeatureAttribute).map(a => a.getAttributeName -> a).toMap

  // TODO: this is really, really bad :)
  def lookup[T](attributeName: String): Attribute[SimpleFeature, T] =
    lookupMap(attributeName).asInstanceOf[Attribute[SimpleFeature, T]]

  def buildSimpleFeatureAttribute(ad: AttributeDescriptor): Attribute[SimpleFeature, _] =
    new SimpleFeatureAttribute(ad.getType.getBinding, sft, ad.getLocalName)
}

object SFTAttributes {
  val fidAttribute: Attribute[SimpleFeature, String] = new SimpleFeatureFidAttribute()
}
