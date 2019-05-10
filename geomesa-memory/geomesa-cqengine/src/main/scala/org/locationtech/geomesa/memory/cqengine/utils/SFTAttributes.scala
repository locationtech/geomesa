/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import com.googlecode.cqengine.attribute.Attribute
import org.locationtech.geomesa.memory.cqengine.attribute.{SimpleFeatureAttribute, SimpleFeatureFidAttribute}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
