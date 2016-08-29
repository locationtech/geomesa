/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import java.util.UUID

import com.googlecode.cqengine.attribute.Attribute
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.memory.cqengine.attribute.{SimpleFeatureFidAttribute, SimpleFeatureAttribute}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

case class SFTAttributes(sft: SimpleFeatureType) {
  private val attributes = sft.getAttributeDescriptors

  private val lookupMap: Map[String, Attribute[SimpleFeature, _]] = attributes.map { attr =>
    val name = attr.getLocalName
    name -> SFTAttributes.buildSimpleFeatureAttribute(attr.getType.getBinding, name)
  }.toMap

  // TODO: this is really, really bad :)
  def lookup[T](attributeName: String): Attribute[SimpleFeature, T] = {
    lookupMap(attributeName).asInstanceOf[Attribute[SimpleFeature, T]]
  }

  def lookupComparable[T <: Comparable[T]](attributeName: String): Attribute[SimpleFeature, T] = {
    lookupMap(attributeName).asInstanceOf[Attribute[SimpleFeature, T]]
  }
}

object SFTAttributes {
  val fidAttribute: Attribute[SimpleFeature, String] = new SimpleFeatureFidAttribute()

  def buildSimpleFeatureAttribute(ad: AttributeDescriptor): Attribute[SimpleFeature, _] = {
    buildSimpleFeatureAttribute(ad.getType.getBinding, ad.getLocalName)
  }

  def buildSimpleFeatureAttribute[A](binding: Class[_], name: String): Attribute[SimpleFeature, _] = {
    binding match {
      case c if classOf[java.lang.String].isAssignableFrom(c) => new SimpleFeatureAttribute(classOf[String], name)
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => new SimpleFeatureAttribute(classOf[Integer], name)
      case c if classOf[java.lang.Long].isAssignableFrom(c) => new SimpleFeatureAttribute(classOf[java.lang.Long], name)
      case c if classOf[java.lang.Float].isAssignableFrom(c) => new SimpleFeatureAttribute(classOf[java.lang.Float], name)
      case c if classOf[java.lang.Double].isAssignableFrom(c) => new SimpleFeatureAttribute(classOf[java.lang.Double], name)
      case c if classOf[java.lang.Boolean].isAssignableFrom(c) => new SimpleFeatureAttribute(classOf[java.lang.Boolean], name)
      case c if classOf[java.util.Date].isAssignableFrom(c) => new SimpleFeatureAttribute(classOf[java.util.Date], name)
      case c if classOf[UUID].isAssignableFrom(c) => new SimpleFeatureAttribute(classOf[UUID], name)
      case c if classOf[Geometry].isAssignableFrom(c) => new SimpleFeatureAttribute(classOf[Geometry], name)
    }
  }
}
