/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import com.googlecode.cqengine.IndexedCollection
import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.index.navigable.NavigableIndex
import org.locationtech.geomesa.memory.cqengine.attribute.SimpleFeatureAttribute
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeature

trait NavIndexBuilder {
  type VALUE <: Comparable[VALUE]
  val valueClass: Class[VALUE]
  type ATTRIBUTE = Attribute[SimpleFeature, VALUE]

  def apply(ad: AttributeDescriptor, coll: IndexedCollection[SimpleFeature]): Unit = {
    val attribute: ATTRIBUTE = new SimpleFeatureAttribute[VALUE](valueClass, ad.getLocalName)
    coll.addIndex(NavigableIndex.onAttribute(attribute))
  }
}

object BuildIntNavIndex extends NavIndexBuilder {
  type VALUE = java.lang.Integer
  val valueClass = classOf[java.lang.Integer]
}

object BuildLongNavIndex extends NavIndexBuilder {
  type VALUE = java.lang.Long
  val valueClass = classOf[java.lang.Long]
}

object BuildFloatNavIndex extends NavIndexBuilder {
  type VALUE = java.lang.Float
  val valueClass = classOf[java.lang.Float]
}

object BuildDoubleNavIndex extends NavIndexBuilder {
  type VALUE = java.lang.Double
  val valueClass = classOf[java.lang.Double]
}

object BuildDateNavIndex extends NavIndexBuilder {
  type VALUE = java.util.Date
  val valueClass = classOf[java.util.Date]
}