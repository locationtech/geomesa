/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.filter.expression

import org.geotools.filter.expression.FilterVisitorExpressionWrapper
import org.geotools.filter.{ExpressionType, FilterVisitor}
import org.geotools.util.Converters
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.{ExpressionVisitor, PropertyName}
import org.xml.sax.helpers.NamespaceSupport

/**
 * Implementation of property name that looks up the value by index
 */
class FastPropertyName(name: String) extends PropertyName with org.geotools.filter.Expression {

  private var index: Int = -1

  override def getPropertyName: String = name

  override def getNamespaceContext: NamespaceSupport = null

  override def evaluate(obj: AnyRef): AnyRef = {
    val sf = try {
      obj.asInstanceOf[SimpleFeature]
    } catch {
      case e: Exception => throw new IllegalArgumentException("Only simple features are supported", e)
    }
    if (index == -1) {
      val nsIndex = name.indexOf(':')
      val localName = if (nsIndex == -1) name else name.substring(nsIndex + 1)
      index = sf.getFeatureType.indexOf(localName)
      if (index == -1) {
        throw new RuntimeException(s"Property name $name does not exist in feature type ${sf.getFeatureType}")
      }
    }
    sf.getAttribute(index)
  }

  override def evaluate[T](obj: AnyRef, target: Class[T]): T = Converters.convert(evaluate(obj), target)

  override def accept(visitor: ExpressionVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)

  // geotools filter methods - deprecated but still sometimes used
  override def getType: Short = ExpressionType.ATTRIBUTE
  override def getValue(feature: SimpleFeature): AnyRef = evaluate(feature.asInstanceOf[AnyRef])
  override def evaluate(feature: SimpleFeature): AnyRef = evaluate(feature.asInstanceOf[AnyRef])
  override def accept(visitor: FilterVisitor): Unit =
    accept(new FilterVisitorExpressionWrapper(visitor), null)
}
