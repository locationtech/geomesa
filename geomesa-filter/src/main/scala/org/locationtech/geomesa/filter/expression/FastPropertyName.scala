/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter.expression

import org.geotools.filter.expression.PropertyAccessors
import org.geotools.util.Converters
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.{ExpressionVisitor, PropertyName}
import org.xml.sax.helpers.NamespaceSupport

/**
 * Implementation of property name that looks up the value by index
 */
class FastPropertyName(name: String) extends PropertyName with org.opengis.filter.expression.Expression {

  private var getProperty: (SimpleFeature) => AnyRef = null

  override def getPropertyName: String = name

  override def getNamespaceContext: NamespaceSupport = null

  override def evaluate(obj: AnyRef): AnyRef = {
    val sf = try {
      obj.asInstanceOf[SimpleFeature]
    } catch {
      case e: Exception => throw new IllegalArgumentException("Only simple features are supported", e)
    }
    if (getProperty == null) {
      val nsIndex = name.indexOf(':')
      val localName = if (nsIndex == -1) name else name.substring(nsIndex + 1)
      val index = sf.getFeatureType.indexOf(localName)
      if (index != -1) {
        getProperty = (sf) => sf.getAttribute(index)
      } else {
        import scala.collection.JavaConverters._
        val accessors = PropertyAccessors.findPropertyAccessors(sf, name, null, null).asScala
        if (accessors.nonEmpty) {
          getProperty = (sf) => {
            accessors.find(_.canHandle(sf, name, classOf[AnyRef])).map(_.get(sf, name, classOf[AnyRef])).orNull
          }
        } else {
          throw new RuntimeException(s"Property name $name does not exist in feature type ${sf.getFeatureType}")
        }
      }
    }
    getProperty(sf)
  }

  override def evaluate[T](obj: AnyRef, target: Class[T]): T = Converters.convert(evaluate(obj), target)

  override def accept(visitor: ExpressionVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)

  // required for some ECQL parsing
  override def toString: String = name
}
