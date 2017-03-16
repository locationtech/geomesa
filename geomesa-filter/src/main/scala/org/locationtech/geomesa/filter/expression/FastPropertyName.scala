/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter.expression

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.expression.PropertyAccessors
import org.geotools.util.Converters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.{ExpressionVisitor, PropertyName}
import org.xml.sax.helpers.NamespaceSupport

/**
 * Implementation of property name that looks up the value by index
 */
class FastPropertyName(name: String)
    extends PropertyName with org.opengis.filter.expression.Expression with LazyLogging {

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
        // some mojo to ensure our property accessor is picked up -
        // our accumulo iterators are not generally available in the system classloader
        // instead, we can set the context classloader (as that will be checked if set)
        val contextClassLoader = Thread.currentThread.getContextClassLoader
        if (contextClassLoader != null) {
          logger.warn(s"Bypassing context classloader $contextClassLoader for PropertyAccessor loading")
        }
        Thread.currentThread.setContextClassLoader(classOf[FastPropertyName].getClassLoader)
        val accessor = try {
          import scala.collection.JavaConversions._
          PropertyAccessors.findPropertyAccessors(sf, name, null, null).find(_.canHandle(sf, name, classOf[AnyRef]))
        } finally {
          // reset the classloader after loading the accessors
          Thread.currentThread.setContextClassLoader(contextClassLoader)
        }
        accessor match {
          case Some(a) => getProperty = (sf) => a.get(sf, name, classOf[AnyRef])
          case None    => throw new RuntimeException(s"Can't handle property '$name' for feature type " +
            s"${sf.getFeatureType.getTypeName} ${SimpleFeatureTypes.encodeType(sf.getFeatureType)}")
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
