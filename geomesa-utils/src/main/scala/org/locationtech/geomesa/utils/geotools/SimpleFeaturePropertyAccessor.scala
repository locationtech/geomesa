/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.expression.{PropertyAccessor, PropertyAccessors}
import org.opengis.feature.simple.SimpleFeature

object SimpleFeaturePropertyAccessor extends LazyLogging {
  def getAccessor(sf: SimpleFeature, name: String): Option[PropertyAccessor] ={
    // some mojo to ensure our property accessor is picked up -
    // our accumulo iterators are not generally available in the system classloader
    // instead, we can set the context classloader (as that will be checked if set)
    val contextClassLoader = Thread.currentThread.getContextClassLoader
    if (contextClassLoader != null) {
      logger.warn(s"Bypassing context classloader $contextClassLoader for PropertyAccessor loading.")
    }
    Thread.currentThread.setContextClassLoader(getClass.getClassLoader)
    try {
      import scala.collection.JavaConversions._
      PropertyAccessors.findPropertyAccessors(sf, name, null, null).headOption
    } finally {
      // reset the classloader after loading the accessors
      Thread.currentThread.setContextClassLoader(contextClassLoader)
    }
  }
}
