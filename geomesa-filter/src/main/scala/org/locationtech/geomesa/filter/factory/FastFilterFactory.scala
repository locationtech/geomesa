/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.factory

import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.expression.FastPropertyName
import org.opengis.feature.`type`.Name
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.{Filter, FilterFactory2}
import org.xml.sax.helpers.NamespaceSupport

/**
 * Filter factory that overrides property name
 */
class FastFilterFactory extends org.geotools.filter.FilterFactoryImpl with FilterFactory2 {

  override def property(name: String): PropertyName = new FastPropertyName(name)

  override def property(name: Name): PropertyName = property(name.getLocalPart)

  override def property(name: String, namespaceContext: NamespaceSupport): PropertyName = property(name)
}

object FastFilterFactory {

  val factory = new FastFilterFactory

  def toFilter(ecql: String): Filter = ECQL.toFilter(ecql, factory)
}