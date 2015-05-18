/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.filter.factory

import org.locationtech.geomesa.filter.expression.FastPropertyName
import org.opengis.feature.`type`.Name
import org.opengis.filter.FilterFactory2
import org.opengis.filter.expression.PropertyName
import org.xml.sax.helpers.NamespaceSupport

class FastFilterFactory extends org.geotools.filter.FilterFactoryImpl with FilterFactory2 {

  override def property(name: String): PropertyName = new FastPropertyName(name)

  override def property(name: Name): PropertyName = property(name.getLocalPart)

  override def property(name: String, namespaceContext: NamespaceSupport): PropertyName = property(name)
}
