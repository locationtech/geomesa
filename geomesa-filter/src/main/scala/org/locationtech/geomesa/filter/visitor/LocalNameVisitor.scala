/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.filter.visitor

import org.geotools.filter.visitor.DuplicatingFilterVisitor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.PropertyName

class LocalNameVisitor(sft: SimpleFeatureType) extends DuplicatingFilterVisitor {
  override def visit(expression: PropertyName, extraData: AnyRef): AnyRef = {
    val name = expression.getPropertyName
    if (name == null || name.isEmpty) {
      // use the default geometry name
      getFactory(extraData).property(sft.getGeometryDescriptor.getLocalName, expression.getNamespaceContext)
    } else {
      val index = name.indexOf(':')
      if (index == -1) {
        expression
      } else {
        // strip off the namespace
        getFactory(extraData).property(name.substring(index + 1), expression.getNamespaceContext)
      }
    }
  }
}
