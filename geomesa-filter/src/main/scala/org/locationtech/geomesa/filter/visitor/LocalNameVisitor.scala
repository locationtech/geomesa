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

trait LocalNameVisitor extends DuplicatingFilterVisitor {

  def sft: SimpleFeatureType

  override def visit(expression: PropertyName, extraData: AnyRef): AnyRef = {
    val name = expression.getPropertyName
    if (name == null || name.isEmpty) {
      // use the default geometry name
      val geomName = sft.getGeometryDescriptor.getLocalName
      super.getFactory(extraData).property(geomName, expression.getNamespaceContext)
    } else {
      val index = name.indexOf(':')
      if (index == -1) {
        expression
      } else {
        // strip off the namespace
        super.getFactory(extraData).property(name.substring(index + 1), expression.getNamespaceContext)
      }
    }
  }
}

class LocalNameVisitorImpl(val sft: SimpleFeatureType) extends LocalNameVisitor
