/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.json

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.expression.PropertyName
import org.geotools.filter.visitor.PropertyNameResolvingVisitor

/**
 * Resolves property names against the schema, but leaves `$`-prefixed JSON paths untouched so that
 * IcebergFilterConverter can translate them into pushed-down predicates on nested struct fields.
 *
 * @param featureType feature type
 */
class JsonPathPropertyNameResolver(featureType: SimpleFeatureType) extends PropertyNameResolvingVisitor(featureType) {
  override def visit(expression: PropertyName, extraData: AnyRef): AnyRef = {
    val name = expression.getPropertyName
    if (name != null && name.startsWith("$")) {
      getFactory(extraData).property(name)
    } else {
      super.visit(expression, extraData)
    }
  }
}
