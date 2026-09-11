/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.filter.FilterAttributeExtractor

/**
 * Helper class that can extract attributes from non-standard expressions
 *
 * @param sft simple feature type
 */
class AttributeExtractingVisitor(sft: SimpleFeatureType) extends FilterAttributeExtractor(sft) {
  override def visit(expression: org.geotools.api.filter.expression.Function, data: AnyRef): AnyRef = {
    expression match {
      case e: AttributeAwareExpression => e.visit(this, data)
      case _ => super.visit(expression, data)
    }
  }
}
