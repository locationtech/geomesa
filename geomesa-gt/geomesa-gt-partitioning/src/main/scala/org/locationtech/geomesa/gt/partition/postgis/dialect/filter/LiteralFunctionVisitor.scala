/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect.filter

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.FilterAttributeExtractor
import org.geotools.filter.visitor.DuplicatingFilterVisitor
import org.opengis.filter.Filter
import org.opengis.filter.expression.Function

/**
 * Replaces 'constant' functions with their literal values
 */
class LiteralFunctionVisitor extends DuplicatingFilterVisitor {
  override def visit(expression: Function, extraData: Any): AnyRef = {
    val extractor = new FilterAttributeExtractor()
    expression.accept(extractor, null)
    if (extractor.isConstantExpression) {
      val literal = expression.evaluate(null)
      LiteralFunctionVisitor.ff.literal(literal)
    } else {
      super.visit(expression, extraData)
    }
  }
}

object LiteralFunctionVisitor {

  private val ff = CommonFactoryFinder.getFilterFactory2()

  def apply(filter: Filter): Filter = filter.accept(new LiteralFunctionVisitor(), null).asInstanceOf[Filter]
}
