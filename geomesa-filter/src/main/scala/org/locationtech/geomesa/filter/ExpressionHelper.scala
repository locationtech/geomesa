/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import org.geotools.filter.visitor.DefaultExpressionVisitor
import org.opengis.filter.expression.{Expression, PropertyName}

object ExpressionHelper {

  private val propertyNameExtractor = new PropertyNameExtractor

  /**
    * Extracts property names from arbitrary expressions, which may be nested, contain functions, etc
    *
    * @param expression expression to evaluate
    * @return
    */
  def propertyNames(expression: Expression): Set[String] =
    expression.accept(propertyNameExtractor, Set.empty[String]).asInstanceOf[Set[String]]

  /**
    * Visitor for extracting property names from expressions
    */
  class PropertyNameExtractor extends DefaultExpressionVisitor {
    override def visit(expression: PropertyName, data: AnyRef): AnyRef = {
      val names = data match {
        case d: Set[String] => d + expression.getPropertyName
        case _ => Set(expression.getPropertyName)
      }
      super.visit(expression, names)
    }
  }
}
