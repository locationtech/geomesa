/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.expression

import org.opengis.filter.MultiValuedFilter.MatchAction
import org.opengis.filter.expression.Expression
import org.opengis.filter.{FilterVisitor, PropertyIsEqualTo}

class FastIsEqualTo(exp1: Expression, exp2: Expression) extends PropertyIsEqualTo {

  override def evaluate(obj: Any): Boolean = exp1.evaluate(obj) == exp2.evaluate(obj)

  override def accept(visitor: FilterVisitor, extraData: Any): AnyRef = visitor.visit(this, extraData)

  override def getExpression1: Expression = exp1

  override def getExpression2: Expression = exp2

  override def isMatchingCase: Boolean = true

  override def getMatchAction: MatchAction = MatchAction.ANY
}

class FastIsEqualToIgnoreCase(exp1: Expression, exp2: Expression) extends PropertyIsEqualTo {

  override def evaluate(obj: Any): Boolean =
    String.valueOf(exp1.evaluate(obj)).equalsIgnoreCase(String.valueOf(exp2.evaluate(obj)))

  override def accept(visitor: FilterVisitor, extraData: Any): AnyRef = visitor.visit(this, extraData)

  override def getExpression1: Expression = exp1

  override def getExpression2: Expression = exp2

  override def isMatchingCase: Boolean = false

  override def getMatchAction: MatchAction = MatchAction.ANY
}

// exp1 is expected to evaluate to a list, exp2 is expected to evaluate to a single value
class FastListIsEqualToAny(exp1: Expression, exp2: Expression) extends PropertyIsEqualTo {

  override def evaluate(obj: Any): Boolean = {
    val left = exp1.evaluate(obj).asInstanceOf[java.util.List[Any]]
    val right = exp2.evaluate(obj)
    left != null && left.contains(right)
  }

  override def accept(visitor: FilterVisitor, extraData: Any): AnyRef = visitor.visit(this, extraData)

  override def getExpression1: Expression = exp1

  override def getExpression2: Expression = exp2

  override def isMatchingCase: Boolean = false

  override def getMatchAction: MatchAction = MatchAction.ANY
}
