/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.expression

import org.opengis.filter.MultiValuedFilter.MatchAction
import org.opengis.filter.expression.{Expression, Literal}
import org.opengis.filter.{FilterVisitor, PropertyIsEqualTo}

abstract class FastPropertyIsEqualTo(exp1: Expression, exp2: Literal) extends PropertyIsEqualTo {

  override def accept(visitor: FilterVisitor, extraData: Any): AnyRef = visitor.visit(this, extraData)

  override def getExpression1: Expression = exp1

  override def getExpression2: Expression = exp2

  override def getMatchAction: MatchAction = MatchAction.ANY

  override def toString: String = s"[ $exp1 = $exp2 ]"
}

object FastPropertyIsEqualTo {

  class FastIsEqualTo(exp1: Expression, exp2: Literal) extends FastPropertyIsEqualTo(exp1, exp2) {
    private val lit = exp2.evaluate(null)
    override def evaluate(obj: Any): Boolean = lit == exp1.evaluate(obj)
    override def isMatchingCase: Boolean = true
  }

  class FastIsEqualToIgnoreCase(exp1: Expression, exp2: Literal) extends FastPropertyIsEqualTo(exp1, exp2) {
    private val lit = String.valueOf(exp2.evaluate(null))
    override def evaluate(obj: Any): Boolean = lit.equalsIgnoreCase(String.valueOf(exp1.evaluate(obj)))
    override def isMatchingCase: Boolean = false
  }

  // exp1 is expected to evaluate to a list, exp2 is expected to evaluate to a single value
  class FastListIsEqualToAny(exp1: Expression, exp2: Literal) extends FastPropertyIsEqualTo(exp1, exp2) {
    private val lit = exp2.evaluate(null)
    override def evaluate(obj: Any): Boolean = {
      val list = exp1.evaluate(obj).asInstanceOf[java.util.List[Any]]
      list != null && list.contains(lit)
    }
    override def isMatchingCase: Boolean = false
  }
}
