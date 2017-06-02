/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.filters

import org.opengis.filter.BinaryComparisonOperator
import org.opengis.filter.expression.Expression
import org.opengis.filter.temporal.BinaryTemporalOperator

object Typeclasses {
  object BinaryFilter {
    implicit object BTOIsBinaryFilter extends BinaryFilter[BinaryTemporalOperator] {
      def left(bto: BinaryTemporalOperator): Expression = bto.getExpression1
      def right(bto: BinaryTemporalOperator): Expression = bto.getExpression2
    }

    implicit object BCOIsBinaryFilter extends BinaryFilter[BinaryComparisonOperator] {
      def left(bco: BinaryComparisonOperator): Expression = bco.getExpression1
      def right(bco: BinaryComparisonOperator): Expression = bco.getExpression2
    }

    implicit def ops[B](b: B)(implicit bf: BinaryFilter[B]) = new bf.Ops(b)
  }

  trait BinaryFilter[-B] {
    def left(b: B): Expression
    def right(b: B): Expression

    class Ops(self: B) {
      def left:  Expression = BinaryFilter.this.left(self)
      def right: Expression = BinaryFilter.this.right(self)
    }
  }
}
