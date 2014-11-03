/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
