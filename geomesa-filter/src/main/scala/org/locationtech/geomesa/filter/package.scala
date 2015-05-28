/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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
package org.locationtech.geomesa

import org.opengis.filter.expression.{Expression, Literal, PropertyName}

package object filter {

  /**
   * Checks the order of properties and literals in the expression
   *
   * @param one
   * @param two
   * @return (prop, literal, whether the order was flipped)
   */
  def checkOrder(one: Expression, two: Expression): Option[PropertyLiteral] =
    (one, two) match {
      case (p: PropertyName, l: Literal) => Some(PropertyLiteral(p.getPropertyName, l, None, flipped = false))
      case (l: Literal, p: PropertyName) => Some(PropertyLiteral(p.getPropertyName, l, None, flipped = true))
      case (_: PropertyName, _: PropertyName) | (_: Literal, _: Literal) => None
      case _ =>
        val msg = s"Unhandled expressions in strategy: ${one.getClass.getName}, ${two.getClass.getName}"
        throw new RuntimeException(msg)

    }

  /**
   * Checks the order of properties and literals in the expression - if the expression does not contain
   * a property and a literal, throws an exception.
   *
   * @param one
   * @param two
   * @return
   */
  def checkOrderUnsafe(one: Expression, two: Expression): PropertyLiteral =
    checkOrder(one, two)
      .getOrElse(throw new RuntimeException("Expressions did not contain valid property and literal"))
}
