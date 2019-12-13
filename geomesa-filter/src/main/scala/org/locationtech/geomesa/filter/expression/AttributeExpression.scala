/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.expression

import org.opengis.filter.expression.{Function, Literal}

/**
  * Expressions operating on attributes
  */
sealed trait AttributeExpression {

  /**
    * Attribute name
    *
    * @return
    */
  def name: String

  /**
    * Literal value the attribute is compared to
    *
    * @return
    */
  def literal: Literal

  /**
    * Attribute name comes second, or not. i.e. foo > 5 -> flipped = false, 5 > foo -> flipped = true
    *
    * @return
    */
  def flipped: Boolean
}

object AttributeExpression {

  /**
    * Holder for a property name, literal value(s), and the order they are in
    *
    * @param name attribute name
    * @param literal literal value
    * @param flipped if the literal comes first, e.g. 5 < foo vs foo > 5
    */
  case class PropertyLiteral(name: String, literal: Literal, flipped: Boolean = false)
      extends AttributeExpression

  /**
    * Holder for a function, literal value, and the order they are in
    *
    * @param name attribute name that the function is operating on
    * @param function function
    * @param literal literal value
    * @param flipped if the literal comes first, e.g. 5 < foo vs foo > 5
    */
  case class FunctionLiteral(name: String, function: Function, literal: Literal, flipped: Boolean = false)
      extends AttributeExpression
}
