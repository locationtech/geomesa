/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.MiscFunctionFactory.LineNumber
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction

object MiscFunctionFactory {
  def intToBoolean(args: Array[Any]): Any = {
    if (args(0) == null) null else args(0).asInstanceOf[Int] != 0
  }
  def withDefault(args: Array[Any]): Any = {
    if (args(0) == null) args(1) else args(0)
  }
  def require(args: Array[Any]): Any = {
    if (args(0) == null) {
      throw new IllegalArgumentException("Required field is null")
    }
    args(0)
  }

  class LineNumber(ec: EvaluationContext) extends NamedTransformerFunction(Seq("lineNo", "lineNumber")) {
    override def apply(args: Array[AnyRef]): AnyRef = Long.box(ec.line)
    override def withContext(ec: EvaluationContext): TransformerFunction = new LineNumber(ec)
    // noinspection ScalaDeprecation
    override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = ec.line
  }
}

class MiscFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(lineNumber, withDefault, require, intToBoolean)

  private val lineNumber = new LineNumber(null)

  private val withDefault = TransformerFunction.pure("withDefault")(MiscFunctionFactory.withDefault)

  private val require = TransformerFunction.pure("require")(MiscFunctionFactory.require)

  private val intToBoolean = TransformerFunction.pure("intToBoolean")(MiscFunctionFactory.intToBoolean)
}
