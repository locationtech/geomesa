/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.locationtech.geomesa.convert.EvaluationContext
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
}

class MiscFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(lineNumber, withDefault, require, intToBoolean)

  private val withDefault = TransformerFunction.pure("withDefault")(MiscFunctionFactory.withDefault)

  private val require = TransformerFunction.pure("require")(MiscFunctionFactory.require)

  private val lineNumber = new NamedTransformerFunction(Seq("lineNo", "lineNumber")) {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = ctx.line
  }

  private val intToBoolean = TransformerFunction.pure("intToBoolean")(MiscFunctionFactory.intToBoolean)
}
