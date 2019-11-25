/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction

object MiscFunctionFactory {
  def intToBoolean(value: Any): Any = {
    if (value == null) null else value.asInstanceOf[Int] != 0
  }
  def withDefault(value: Any, default: Any): Any = {
    if (value == null) default else value
  }
  def require(value: Any): Any = {
    if (value == null) {
      throw new IllegalArgumentException("Required field is null")
    }
    value
  }
}

class MiscFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(lineNumber, withDefault, require, intToBoolean)

  private val withDefault = TransformerFunction.pure("withDefault") { args =>
    MiscFunctionFactory.withDefault(args(0), args(1))
  }

  private val require = TransformerFunction.pure("require") { args =>
    MiscFunctionFactory.require(args(0))
  }

  private val lineNumber = new NamedTransformerFunction(Seq("lineNo", "lineNumber")) {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = ctx.line
  }

  private val intToBoolean = TransformerFunction.pure("intToBoolean") { args =>
    MiscFunctionFactory.intToBoolean(args(0))
  }
}
