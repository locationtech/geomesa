/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
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
    var i = 0
    while (i < args.length) {
      if (args(i) != null) {
        return args(i)
      }
      i += 1
    }
    null
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
    // noinspection ScalaDeprecation
    override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = ec.line
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
    // noinspection ScalaDeprecation
    override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = ec.line
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
  }
}

class MiscFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(lineNumber, withDefault, require, intToBoolean)

  private val lineNumber = new LineNumber(null)

  private val withDefault = TransformerFunction.pure("withDefault")(MiscFunctionFactory.withDefault)

  private val require = TransformerFunction.pure("require")(MiscFunctionFactory.require)

  private val intToBoolean = TransformerFunction.pure("intToBoolean")(MiscFunctionFactory.intToBoolean)
}
