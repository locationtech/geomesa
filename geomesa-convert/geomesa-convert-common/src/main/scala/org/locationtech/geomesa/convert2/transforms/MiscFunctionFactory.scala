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

class MiscFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(lineNumber, withDefault)

  private val withDefault = TransformerFunction.pure("withDefault") { args =>
    if (args(0) == null) { args(1) } else { args(0) }
  }

  private val lineNumber = new NamedTransformerFunction(Seq("lineNo", "lineNumber")) {
    def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = ctx.counter.getLineCount
  }
}
