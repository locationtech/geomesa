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

class EnrichmentCacheFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(cacheLookup)

  private val cacheLookup = new NamedTransformerFunction(Seq("cacheLookup")) {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      val cache = ctx.getCache(args(0).asInstanceOf[String])
      cache.get(Array(args(1).asInstanceOf[String], args(2).asInstanceOf[String]))
    }
  }
}
