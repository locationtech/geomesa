/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert.json

import com.google.gson.JsonElement
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.locationtech.geomesa.convert.{TransformerFn, TransformerFunctionFactory}

class JsonFunctionFactory extends TransformerFunctionFactory {
  private val json2string = new TransformerFn {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      args(0).asInstanceOf[JsonElement].toString

    override def name: String = "json2string"
  }

  override val functions: Seq[TransformerFn] = Seq(json2string)
}
