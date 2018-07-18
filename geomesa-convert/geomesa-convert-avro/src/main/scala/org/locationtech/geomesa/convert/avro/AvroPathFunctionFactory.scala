/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import org.apache.avro.generic.GenericRecord
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{TransformerFunction, TransformerFunctionFactory}

class AvroPathFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(avroPath)

  private val avroPath = new AvroPathFn()

  class AvroPathFn extends NamedTransformerFunction(Seq("avroPath")) {
    private var path: AvroPath = _
    override def getInstance: AvroPathFn = new AvroPathFn()
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if (path == null) {
        path = AvroPath(args(1).asInstanceOf[String])
      }
      path.eval(args(0).asInstanceOf[GenericRecord]).orNull
    }
  }
}
