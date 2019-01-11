/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import java.util.ServiceLoader

import org.locationtech.geomesa.convert.EvaluationContext

trait TransformerFunction {
  def names: Seq[String]
  def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any
  // some transformers cache arguments that don't change, override getInstance in order
  // to return a new transformer that can cache args
  def getInstance: TransformerFunction = this
}

object TransformerFunction {

  import scala.collection.JavaConverters._

  lazy val functions: Map[String, TransformerFunction] = {
    val map = Map.newBuilder[String, TransformerFunction]
    // noinspection ScalaDeprecation
    ServiceLoader.load(classOf[org.locationtech.geomesa.convert.TransformerFunctionFactory]).asScala.foreach { factory =>
      factory.functions.foreach(f => f.names.foreach(n => map += n -> f))
    }
    ServiceLoader.load(classOf[TransformerFunctionFactory]).asScala.foreach { factory =>
      factory.functions.foreach(f => f.names.foreach(n => map += n -> f))
    }
    map.result()
  }

  def apply(n: String*)(f: (Array[Any]) => Any): TransformerFunction = {
    new NamedTransformerFunction(n) {
      override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = f(args)
    }
  }

  abstract class NamedTransformerFunction(override val names: Seq[String]) extends TransformerFunction
}
