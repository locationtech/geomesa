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

  /**
    * The unique names used to reference this function
    *
    * Generally a function should have one name, but we keep old names around for back-compatibility
    *
    * @return
    */
  def names: Seq[String]

  /**
    * Evaluate the function
    *
    * @param args arguments
    * @param ctx evalution context
    * @return
    */
  def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any

  /**
    * Returns an uninitialized instance of this function
    *
    * If the function caches state about its current context, this function should return an instance
    * without any state. Stateless functions can generally just return themselves
    *
    * @return
    */
  def getInstance: TransformerFunction = this

  /**
    * Is the a 'pure' function? Pure functions a) given the same inputs, always return the same result, and
    * b) do not have any observable side effects. In the context of converters, it also does not rely on the
    * evaluation context.
    *
    * If the function is pure, it may be optimized out if e.g. all its inputs are literals
    *
    * @return
    */
  def pure: Boolean = false
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

  def apply(n: String*)(f: Array[Any] => Any): TransformerFunction = {
    new NamedTransformerFunction(n) {
      override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = f(args)
    }
  }

  def pure(n: String*)(f: Array[Any] => Any): TransformerFunction = {
    new NamedTransformerFunction(n, pure = true) {
      override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = f(args)
    }
  }

  abstract class NamedTransformerFunction(override val names: Seq[String], override val pure: Boolean = false)
      extends TransformerFunction
}
