/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.EvaluationContext.ContextDependent

import java.util.ServiceLoader

<<<<<<< HEAD
<<<<<<< HEAD
=======
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.EvaluationContext.ContextDependent
import org.locationtech.geomesa.convert2.AbstractConverter.TransformerFunctionApiError
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.DelegateFunction

<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
trait TransformerFunction extends ContextDependent[TransformerFunction] {

  /**
    * The unique names used to reference this function
    *
    * Generally a function should have one name, but we keep old names around for back-compatibility
    *
    * @return
    */
  def names: Seq[String]

  /**
   * Evaluate the expression against an input row
   *
   * @param args arguments
   * @return
   */
  def apply(args: Array[AnyRef]): AnyRef
<<<<<<< HEAD
=======
  def apply(args: Array[AnyRef]): AnyRef =
    // this error will be caught and handled by the evaluation context
    throw TransformerFunctionApiError // TODO remove default impl in next major release
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)

  /**
    * Returns an uninitialized instance of this function
    *
    * If the function caches state about its current context, this function should return an instance
    * without any state. Stateless functions can generally just return themselves
    *
    * @return
    */
  def getInstance(args: List[Expression]): TransformerFunction = this

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
    ServiceLoader.load(classOf[TransformerFunctionFactory]).asScala.foreach { factory =>
      factory.functions.foreach(f => f.names.foreach(n => map += n -> f))
    }
    map.result()
  }

  def apply(n: String*)(f: Array[Any] => Any): TransformerFunction = {
    new NamedTransformerFunction(n) {
<<<<<<< HEAD
<<<<<<< HEAD
=======
      override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = f(args)
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
      override def apply(args: Array[AnyRef]): AnyRef = f(args.asInstanceOf[Array[Any]]).asInstanceOf[AnyRef]
    }
  }

  def pure(n: String*)(f: Array[Any] => Any): TransformerFunction = {
    new NamedTransformerFunction(n, pure = true) {
<<<<<<< HEAD
<<<<<<< HEAD
=======
      override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = f(args)
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
      override def apply(args: Array[AnyRef]): AnyRef = f(args.asInstanceOf[Array[Any]]).asInstanceOf[AnyRef]
    }
  }

  abstract class NamedTransformerFunction(override val names: Seq[String], override val pure: Boolean = false)
      extends TransformerFunction {
    override def withContext(ec: EvaluationContext): TransformerFunction = this
    override def apply(args: Array[AnyRef]): AnyRef
<<<<<<< HEAD
=======
    override def apply(args: Array[AnyRef]): AnyRef =
      eval(args.asInstanceOf[Array[Any]])(null).asInstanceOf[AnyRef] // TODO remove this in next major release
  }

  class DelegateFunction(delegate: TransformerFunction, ec: EvaluationContext) extends TransformerFunction {
    override def names: Seq[String] = delegate.names
    override def apply(args: Array[AnyRef]): AnyRef =
      delegate.eval(args.asInstanceOf[Array[Any]])(ec).asInstanceOf[AnyRef]
    override def getInstance(args: List[Expression]): TransformerFunction =
      new DelegateFunction(delegate.getInstance(args), ec)
    override def pure: Boolean = delegate.pure
    override def withContext(ec: EvaluationContext): TransformerFunction = new DelegateFunction(delegate, ec)
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
  }
}
