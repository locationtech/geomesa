/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.Aggregate

object Aggregates {
  /** Create a new instance of Aggregate allowing to pass varargs */
  def instance(args: Any*): Aggregate = {
    import scala.reflect.runtime.{universe => ru}
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val objSymbol = rm.staticModule("org.apache.spark.sql.catalyst.plans.logical.Aggregate")
    val objMirror = rm.reflectModule(objSymbol)
    val obj = objMirror.instance
    val objTyp = objSymbol.typeSignature
    val applyTerm = objTyp.decl(ru.TermName("apply"))
    require(applyTerm.isMethod, "Aggregate.apply is not defined")
    val methodSymbol = applyTerm.asMethod
    val instanceMirror = rm.reflect(obj)
    val methodMirror = instanceMirror.reflectMethod(methodSymbol)
    val size = methodMirror.symbol.paramLists.head.size
    require(size <= args.length, s"Aggregate.apply requires $size arguments but was passed at most ${args.length}")
    methodMirror(args.take(size): _*).asInstanceOf[Aggregate]
  }
}
