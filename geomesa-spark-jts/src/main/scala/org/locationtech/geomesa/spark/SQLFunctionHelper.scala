/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.functions.udf
import scala.reflect.runtime.universe._

object SQLFunctionHelper {
  def nullableUDF[A1, RT](f: A1 => RT): A1 => RT = {
    in1 => in1 match {
      case null => null.asInstanceOf[RT]
      case out1 => f(out1)
    }
  }

  def nullableUDF[A1, A2, RT](f: (A1, A2) => RT): (A1, A2) => RT = {
    (in1, in2) => (in1, in2) match {
      case (null, _) => null.asInstanceOf[RT]
      case (_, null) => null.asInstanceOf[RT]
      case (out1, out2) => f(out1, out2)
    }
  }

  def nullableUDF[A1, A2, A3, RT](f: (A1, A2, A3) => RT): (A1, A2, A3) => RT = {
    (in1, in2, in3) => (in1, in2, in3) match {
      case (null, _, _) => null.asInstanceOf[RT]
      case (_, null, _) => null.asInstanceOf[RT]
      case (_, _, null) => null.asInstanceOf[RT]
      case (out1, out2, out3) => f(out1, out2, out3)
    }
  }

  def nullableUDF[A1, A2, A3, A4, RT](f: (A1, A2, A3, A4) => RT): (A1, A2, A3, A4) => RT = {
    (in1, in2, in3, in4) => (in1, in2, in3, in4) match {
      case (null, _, _, _) => null.asInstanceOf[RT]
      case (_, null, _, _) => null.asInstanceOf[RT]
      case (_, _, null, _) => null.asInstanceOf[RT]
      case (_, _, _, null) => null.asInstanceOf[RT]
      case (out1, out2, out3, out4) => f(out1, out2, out3, out4)
    }
  }

  def udfToColumn[A1: TypeTag, RT: TypeTag](f: A1 => RT, name: String, col: Column): Column = {
    withAlias(name, col)(udf(f).apply(col))
  }
  def udfToColumn[A1: TypeTag, A2: TypeTag, RT: TypeTag](f: (A1, A2) => RT,
                                                         name: String,
                                                         colA: Column, colB: Column): Column = {
    withAlias(name, colA, colB)(udf(f).apply(colA, colB))
  }
  def udfToColumn[A1: TypeTag, A2: TypeTag, A3: TypeTag, RT: TypeTag](f: (A1, A2, A3) => RT,
                                                                      name: String,
                                                                      colA: Column, colB: Column, colC: Column): Column = {
    withAlias(name, colA, colB, colC)(udf(f).apply(colA, colB, colC))
  }
  def udfToColumn[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, RT: TypeTag](f: (A1, A2, A3, A4) => RT,
                                                                                   name: String,
                                                                                   colA: Column, colB: Column,
                                                                                   colC: Column, colD: Column): Column = {
    withAlias(name, colA, colB, colC)(udf(f).apply(colA, colB, colC, colD))
  }


  def udfToColumnLiterals[A1: TypeTag, RT: TypeTag](f: A1 => RT, name: String, a1: A1): Column = {
    udf(() => f(a1)).apply().as(name)
  }
  def udfToColumnLiterals[A1: TypeTag, A2: TypeTag, RT: TypeTag](f: (A1, A2) => RT,
                                                                 name: String,
                                                                 a1: A1, a2: A2): Column = {
    udf(() => f(a1, a2)).apply().as(name)
  }
  def udfToColumnLiterals[A1: TypeTag, A2: TypeTag, A3: TypeTag, RT: TypeTag](f: (A1, A2, A3) => RT,
                                                                              name: String,
                                                                              a1: A1, a2: A2, a3: A3): Column = {
    udf(() => f(a1, a2, a3)).apply().as(name)
  }
  def udfToColumnLiterals[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, RT: TypeTag](f: (A1, A2, A3, A4) => RT,
                                                                                           name: String,
                                                                                           a1: A1, a2: A2, a3: A3, a4: A4): Column = {
    udf(() => f(a1, a2, a3, a4)).apply().as(name)
  }

  def columnName(column: Column): String = {
    column.expr match {
      case ua: UnresolvedAttribute ⇒ ua.name
      case ar: AttributeReference ⇒ ar.name
      case as: Alias ⇒ as.name
      case o ⇒ o.prettyName
    }
  }

  def withAlias(name: String, inputs: Column*)(output: Column): Column = {
    val paramNames = inputs.map(columnName).mkString(",")
    output.as(s"$name($paramNames)")
  }
}
