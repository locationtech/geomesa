/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.vividsolutions.jts.geom.{Geometry, Point, Polygon}
import org.apache.spark.sql.{Column, Encoder, TypedColumn}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, Encoder, TypedColumn}

import scala.reflect.runtime.universe._

// This should be some level of package private, but there's a dependency on it
// from org.apache.spark.sql.SQLGeometricConstructorFunctions, which could/should be moved
// into a org.locationtech.geomesa package eventually, and this access restriction reenabled
/*private[geomesa]*/ object SQLFunctionHelper {
import org.apache.spark.sql.jts.{PointUDT, PolygonUDT}

import scala.reflect.runtime.universe._

private[geomesa] object SQLFunctionHelper {
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

  def udfToColumn[A1: TypeTag, RT: TypeTag: Encoder, N >: (A1 => RT)](
    f: A1 => RT, namer: N => String, col: Column): TypedColumn[Any, RT] = {
    withAlias(namer(f), col)(udf(f).apply(col)).as[RT]
  }

  def udfToColumn[A1: TypeTag, A2: TypeTag, RT: TypeTag: Encoder, N >: (A1, A2) => RT](
    f: (A1, A2) => RT, namer: N => String, colA: Column, colB: Column): TypedColumn[Any, RT] = {
    withAlias(namer(f), colA, colB)(udf(f).apply(colA, colB)).as[RT]
  }

  def udfToColumn[A1: TypeTag, A2: TypeTag, A3: TypeTag, RT: TypeTag: Encoder, N >: (A1, A2, A3) => RT](
    f: (A1, A2, A3) => RT, namer: N => String, colA: Column, colB: Column, colC: Column): TypedColumn[Any, RT] = {
    withAlias(namer(f), colA, colB, colC)(udf(f).apply(colA, colB, colC)).as[RT]
  }

  def udfToColumn[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, RT: TypeTag: Encoder, N >: (A1, A2, A3, A4) => RT](
    f: (A1, A2, A3, A4) => RT, namer: N => String,
    colA: Column, colB: Column, colC: Column, colD: Column): TypedColumn[Any, RT] = {
    withAlias(namer(f), colA, colB, colC)(udf(f).apply(colA, colB, colC, colD)).as[RT]
  }

  def udfToColumnLiterals[A1: TypeTag, RT: TypeTag: Encoder, N >: A1 => RT](
    f: A1 => RT, namer: N => String, a1: A1): TypedColumn[Any, RT] = {
    udf(() => f(a1)).apply().as(namer(f)).as[RT]
  }

  def udfToColumnLiterals[A1: TypeTag, A2: TypeTag, RT: TypeTag: Encoder, N >: (A1, A2) => RT](
    f: (A1, A2) => RT, namer: N => String, a1: A1, a2: A2): TypedColumn[Any, RT] = {
    udf(() => f(a1, a2)).apply().as(namer(f)).as[RT]
  }

  def udfToColumnLiterals[A1: TypeTag, A2: TypeTag, A3: TypeTag, RT: TypeTag: Encoder, N >: (A1, A2, A3) => RT](
    f: (A1, A2, A3) => RT, namer: N => String, a1: A1, a2: A2, a3: A3): TypedColumn[Any, RT] = {
    udf(() => f(a1, a2, a3)).apply().as(namer(f)).as[RT]
  }

  def udfToColumnLiterals[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, RT: TypeTag: Encoder, N >: (A1, A2, A3, A4) => RT](
    f: (A1, A2, A3, A4) => RT, namer: N => String, a1: A1, a2: A2, a3: A3, a4: A4): TypedColumn[Any, RT] = {
    udf(() => f(a1, a2, a3, a4)).apply().as(namer(f)).as[RT]
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
