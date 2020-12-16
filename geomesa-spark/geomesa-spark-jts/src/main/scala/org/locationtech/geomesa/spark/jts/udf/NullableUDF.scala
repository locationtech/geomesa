/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import java.util.Locale

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, Encoder, SQLContext, TypedColumn}
import org.locationtech.geomesa.spark.jts.udf.UDFFactory.Registerable

import scala.reflect.runtime.universe._

abstract class NullableUDF[RT: TypeTag: Encoder] extends Registerable with Serializable {

  // st_camelCase
  val name: String = {
    val simple = getClass.getSimpleName
    if (simple.length < 4) { simple } else {
      simple.substring(0, 4).toLowerCase(Locale.US) + simple.substring(4)
    }
  }

  protected def function: UserDefinedFunction

  def toColumn(cols: Column*): TypedColumn[Any, RT] =
    function.apply(cols: _*).as(s"$name(${cols.map(NullableUDF.columnName).mkString(",")})").as[RT]
}


// This should be some level of package private, but there's a dependency on it
// from org.apache.spark.sql.SQLGeometricConstructorFunctions, which could/should be moved
// into a org.locationtech.geomesa package eventually, and this access restriction reenabled
/*private[geomesa]*/

object NullableUDF {

  class NullableUDF1[A1: TypeTag, RT: TypeTag: Encoder](f: A1 => RT)
      extends NullableUDF[RT] with (A1 => RT) {
    override protected def function: UserDefinedFunction = udf(apply _)
    override def register(sqlContext: SQLContext): Unit = sqlContext.udf.register(name, this)
    override def apply(v1: A1): RT = if (v1 == null) { null.asInstanceOf[RT] } else { f(v1) }
  }

  class NullableUDF2[A1: TypeTag, A2: TypeTag, RT: TypeTag: Encoder](f: (A1, A2) => RT)
      extends NullableUDF[RT] with ((A1, A2) => RT) {
    override protected def function: UserDefinedFunction = udf(apply _)
    override def register(sqlContext: SQLContext): Unit = sqlContext.udf.register(name, this)
    override def apply(v1: A1, v2: A2): RT =
      if (v1 == null || v2 == null) { null.asInstanceOf[RT] } else { f(v1, v2) }
  }

  class NullableUDF3[A1: TypeTag, A2: TypeTag, A3: TypeTag, RT: TypeTag: Encoder](f: (A1, A2, A3) => RT)
      extends NullableUDF[RT] with ((A1, A2, A3) => RT) {
    override protected def function: UserDefinedFunction = udf(apply _)
    override def register(sqlContext: SQLContext): Unit = sqlContext.udf.register(name, this)
    override def apply(v1: A1, v2: A2, v3: A3): RT =
      if (v1 == null || v2 == null || v3 == null) { null.asInstanceOf[RT] } else { f(v1, v2, v3) }
  }

  class NullableUDF4[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, RT: TypeTag: Encoder](f: (A1, A2, A3, A4) => RT)
      extends NullableUDF[RT] with ((A1, A2, A3, A4) => RT) {
    override protected def function: UserDefinedFunction = udf(apply _)
    override def register(sqlContext: SQLContext): Unit = sqlContext.udf.register(name, this)
    override def apply(v1: A1, v2: A2, v3: A3, v4: A4): RT =
      if (v1 == null || v2 == null || v3 == null || v4 == null) { null.asInstanceOf[RT] } else { f(v1, v2, v3, v4) }
  }

  def nullableUDF[A1: TypeTag, RT: TypeTag: Encoder](f: A1 => RT, name: String): NullableUDF1[A1, RT] = {
    val n = name
    new NullableUDF1(f) {
      override val name: String = n
    }
  }

  def nullableUDF[A1: TypeTag, A2: TypeTag, RT: TypeTag: Encoder](f: (A1, A2) => RT, name: String): NullableUDF2[A1, A2, RT] = {
    val n = name
    new NullableUDF2(f) {
      override val name: String = n
    }
  }

  def nullableUDF[A1: TypeTag, A2: TypeTag, A3: TypeTag, RT: TypeTag: Encoder](f: (A1, A2, A3) => RT, name: String): NullableUDF3[A1, A2, A3, RT] = {
    val n = name
    new NullableUDF3(f) {
      override val name: String = n
    }
  }

  def nullableUDF[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, RT: TypeTag: Encoder](f: (A1, A2, A3, A4) => RT, name: String): NullableUDF4[A1, A2, A3, A4, RT] = {
    val n = name
    new NullableUDF4(f) {
      override val name: String = n
    }
  }

  private def columnName(column: Column): String = {
    column.expr match {
      case ua: UnresolvedAttribute ⇒ ua.name
      case ar: AttributeReference ⇒ ar.name
      case as: Alias ⇒ as.name
      case o ⇒ o.prettyName
    }
  }
}
