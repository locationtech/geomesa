/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.encoders

import java.lang

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe._

/**
 * These encoders exist only for simplifying the construction of DataFrame/Dataset DSL
 * functions. End users should get their default encoders via the Spark recommended
 * pattern:
 * {{{
 *   val spark: SparkSession = ...
 *   import spark.implicits._
 * }}}
 *
 */
private[jts] trait SparkDefaultEncoders {
  implicit def stringEncoder: Encoder[String] = Encoders.STRING
  implicit def jFloatEncoder: Encoder[lang.Float] = Encoders.FLOAT
  implicit def doubleEncoder: Encoder[Double] = Encoders.scalaDouble
  implicit def jDoubleEncoder: Encoder[lang.Double] = Encoders.DOUBLE
  implicit def intEncoder: Encoder[Int] = Encoders.scalaInt
  implicit def jBooleanEncoder: Encoder[lang.Boolean] = Encoders.BOOLEAN
  implicit def booleanEncoder: Encoder[Boolean] = Encoders.scalaBoolean
  implicit def arrayEncoder[T: TypeTag]: Encoder[Array[T]] = ExpressionEncoder()
}
private[jts] object SparkDefaultEncoders extends SparkDefaultEncoders
