/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark


import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import scala.reflect.runtime.universe._

trait SparkDefaultEncoders {
  implicit def stringEncoder = ExpressionEncoder[String]()
  implicit def floatEncoder = ExpressionEncoder[Float]()
  implicit def intEncoder = ExpressionEncoder[Int]()
  implicit def booleanEncoder = ExpressionEncoder[Boolean]()
  implicit def arrayEncoder[T: TypeTag] = ExpressionEncoder[Array[T]]()
}

object SparkDefaultEncoders extends SparkDefaultEncoders