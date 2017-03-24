/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

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
}
