/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

class MathFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(add, subtract, multiply, divide, mean, min, max)

  private val add = TransformerFunction("add") { args =>
    var s: Double = 0.0
    args.foreach(s += parseDouble(_))
    s
  }

  private val multiply = TransformerFunction("multiply") { args =>
    var s: Double = 1.0
    args.foreach(s *= parseDouble(_))
    s
  }

  private val subtract = TransformerFunction("subtract") { args =>
    var s: Double = parseDouble(args(0))
    args.drop(1).foreach(s -= parseDouble(_))
    s
  }

  private val divide = TransformerFunction("divide") { args =>
    var s: Double = parseDouble(args(0))
    args.drop(1).foreach(s /= parseDouble(_))
    s
  }

  private val mean = TransformerFunction("mean") { args =>
    if (args.length == 0) { 0d } else {
      var count = 0d
      args.map(parseDouble).foreach(d => count += d)
      count / args.length
    }
  }

  private val min = TransformerFunction("min") { args =>
    var min = java.lang.Double.POSITIVE_INFINITY
    args.map(parseDouble).foreach(d => if (min > d) { min = d })
    min
  }

  private val max = TransformerFunction("max") { args =>
    var max = java.lang.Double.NEGATIVE_INFINITY
    args.map(parseDouble).foreach(d => if (max < d) { max = d })
    max
  }


  private def parseDouble(v: Any): Double = {
    v match {
      case n: Int    => n.toDouble
      case n: Double => n
      case n: Float  => n.toDouble
      case n: Long   => n.toDouble
      case n: String => n.toDouble
      case n: Any    => n.toString.toDouble
    }
  }
}
