/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

class MathFunctionFactory extends TransformerFunctionFactory {

  import scala.collection.JavaConverters._

  override def functions: Seq[TransformerFunction] = Seq(add, subtract, multiply, divide, mean, min, max)

  private val add = TransformerFunction.pure("add") { args =>
    var s: Double = 0.0
    if (args.length != 0) {
      val numbers: scala.collection.Seq[Any] = args(0) match {
        case list: java.util.List[_] => list.asScala
        case _ => args
      }
      numbers.foreach(s += parseDouble(_))
    }
    s
  }

  private val multiply = TransformerFunction.pure("multiply") { args =>
    var s: Double = 1.0
    if (args.length != 0) {
      val numbers: scala.collection.Seq[Any] = args(0) match {
        case list: java.util.List[_] => list.asScala
        case _ => args
      }
      numbers.foreach(s *= parseDouble(_))
    }
    s
  }

  private val subtract = TransformerFunction.pure("subtract") { args =>
    if (args.length == 0) {
      throw new IllegalArgumentException("Subtract called without any arguments")
    }
    val numbers: scala.collection.Seq[Any] = args(0) match {
      case list: java.util.List[_] => list.asScala
      case _ => args
    }
    var s: Double = parseDouble(numbers.head)
    numbers.drop(1).foreach(s -= parseDouble(_))
    s
  }

  private val divide = TransformerFunction.pure("divide") { args =>
    if (args.length == 0) {
      throw new IllegalArgumentException("Divide called without any arguments")
    }
    val numbers: scala.collection.Seq[Any] = args(0) match {
      case list: java.util.List[_] => list.asScala
      case _ => args
    }
    var s: Double = parseDouble(numbers.head)
    numbers.drop(1).foreach(s /= parseDouble(_))
    s
  }

  private val mean = TransformerFunction.pure("mean") { args =>
    if (args.length == 0) { 0d } else {
      var count = 0d
      val numbers: scala.collection.Seq[Any] = args(0) match {
        case list: java.util.List[_] => list.asScala
        case _ => args
      }
      numbers.foreach(n => count += parseDouble(n))
      count / numbers.length
    }
  }

  private val min = TransformerFunction.pure("min") { args =>
    if (args.length == 0) {
      throw new IllegalArgumentException("Min called without any arguments")
    }
    val numbers: scala.collection.Seq[Any] = args(0) match {
      case list: java.util.List[_] => list.asScala
      case _ => args
    }
    var min = numbers.head.asInstanceOf[Comparable[Any]]
    numbers.drop(1).foreach { n =>
      if (min.compareTo(n) > 0) {
        min = n.asInstanceOf[Comparable[Any]]
      }
    }
    min
  }

  private val max = TransformerFunction.pure("max") { args =>
    if (args.length == 0) {
      throw new IllegalArgumentException("Max called without any arguments")
    }
    val numbers: scala.collection.Seq[Any] = args(0) match {
      case list: java.util.List[_] => list.asScala
      case _ => args
    }
    var max = numbers.head.asInstanceOf[Comparable[Any]]
    numbers.drop(1).foreach { n =>
      if (max.compareTo(n) < 0) {
        max = n.asInstanceOf[Comparable[Any]]
      }
    }
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
      case null      => throw new NullPointerException()
    }
  }
}
