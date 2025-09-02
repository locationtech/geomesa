/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

class MathFunctionFactory extends TransformerFunctionFactory {

  import scala.collection.JavaConverters._

  override def functions: Seq[TransformerFunction] =
    Seq(add, subtract, multiply, divide, mean, min, max, sin, asin, cos, acos, tan, atan, ln, exp, sqrt, modulo)

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

  private val sin = TransformerFunction.pure("sin") { args => Math.sin(parseSingleDouble(args, "sin")) }

  private val cos = TransformerFunction.pure("cos") { args => Math.cos(parseSingleDouble(args, "cos")) }

  private val tan = TransformerFunction.pure("tan") { args => Math.tan(parseSingleDouble(args, "tan")) }

  private val asin = TransformerFunction.pure("asin") { args => Math.asin(parseSingleDouble(args, "asin")) }

  private val acos = TransformerFunction.pure("acos") { args => Math.acos(parseSingleDouble(args, "acos")) }

  private val atan = TransformerFunction.pure("atan") { args => Math.atan(parseSingleDouble(args, "atan")) }

  private val ln = TransformerFunction.pure("ln") { args => Math.log(parseSingleDouble(args, "ln")) }

  private val exp = TransformerFunction.pure("exp") { args => Math.exp(parseSingleDouble(args, "exp")) }

  private val sqrt = TransformerFunction.pure("sqrt") { args => Math.sqrt(parseSingleDouble(args, "sqrt")) }

  private val modulo = TransformerFunction.pure("modulo") { args =>
    if (args.length == 0) {
      throw new IllegalArgumentException("modulo called without any arguments")
    }
    val numbers: scala.collection.Seq[Any] = args(0) match {
      case list: java.util.List[_] => list.asScala
      case _ => args
    }
    if (numbers.length != 2) {
      throw new IllegalArgumentException(s"modulo called with an invalid number of arguments: expected 2 but got ${args.length}")
    }
    parseInt(args(0)) % parseInt(args(1))
  }

  private def parseSingleDouble(args: Array[Any], fn: String): Double = {
    if (args.length != 1) {
      throw new IllegalArgumentException(s"$fn called with an invalid number of arguments: expected 1 but got ${args.length}")
    }
    parseDouble(args(0))
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

  private def parseInt(v: Any): Int = {
    v match {
      case n: Int    => n
      case n: Double => n.toInt
      case n: Float  => n.toInt
      case n: Long   => n.toInt
      case n: String => n.toInt
      case n: Any    => n.toString.toInt
      case null      => throw new NullPointerException()
    }
  }
}
